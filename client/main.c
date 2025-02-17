#include "main.h"

#include <stdlib.h>
#include <sisci_api.h>
#include <pthread.h>
#include <string.h>

#include "sisci_glob_defs.h"
#include "put_request_region.h"
#include "index_data_protocol.h"

#include "slots.h"
#include "put_request_region_utils.h"
#include "super_fast_hash.h"
#include "get_protocol.h"
#include "contingency_get_fetch.h"

#define GetReceiveSegmentIdBase 42
#define GET_TIMEOUT_NS 1000000

static sci_desc_t sd;
static sci_dma_queue_t replica_dma_queues[REPLICA_COUNT];
static sci_local_segment_t get_receive_segment[REPLICA_COUNT];
static sci_remote_segment_t index_segments[REPLICA_COUNT];
static sci_remote_segment_t data_segments[REPLICA_COUNT];
static sci_map_t get_receive_map[REPLICA_COUNT];
static void *get_receive_data[REPLICA_COUNT];

static volatile put_request_region_t *put_request_region;

static uint32_t free_header_slot = 0;
static slot_metadata_t *slots[PUT_REQUEST_BUCKETS];

static get_index_response_args_t get_index_args[REPLICA_COUNT];
static pthread_mutex_t completed_index_fetches_mutex;
static uint8_t completed_fetches_of_index = 0;
static size_t completed_index_fetch_replica_index_order[REPLICA_COUNT];
static stored_index_data_t stored_index_data[REPLICA_COUNT];
static get_data_response_args_t preferred_data_fetch_args;

// Only allow a single get at a time, the thing is that we want to use the DMA Callbacks, which means that we can not
// simultaneously use the wait for dma queue function. If we did not use the callbacks but rather the wait for function
// we could still not have any more stuff in flight, so I don't think that we are loosing anything in doing it this way.
// Another pro of doing it this way is that we prioritize the data fetch over a new index fetch, potentially leading to
// starvation or just very long latencies, even though throughput does not decline.
static pthread_mutex_t get_in_progress;
static pending_get_status_t pending_get_status;


sci_callback_action_t index_fetch_completed_callback(void IN *arg, sci_dma_queue_t queue, sci_error_t status);
static void put(const char *key, uint8_t key_len, void *value, uint32_t value_len, bool block_for_completion);
sci_callback_action_t preferred_data_fetch_completed_callback(void IN *arg, sci_dma_queue_t queue, sci_error_t status);
static get_return_t *get(const char *key, uint8_t key_len);

int main(int argc, char* argv[]) {
    if (argc < REPLICA_COUNT + 1) {
        fprintf(stderr, "Usage: %s replica_id[0] ... replica_id[n]\n", argv[0]);
    }
    SEOE(SCIInitialize, NO_FLAGS);
    SEOE(SCIOpen, &sd, NO_FLAGS);

    init_bucket_desc();
    connect_to_put_request_region(sd, &put_request_region); //TODO: Reset some state if client reconnects?
    init_slots(slots, put_request_region);

    pthread_mutex_init(&completed_index_fetches_mutex, NULL);
    pthread_mutex_init(&get_in_progress, NULL);
    contingency_init(sd);

    for (uint8_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        char *endptr;
        long replica_node_id;
        replica_node_id = strtol(argv[1 + replica_index], &endptr, 10);
        if (replica_node_id > UINT8_MAX) {
            fprintf(stderr, "String not convertable to uint8!\n");
            exit(EXIT_FAILURE);
        }
        
        SEOE(SCICreateDMAQueue,
             sd,
             &replica_dma_queues[replica_index],
             ADAPTER_NO,
             INDEX_SLOTS_PR_BUCKET,
             NO_FLAGS);

        SEOE(SCICreateSegment,
             sd,
             &get_receive_segment[replica_index],
             GetReceiveSegmentIdBase,
             GET_RECEIVE_SEG_SIZE,
             NO_CALLBACK,
             NO_ARG,
             SCI_FLAG_DMA_GLOBAL | SCI_FLAG_AUTO_ID);

        SEOE(SCIPrepareSegment,
             get_receive_segment[replica_index],
             ADAPTER_NO,
             NO_FLAGS);

        sci_error_t sci_error;

        get_receive_data[replica_index] = SCIMapLocalSegment(
                get_receive_segment[replica_index],
                &get_receive_map[replica_index],
                NO_OFFSET,
                GET_RECEIVE_SEG_SIZE,
                NO_SUGGESTED_ADDRESS,
                NO_FLAGS,
                &sci_error);

        if (sci_error != SCI_ERR_OK) {
            fprintf(stderr, "SCI error: %s\n", SCIGetErrorString(sci_error));
            exit(EXIT_FAILURE);
        }

        SEOE(SCIConnectSegment,
             sd,
             &index_segments[replica_index],
             replica_node_id,
             replica_index_segment_id[replica_index],
             ADAPTER_NO,
             NO_CALLBACK,
             NO_ARG,
             SCI_INFINITE_TIMEOUT,
             NO_FLAGS);

        SEOE(SCIConnectSegment,
             sd,
             &data_segments[replica_index],
             replica_node_id,
             replica_data_segment_id[replica_index],
             ADAPTER_NO,
             NO_CALLBACK,
             NO_ARG,
             SCI_INFINITE_TIMEOUT,
             NO_FLAGS);
    }

    create_put_ack_data_interrupt(sd, slots);
    put_request_region->status = ACTIVE;

    unsigned char sample_data[128];

    for (unsigned char i = 0; i < 128; i++) {
        sample_data[i] = i;
    }

    unsigned char sample_data2[128];

    for (unsigned char i = 0; i < 128; i++) {
        sample_data2[i] = i/2;
    }

    char key[] = "tall";
    char key2[] = "tall2";

    put(key, 4, sample_data, sizeof(sample_data), false);
    put(key2, 5, sample_data2, sizeof(sample_data2), false);
    put(key, 4, sample_data, sizeof(sample_data), true);

    get_return_t *return_struct1 = get(key2, 5);
    get_return_t *return_struct2 = get(key, 4);

    printf("At place 69 of get with data length %u: %u\n", return_struct1->data_length, ((unsigned char *) return_struct1->data)[69]);
    printf("At place 69 of get with data_2 length %u: %u\n", return_struct2->data_length, ((unsigned char *) return_struct2->data)[69]);

    free(return_struct1->data);
    free(return_struct2->data);
    free(return_struct1);
    free(return_struct2);

    // TODO: How to free the slots in buddy and in general
    while(1);

    SEOE(SCIClose, sd, NO_FLAGS);
    SCITerminate();

    return EXIT_SUCCESS;
}

static void put(const char *key, uint8_t key_len, void *value, uint32_t value_len, bool block_for_completion) {
    slot_metadata_t *slot = put_into_available_slot(slots, key, key_len, value, value_len);
    put_request_region->header_slots[free_header_slot] = (size_t) slot->offset;
    free_header_slot = (free_header_slot + 1) % MAX_PUT_REQUEST_SLOTS;
    if (block_for_completion) while (slot->status == SLOT_STATUS_PUT);
}

// Always blocking semantics as we return the data, you need to free the data and return struct itself after use.
// You need to free the data even though there is an error and data length is 0
static get_return_t *get(const char *key, uint8_t key_len) {
    pthread_mutex_lock(&get_in_progress);
    pending_get_status.status = NOT_POSTED;
    pending_get_status.data_length = 0;
    pending_get_status.data = NULL;
    // First we need to get the hash of the key
    uint32_t key_hash = super_fast_hash(key, key_len);

    // Now we need to read index buckets from the replicas this should be done asynchronously preferably, so that
    // we can get the data slot from the one that answers first
    // This is most simple to do using DMA transfers as we can specify callback functions that will be called upon
    // completion. This is not a big drawback it would seem from experiments.

    pending_get_status.data = malloc(MAX_SIZE_ELEMENT);
    if (pending_get_status.data == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    for (size_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        stored_index_data[replica_index].completed = false;

        uint32_t bucket_no = key_hash % INDEX_BUCKETS;

        size_t offset = bucket_no * INDEX_SLOTS_PR_BUCKET * sizeof(index_entry_t);

        get_index_response_args_t *args = &get_index_args[replica_index];

        args->replica_index = replica_index;
        args->key_hash = key_hash;
        args->key = key;
        args->key_len = key_len;

        pthread_mutex_lock(&completed_index_fetches_mutex);
        completed_fetches_of_index = 0;
        pthread_mutex_unlock(&completed_index_fetches_mutex);

        SEOE(SCIStartDmaTransfer,
             replica_dma_queues[replica_index],
             get_receive_segment[replica_index],
             index_segments[replica_index],
             NO_OFFSET,
             INDEX_SLOTS_PR_BUCKET * sizeof(index_entry_t), // TODO: Will it go quicker if we align this?
             offset,
             index_fetch_completed_callback,
             args,
             SCI_FLAG_USE_CALLBACK | SCI_FLAG_DMA_READ | SCI_FLAG_DMA_GLOBAL);

        pending_get_status.status = POSTED;
    }

    get_return_t *return_struct = malloc(sizeof(get_return_t));
    if (return_struct == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    // We could perhaps use waiting on a condition or sched_yield? Not sure how it would affect performance, but would
    // save resources
    struct timespec ts_pre;
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts_pre);
    clock_gettime(CLOCK_MONOTONIC, &ts);

    while (pending_get_status.status == POSTED) {
        clock_gettime(CLOCK_MONOTONIC, &ts);
        if ((ts.tv_sec - ts_pre.tv_sec) != 0 ||
            (ts.tv_nsec - ts_pre.tv_nsec) > GET_TIMEOUT_NS) {
            // Timeout!
            printf("TIMEOUT!\n");
            for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
                // Abort all pending DMA operations
                SEOE(SCIAbortDMAQueue,
                     replica_dma_queues[replica_index],
                     NO_FLAGS);
            }

            return_struct->status = GET_RETURN_ERROR;
            return_struct->data_length = 0;
            return_struct->data = NULL;
            return_struct->error_message = TIMEOUT;

            return return_struct;
        }
    }

    switch (pending_get_status.status) {
        case COMPLETED_SUCCESS:
            return_struct->status = GET_RETURN_SUCCESS;
            return_struct->data_length = pending_get_status.data_length;
            return_struct->data = pending_get_status.data;
            return_struct->error_message = NO_ERROR_MSG;
            break;
        case COMPLETED_ERROR:
            return_struct->status = GET_RETURN_ERROR;
            return_struct->data_length = 0;
            return_struct->data = NULL;
            return_struct->error_message = pending_get_status.error_message;
            break;
        case NOT_POSTED:
        case POSTED:
        default:
            fprintf(stderr, "Got illegal status from pending_get_status\n");
            free(return_struct);
            exit(EXIT_FAILURE);
    }

    return return_struct;
}

sci_callback_action_t index_fetch_completed_callback(void IN *arg, sci_dma_queue_t queue, sci_error_t status) {
    get_index_response_args_t *args = ((get_index_response_args_t *) arg);
    size_t replica_index = args->replica_index;
    uint32_t key_hash = args->key_hash;

    stored_index_data[replica_index].completed = true;

    index_entry_t *slot = (index_entry_t *) get_receive_data[replica_index];

    stored_index_data[replica_index].slots_length = 0;

    if (status == SCI_ERR_OK) {
        for (uint8_t slot_index = 0; slot_index < INDEX_SLOTS_PR_BUCKET; slot++, slot_index++) {
            if (slot->status != 1) continue;
            if (slot->hash != key_hash) continue;
            if (slot->key_length != args->key_len) continue;

            memcpy(&stored_index_data[replica_index].slots[stored_index_data[replica_index].slots_length++],
                   slot,
                   sizeof(*slot));
        }
    } else {
        fprintf(stderr, "not ok index fetch: %s\n", SCIGetErrorString(status));
        // We let this fall through as it will be handled by slots_length being 0
    }

    pthread_mutex_lock(&completed_index_fetches_mutex);
    completed_index_fetch_replica_index_order[completed_fetches_of_index] = replica_index;
    if (completed_fetches_of_index == 0) {
        completed_fetches_of_index++;
        pthread_mutex_unlock(&completed_index_fetches_mutex);

        slot = (index_entry_t *) stored_index_data[replica_index].slots;
        uint8_t slots_with_hash_count = 0;
        dis_dma_vec_t dma_vec[INDEX_SLOTS_PR_BUCKET];
        unsigned int local_offset = 0;

        for (uint8_t slot_index = 0; slot_index < stored_index_data[replica_index].slots_length; slot++, slot_index++) {
            dma_vec[slots_with_hash_count].size = slot->key_length + slot->data_length;
            dma_vec[slots_with_hash_count].remote_offset = (unsigned int) slot->offset;
            dma_vec[slots_with_hash_count].local_offset = local_offset;
            dma_vec[slots_with_hash_count].flags = NO_FLAGS;

            local_offset += dma_vec[slots_with_hash_count].size;

            slots_with_hash_count++;
        }

        if (slots_with_hash_count == 0) {
            // If we either did not find any slots in the preferred replica or if the fetch failed, defer all to the
            // contingency fetch. This will also mean that we try another backend, as this replica does not have any
            // candidates
            contingency_backend_fetch(&pending_get_status,
                                      &get_in_progress,
                                      &completed_fetches_of_index,
                                      &completed_index_fetches_mutex,
                                      data_segments,
                                      stored_index_data,
                                      completed_index_fetch_replica_index_order,
                                      NULL,
                                      0,
                                      args->key);

            return SCI_CALLBACK_CONTINUE;
        }

        preferred_data_fetch_args.key_len = args->key_len;
        preferred_data_fetch_args.key = args->key;
        preferred_data_fetch_args.replica_index = (uint8_t) args->replica_index;

        // Fetch the data segments for all those slots
        // No need of timing this out either, it will be handled by the general TIMEOUT. Retries would not likely help anyways
        SEOE(SCIStartDmaTransferVec,
             replica_dma_queues[replica_index],
             get_receive_segment[replica_index],
             data_segments[replica_index],
             slots_with_hash_count,
             dma_vec,
             preferred_data_fetch_completed_callback,
             &preferred_data_fetch_args,
             SCI_FLAG_USE_CALLBACK | SCI_FLAG_DMA_READ | SCI_FLAG_DMA_GLOBAL);
    } else {
        completed_fetches_of_index++;
        pthread_mutex_unlock(&completed_index_fetches_mutex);
    }

    return SCI_CALLBACK_CONTINUE;
}

sci_callback_action_t preferred_data_fetch_completed_callback(void IN *arg, sci_dma_queue_t queue, sci_error_t status) {
    get_data_response_args_t *args = (get_data_response_args_t *) arg;
    uint32_t tried_vnrs[INDEX_SLOTS_PR_BUCKET];

    if (status != SCI_ERR_OK) {
        contingency_backend_fetch(&pending_get_status,
                                  &get_in_progress,
                                  &completed_fetches_of_index,
                                  &completed_index_fetches_mutex,
                                  data_segments,
                                  stored_index_data,
                                  completed_index_fetch_replica_index_order,
                                  NULL,
                                  0,
                                  args->key);
        return SCI_CALLBACK_CONTINUE;
    }

    // First figure out which of the slots has the correct key
    long version_number = -1;
    uint8_t correct_slot;

    uint32_t tried_vnr_count = 0;
    char *data_slot = (char *) get_receive_data[args->replica_index];
    for (uint8_t slot = 0; slot < stored_index_data[args->replica_index].slots_length; slot++) {
        index_entry_t index_slot = stored_index_data[args->replica_index].slots[slot];

        if (strncmp(args->key, data_slot, args->key_len) != 0) {
            tried_vnrs[tried_vnr_count++] = index_slot.version_number;
            data_slot += index_slot.key_length + index_slot.data_length;
            continue;
        }

        version_number = index_slot.version_number;
        correct_slot = slot;
        break;
    }

    if (version_number == -1) {
        contingency_backend_fetch(&pending_get_status,
                                  &get_in_progress,
                                  &completed_fetches_of_index,
                                  &completed_index_fetches_mutex,
                                  data_segments,
                                  stored_index_data,
                                  completed_index_fetch_replica_index_order,
                                  tried_vnrs,
                                  tried_vnr_count,
                                  args->key);
        return SCI_CALLBACK_CONTINUE;
    }

    // No need in timing out this, if we dont exit this loop, the quorum can not be gotten
    while(1) {
        pthread_mutex_lock(&completed_index_fetches_mutex);
        if (completed_fetches_of_index >= (REPLICA_COUNT + 1)/2) { pthread_mutex_unlock(&completed_index_fetches_mutex); break; }
        pthread_mutex_unlock(&completed_index_fetches_mutex);
    }

    uint8_t correct_vnr_count = 0;
    for (uint8_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {

        if (!stored_index_data[replica_index].completed) continue;

        for (uint8_t slot = 0; slot < stored_index_data[replica_index].slots_length; slot++) {
            if (stored_index_data[replica_index].slots[slot].version_number == version_number) {
                correct_vnr_count++;
                break;
            }
        }
    }

    if (correct_vnr_count >= (REPLICA_COUNT + 1)/2) {
        pending_get_status.data_length = stored_index_data[args->replica_index].slots[correct_slot].data_length;

        memcpy(pending_get_status.data, data_slot + args->key_len, pending_get_status.data_length);
        pending_get_status.status = COMPLETED_SUCCESS;
        pthread_mutex_unlock(&get_in_progress);
    } else {
        contingency_backend_fetch(&pending_get_status,
                                  &get_in_progress,
                                  &completed_fetches_of_index,
                                  &completed_index_fetches_mutex,
                                  data_segments,
                                  stored_index_data,
                                  completed_index_fetch_replica_index_order,
                                  tried_vnrs,
                                  tried_vnr_count,
                                  args->key);
    }

    return SCI_CALLBACK_CONTINUE;
}
