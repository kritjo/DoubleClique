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
#include "dma_utils.h"

static sci_desc_t sd;
static sci_dma_queue_t replica_dma_queues[REPLICA_COUNT];
static sci_local_segment_t get_receive_segment[REPLICA_COUNT]; // TODO: Maybe this needs to be done otherwise if we want asyncronicity with public API
static sci_remote_segment_t index_segments[REPLICA_COUNT];
static sci_remote_segment_t data_segments[REPLICA_COUNT];
static sci_map_t get_receive_map[REPLICA_COUNT];
static void *get_receive_data[REPLICA_COUNT];

static volatile put_request_region_t *put_request_region;

static uint32_t free_header_slot = 0;
static slot_metadata_t *slots[PUT_REQUEST_BUCKETS];

static pthread_mutex_t mutex;

static uint8_t completed_fetches_of_index = 0;
static stored_index_data_t stored_index_data[REPLICA_COUNT];

sci_callback_action_t index_fetch_completed_callback(void IN *arg, sci_dma_queue_t queue, sci_error_t status);
static void put(const char *key, uint8_t key_len, void *value, uint32_t value_len);
sci_callback_action_t data_fetch_completed_callback(void IN *arg, sci_dma_queue_t queue, sci_error_t status);
static void *get(const char *key, uint8_t key_len);

int main(int argc, char* argv[]) {
    if (argc < REPLICA_COUNT + 1) {
        fprintf(stderr, "Usage: %s replica_id[0] ... replica_id[n]\n", argv[0]);
    }
    SEOE(SCIInitialize, NO_FLAGS);
    SEOE(SCIOpen, &sd, NO_FLAGS);

    init_bucket_desc();
    connect_to_put_request_region(sd, &put_request_region);
    init_slots(slots, put_request_region);

    pthread_mutex_init(&mutex, NULL);

    for (uint8_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        char *endptr;
        long replica_node_id;
        replica_node_id = strtol(argv[1 + replica_index], &endptr, 10);
        if (replica_node_id > UINT8_MAX) {
            fprintf(stderr, "String not convertable to uint8!\n");
            exit(EXIT_FAILURE);
        }

        init_replica_data_index_region(sd, replica_index, (uint8_t) replica_node_id);

        SEOE(SCICreateDMAQueue,
             sd,
             &replica_dma_queues[replica_index],
             ADAPTER_NO,
             1, //TODO: Check if maxEntries affects how much we can legally post to the queue, and how queue state is affected
             NO_FLAGS);

#define GetReceiveSegmentIdBase 42
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

    char key[] = "tall";
    char key2[] = "tall2";

    put(key, 4, sample_data, sizeof(sample_data));
    put(key2, 5, sample_data, sizeof(sample_data));
    put(key, 4, sample_data, sizeof(sample_data));
    sleep(1); //TODO: Figure out why this is needed, some race condition
    get(key2, 5);
    get(key, 4);

    // TODO: How to free the slots in buddy and in general
    while(1);

    SEOE(SCIClose, sd, NO_FLAGS);
    SCITerminate();

    return EXIT_SUCCESS;
}

static void put(const char *key, uint8_t key_len, void *value, uint32_t value_len) {
    slot_metadata_t *slot = put_into_available_slot(slots, key, key_len, value, value_len);
    put_request_region->header_slots[free_header_slot] = (size_t) slot->offset;
    free_header_slot = (free_header_slot + 1) % MAX_PUT_REQUEST_SLOTS;
}

//TODO: support for concurrent gets
static void *get(const char *key, uint8_t key_len) {
    // First we need to get the hash of the key
    uint32_t key_hash = super_fast_hash(key, key_len);
    printf("getting key_hash %u\n", key_hash);

    // Now we need to read index buckets from the replicas this should be done asynchronously preferably, so that
    // we can get the data slot from the one that answers first
    // This is most simple to do using DMA transfers as we can specify callback functions that will be called upon
    // completion. This is not a big drawback it would seem from experiments.

    for (size_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        stored_index_data[replica_index].completed = false;

        uint32_t bucket_no = key_hash % INDEX_BUCKETS;
        printf("bucket number: %u\n", bucket_no);

        size_t offset = bucket_no * INDEX_SLOTS_PR_BUCKET * sizeof(index_entry_t);
        printf("offset: %zu\n", offset);

        get_index_response_args_t *args = malloc(sizeof(get_index_response_args_t));
        args->replica_index = replica_index;
        args->key_hash = key_hash;
        args->key = key;
        args->key_len = key_len;

        block_for_dma(replica_dma_queues[replica_index]);

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
    }
}

sci_callback_action_t index_fetch_completed_callback(void IN *arg, sci_dma_queue_t queue, sci_error_t status) {
    // Inside this function we will handle what happens when a single fetch of an index bucket completes
    // When the first bucket returns, we will immediately fetch the data slot
    // When the second bucket returns, we will check if it gives us a quorm, if yes return the data that we are already
    // fetching when it is ready
    // - if not-> start fetching the data that we would otherwise fetch if this arrived first and decide what to do when last arrives.
    // If it agrees with one of them, great return that, if not abort with an error.
    if (status != SCI_ERR_OK) {
        fprintf(stderr, "not ok index fetch\n");
        while(1);
    }
    printf("Index fetch completed\n");

    get_index_response_args_t *args = ((get_index_response_args_t *) arg);
    size_t replica_index = args->replica_index;
    uint32_t key_hash = args->key_hash;

    stored_index_data[replica_index].completed = true;

    index_entry_t *slot = (index_entry_t *) get_receive_data[replica_index];

    for (uint8_t slot_index = 0; slot_index < INDEX_SLOTS_PR_BUCKET; slot++, slot_index++) {
        if (slot->hash != key_hash) continue;
        if (slot->key_length != args->key_len) continue;

        memcpy(&stored_index_data[replica_index].slots[stored_index_data[replica_index].slots_length++],
               slot,
               sizeof(*slot));
    }

    printf("locing mutex\n");
    pthread_mutex_lock(&mutex);
    printf("got mutex\n");
    if (completed_fetches_of_index == 0) {
        printf("f1\n");
        completed_fetches_of_index++;
        pthread_mutex_unlock(&mutex);
        printf("f2\n");

        slot = (index_entry_t *) get_receive_data[replica_index];
        uint8_t slots_with_hash_count = 0;
        dis_dma_vec_t dma_vec[INDEX_SLOTS_PR_BUCKET];
        unsigned int local_offset = 0;

        printf("f3\n");

        for (uint8_t slot_index = 0; slot_index < stored_index_data[replica_index].slots_length; slot++, slot_index++) {
            dma_vec[slots_with_hash_count].size = slot->key_length + slot->data_length;
            dma_vec[slots_with_hash_count].remote_offset = (unsigned int) slot->offset; //TODO: should think about all casts and check
            dma_vec[slots_with_hash_count].local_offset = local_offset;
            dma_vec[slots_with_hash_count].flags = NO_FLAGS;

            local_offset += dma_vec[slots_with_hash_count].size;

            slots_with_hash_count++;
        }
        printf("f4\n");

        if (slots_with_hash_count == 0) {
            printf("xxxf5\n");
            return SCI_CALLBACK_CONTINUE;
        }

        block_for_dma(replica_dma_queues[replica_index]); // TODO: check if we can have more space in the queue

        get_data_response_args_t *data_args = malloc(sizeof(get_data_response_args_t));
        data_args->key_len = args->key_len;
        data_args->key = args->key;
        data_args->replica_index = (uint8_t) args->replica_index;

        // Fetch the data segments for all those slots
        printf("sending dma transfer\n");
        SEOE(SCIStartDmaTransferVec,
             replica_dma_queues[replica_index],
             get_receive_segment[replica_index],
             data_segments[replica_index],
             slots_with_hash_count,
             dma_vec,
             data_fetch_completed_callback,
             data_args,
             SCI_FLAG_USE_CALLBACK | SCI_FLAG_DMA_READ | SCI_FLAG_DMA_GLOBAL);
    } else {
        printf("a1\n");
        completed_fetches_of_index++;
        pthread_mutex_unlock(&mutex);
    }

    free (args);

    return SCI_CALLBACK_CONTINUE;
}

sci_callback_action_t data_fetch_completed_callback(void IN *arg, sci_dma_queue_t queue, sci_error_t status) {
    printf("Data fetch completed\n");
    get_data_response_args_t *args = (get_data_response_args_t *) arg;

    // First figure out which of the slots has the correct key
    long version_number = -1;
    uint8_t correct_slot = 0;
    char *key = malloc(args->key_len + 1); //TODO: Only for debug purposes
    char *data_slot = (char *) get_receive_data[args->replica_index];
    for (uint8_t slot = 0; slot < stored_index_data[args->replica_index].slots_length; slot++) {
        if (strncmp(args->key, data_slot, args->key_len) != 0) continue;
        version_number = stored_index_data[args->replica_index].slots[slot].version_number;
        strncpy(key, data_slot, args->key_len);
        key[args->key_len] = '\0';
        correct_slot = slot;
        break;
    }

    if (version_number == -1) {
        // TODO: Did not find in this replica, need to check other quorums
        free(args);
        return SCI_CALLBACK_CANCEL;
    }

    while(1) {
        pthread_mutex_lock(&mutex);
        if (completed_fetches_of_index >= (REPLICA_COUNT + 1)/2) break;
        pthread_mutex_unlock(&mutex);
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
        // We have found the right one
        printf("Found data with key: %s\n", key);
        /*unsigned char *data = (unsigned char *) data_slot + args->key_len;
        for (long i = 0; i < stored_index_data[args->replica_index].slots[correct_slot].data_length; i++) {
            printf("data[%ld] = %u\n", i, *(data + i));
        }*/
        //TODO: Return this somehow to the client function
    } else {
        // TODO: Found conflicting and need to do new data fetch if we find a quorum
        fprintf(stderr, "Found conflicting\n");
    }

    free(args);
    return SCI_CALLBACK_CONTINUE;
}
