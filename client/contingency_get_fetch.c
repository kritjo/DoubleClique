#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <sisci_types.h>
#include <stdbool.h>
#include <sisci_api.h>
#include <string.h>
#include "contingency_get_fetch.h"
#include "sisci_glob_defs.h"
#include "index_data_protocol.h"
#include "get_protocol.h"

#define GetReceiveSegmentIdBase 43

static contingency_fetch_completed_args_t contingency_fetch_args[REPLICA_COUNT * INDEX_SLOTS_PR_BUCKET];
static uint32_t found_contingency_candidates_count = 0;
static bool completed_contingency_fetches[REPLICA_COUNT * INDEX_SLOTS_PR_BUCKET];

static pthread_mutex_t completed_contingency_fetches_mutex; //TODO: init

static sci_local_segment_t get_receive_segment[REPLICA_COUNT];
static volatile void *get_receive_data[REPLICA_COUNT];
static sci_map_t get_receive_map[REPLICA_COUNT];
static sci_dma_queue_t replica_dma_queues[REPLICA_COUNT];

void contingency_init(sci_desc_t sd) {
    pthread_mutex_init(&completed_contingency_fetches_mutex, NULL);

    for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
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

        SEOE(SCICreateDMAQueue,
             sd,
             &replica_dma_queues[replica_index],
             ADAPTER_NO,
             INDEX_SLOTS_PR_BUCKET,
             NO_FLAGS);
    }
}

void contingency_destroy(void) {
    //TODO
}

void contingency_backend_fetch(pending_get_status_t *pending_get_status,
                               pthread_mutex_t *get_in_progress,

                               const uint8_t *completed_fetches_of_index,
                               pthread_mutex_t *completed_index_fetches_mutex,

                               sci_remote_segment_t *data_segments,
                               stored_index_data_t *stored_index_data,
                               const size_t *completed_index_fetch_replica_index_order,

                               const uint32_t *already_tried_vnr,
                               uint32_t already_tried_vnr_count,

                               const char *key) {
    version_count_t version_count[REPLICA_COUNT * INDEX_SLOTS_PR_BUCKET];
    uint32_t version_count_count = 0;

    for (uint32_t i = 0; i < REPLICA_COUNT * INDEX_SLOTS_PR_BUCKET; i++) {
        version_count[i].count = 0;
        version_count[i].version_number = 0;
        for (uint32_t j = 0; j < REPLICA_COUNT; j++) {
            version_count[i].index_entry[j].status = 0; // Good thing that this is a copy and not a pointer
        }
    }

    struct timespec ts_pre;
    struct timespec ts;

    // Wait for completion of all index fetches MAYBE: use CLOCK_MONOTONIC_COARSE?
    // If we can not find it in time, just use what we have
    clock_gettime(CLOCK_MONOTONIC, &ts_pre);
    clock_gettime(CLOCK_MONOTONIC, &ts);

    while((ts.tv_sec - ts_pre.tv_sec) == 0 &&
          (ts.tv_nsec = ts_pre.tv_nsec) < CONTINGENCY_WAIT_FOR_INDEX_FETCHES_TIMEOUT_NS) {
        pthread_mutex_lock(completed_index_fetches_mutex);
        if (*completed_fetches_of_index == REPLICA_COUNT)
        { pthread_mutex_unlock(completed_index_fetches_mutex); break; }
        pthread_mutex_unlock(completed_index_fetches_mutex);
        clock_gettime(CLOCK_MONOTONIC, &ts);
    }

    // Check if we have a quorum of version numbers
    for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        // For every replica, we need to check all of their returned index entries
        for (uint32_t slot_index = 0; slot_index < stored_index_data[replica_index].slots_length; slot_index++) {
            index_entry_t slot = stored_index_data[replica_index].slots[slot_index];
            bool found = false;

            // For every index entry's version number we need to count it in the version_count structure
            // This could be done more efficiently with a hash map or something but the structures are very short
            // so that is not done yet.
            for (uint32_t version_count_index = 0; version_count_index < version_count_count; version_count_index++) {
                if (version_count[version_count_index].version_number == slot.version_number) {
                    found = true;
                    version_count[version_count_index].count++;
                    version_count[version_count_index].index_entry[replica_index] = slot;
                }
            }

            if (!found) {
                version_count[version_count_count].version_number = slot.version_number;
                version_count[version_count_count].index_entry[replica_index] = slot;
                version_count[version_count_count++].count = 1;
            }
        }
    }

    found_candidates_t found_candidates[REPLICA_COUNT * INDEX_SLOTS_PR_BUCKET];

    for (uint32_t version_count_index = 0; version_count_index < version_count_count; version_count_index++) {
        if (version_count[version_count_index].count >= (REPLICA_COUNT + 1)/2) {
            // Check if we have seen this version number before
            for (uint32_t i = 0; i < already_tried_vnr_count; i++) {
                if (already_tried_vnr[i] == version_count[version_count_index].version_number)
                    goto continue_outer;
            }

            for (uint32_t i = 0; i < REPLICA_COUNT; i++) {
                found_candidates[found_contingency_candidates_count].index_entry[i] = version_count[version_count_index].index_entry[i];
            }
            found_candidates[found_contingency_candidates_count++].version_number = version_count[version_count_index].version_number;
        }
        continue_outer:;
    }

    if (found_contingency_candidates_count == 0) {
        // Did not find a quorum, fail the fetch
        pending_get_status->data_length = 0;
        pending_get_status->data = NULL;
        pending_get_status->status = COMPLETED_ERROR;
        pending_get_status->error_message = NO_INDEX_ENTRIES;
        pthread_mutex_unlock(get_in_progress);
    }

    size_t local_offset[REPLICA_COUNT];
    for (uint32_t i = 0; i < REPLICA_COUNT; i++) {
        local_offset[i] = 0;
    }

    pthread_mutex_lock(&completed_contingency_fetches_mutex);
    for (uint32_t candidate_index = 0; candidate_index < found_contingency_candidates_count; candidate_index++) {
        completed_contingency_fetches[candidate_index] = false;
    }
    pthread_mutex_unlock(&completed_contingency_fetches_mutex);

    // Did find one or more quorums, lets try to get them.
    for (uint32_t candidate_index = 0; candidate_index < found_contingency_candidates_count; candidate_index++) {
        for (uint32_t completed_first_index = 0; completed_first_index < REPLICA_COUNT; completed_first_index++) {
            size_t replica_index = completed_index_fetch_replica_index_order[completed_first_index];

            // Check if this replica has that data
            if (found_candidates[candidate_index].index_entry[replica_index].status == 0) continue;

            // Now we have found the (assumed) fastest replica that has this candidate ship it and continue with next c
            // We could optimize it so that if multiple of these candidates were shipped to a single server
            // we would switch to a vector dma transfer, but I think that the overhead of computing that outweighs the
            // benefit, at least since we probably usually only have a single candidate
            size_t transfer_size = found_candidates[candidate_index].index_entry[replica_index].key_length +
                                   found_candidates[candidate_index].index_entry[replica_index].data_length;

            uint32_t offset = (uint32_t) found_candidates[candidate_index].index_entry[replica_index].offset;

            contingency_fetch_completed_args_t *args = &contingency_fetch_args[candidate_index];
            args->offset = offset;
            args->replica_index = (uint32_t) replica_index;
            args->index_entry = found_candidates[candidate_index].index_entry[replica_index];
            args->key = key;
            args->candidate_index = candidate_index;
            args->pending_get_status = pending_get_status;
            args->get_in_progress = get_in_progress;

            SEOE(SCIStartDmaTransfer,
                 replica_dma_queues[replica_index],
                 get_receive_segment[replica_index],
                 data_segments[replica_index],
                 local_offset[replica_index],
                 transfer_size,
                 offset,
                 contingency_data_fetch_completed_callback,
                 args,
                 SCI_FLAG_USE_CALLBACK | SCI_FLAG_DMA_READ | SCI_FLAG_DMA_GLOBAL);

            local_offset[replica_index] += transfer_size;

            break;
        }
    }
}

sci_callback_action_t contingency_data_fetch_completed_callback(void IN *arg, sci_dma_queue_t queue, sci_error_t status) {
    contingency_fetch_completed_args_t *args = (contingency_fetch_completed_args_t *) arg;

    pthread_mutex_lock(&completed_contingency_fetches_mutex);
    completed_contingency_fetches[args->candidate_index] = true;
    pthread_mutex_unlock(&completed_contingency_fetches_mutex);

    if (status != SCI_ERR_OK) {
        pthread_mutex_lock(&completed_contingency_fetches_mutex);

        uint32_t completed_contingency_fetches_count = 0;
        for (uint32_t candidate_index = 0; candidate_index < found_contingency_candidates_count; candidate_index++) {
            if (completed_contingency_fetches[candidate_index])
                completed_contingency_fetches_count++;
        }

        pthread_mutex_unlock(&completed_contingency_fetches_mutex);

        if (completed_contingency_fetches_count == found_contingency_candidates_count) {
            fprintf(stderr, "not ok contingency fetch: %s\n", SCIGetErrorString(status));
            args->pending_get_status->data_length = 0;
            args->pending_get_status->data = 0;
            args->pending_get_status->status = COMPLETED_ERROR;
            args->pending_get_status->error_message = CONTINGENCY_ERROR;
            pthread_mutex_unlock(args->get_in_progress);
        }

        return SCI_CALLBACK_CONTINUE;
    }

    volatile char *slot = ((volatile char *) get_receive_data[args->replica_index]) + args->offset;

    // Need to this instead of strncmp because of volatile modifier
    bool match = true;
    for (uint32_t i = 0; i < args->index_entry.key_length; i++) {
        if (slot[i] != args->key[i]) { match = false; break; }
    }

    if (match) {
        args->pending_get_status->data_length = args->index_entry.data_length;

        // Need to do this instead of memcpy because of volatile modifier
        for (uint32_t i = 0; i < args->pending_get_status->data_length; i++) {
            *(((char *) args->pending_get_status->data) + i) = (slot + args->index_entry.key_length)[i];
        }

        args->pending_get_status->status = COMPLETED_SUCCESS;
        pthread_mutex_unlock(args->get_in_progress);
    } else {
        pthread_mutex_lock(&completed_contingency_fetches_mutex);

        uint32_t completed_contingency_fetches_count = 0;
        for (uint32_t candidate_index = 0; candidate_index < found_contingency_candidates_count; candidate_index++) {
            if (completed_contingency_fetches[candidate_index])
                completed_contingency_fetches_count++;
        }

        pthread_mutex_unlock(&completed_contingency_fetches_mutex);

        if (completed_contingency_fetches_count == found_contingency_candidates_count) {
            args->pending_get_status->data_length = 0;
            args->pending_get_status->data = 0;
            args->pending_get_status->status = COMPLETED_ERROR;
            args->pending_get_status->error_message = NO_DATA_MATCH;
            pthread_mutex_unlock(args->get_in_progress);
        }
    }

    return SCI_CALLBACK_CONTINUE;
}
