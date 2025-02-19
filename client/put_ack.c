#include <sisci_api.h>
#include <stdlib.h>
#include <sched.h>
#include "put_ack.h"
#include "sisci_glob_defs.h"

static volatile _Atomic uint32_t free_header_slot = 0;
static volatile _Atomic uint32_t oldest_header_slot = 0;

static enum replica_ack_type *replica_ack;
static put_ack_slot_t put_ack_slots[MAX_PUT_REQUEST_SLOTS];

static sci_local_segment_t put_ack_segment;
static sci_map_t put_ack_map;

void init_put_ack(sci_desc_t sd) {
    sci_error_t sci_error;
    SEOE(SCICreateSegment,
         sd,
         &put_ack_segment,
         PUT_ACK_SEGMENT_ID,
         MAX_PUT_REQUEST_SLOTS * sizeof(enum replica_ack_type) * REPLICA_COUNT,
         NO_CALLBACK,
         NO_ARG,
         NO_FLAGS
    );

    SEOE(SCIPrepareSegment,
         put_ack_segment,
         ADAPTER_NO,
         NO_FLAGS);

    SEOE(SCISetSegmentAvailable,
         put_ack_segment,
         ADAPTER_NO,
         NO_FLAGS);

    replica_ack = (enum replica_ack_type *) SCIMapLocalSegment(
            put_ack_segment,
            &put_ack_map,
            NO_OFFSET,
            MAX_PUT_REQUEST_SLOTS * sizeof(enum replica_ack_type) * REPLICA_COUNT,
            NO_SUGGESTED_ADDRESS,
            NO_FLAGS,
            &sci_error);

    if (sci_error != SCI_ERR_OK) {
        fprintf(stderr, "Could not map local segment: %s\n", SCIGetErrorString(sci_error));
        exit(EXIT_FAILURE);
    }
}

// Critical region function
put_promise_t *acquire_header_slot_blocking(slot_metadata_t *metadata_slot) {
    // Check if it is actually in-flight
    // TODO: sched_yield optimize?
    while ((free_header_slot + 1) % MAX_PUT_REQUEST_SLOTS == oldest_header_slot) sched_yield(); // This complains but I think using atomics should work TODO: try removing volatile


    for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        *(replica_ack + (free_header_slot * REPLICA_COUNT) + replica_index) = REPLICA_NOT_ACKED;
    }

    uint32_t my_header_slot = free_header_slot;
    put_ack_slot_t *put_ack_slot = &put_ack_slots[my_header_slot];

    put_ack_slot->metadata_slot = metadata_slot;
    clock_gettime(CLOCK_MONOTONIC, &put_ack_slot->start_time);

    put_promise_t *promise = malloc(sizeof(put_promise_t));
    if (promise == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    promise->result = PUT_PENDING;
    promise->header_slot = my_header_slot;

    put_ack_slot->promise = promise;

    free_header_slot = (free_header_slot + 1) % MAX_PUT_REQUEST_SLOTS;

    return promise;
}

void *put_ack_thread(__attribute__((unused)) void *_args) {
    while (1) {
        if (oldest_header_slot == free_header_slot) { sched_yield(); continue; }

        uint32_t ack_success_count = 0;
        uint32_t ack_count = 0;
        for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
            if (*(replica_ack + (oldest_header_slot * REPLICA_COUNT) + replica_index) != REPLICA_NOT_ACKED)
                ack_count++;

            if (*(replica_ack + (oldest_header_slot * REPLICA_COUNT) + replica_index) == REPLICA_ACK_SUCCESS)
                ack_success_count++;
        }

        put_ack_slot_t *put_ack_slot = &put_ack_slots[oldest_header_slot];

        // If we got a quorum of success acks, count as success
        if (ack_success_count > (REPLICA_COUNT + 1) / 2) {
            // Success!
            put_ack_slot->promise->result = PUT_RESULT_SUCCESS;
            put_ack_slot->metadata_slot->status = SLOT_STATUS_FREE;
            oldest_header_slot = (oldest_header_slot + 1) % MAX_PUT_REQUEST_SLOTS;
            continue;
        }

        if (ack_count == REPLICA_COUNT) {
            // Check what errors we have gotten
            enum replica_ack_type replica_ack_type;
            bool mix = false;
            for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
                if (replica_index == 0) {
                    replica_ack_type = *(replica_ack + (oldest_header_slot * REPLICA_COUNT) + replica_index);
                    continue;
                }
                if (*(replica_ack + (oldest_header_slot * REPLICA_COUNT) + replica_index) != replica_ack_type) {
                    mix = true;
                }
            }

            if (mix) {
                put_ack_slot->promise->result = PUT_RESULT_ERROR_MIX;
                put_ack_slot->metadata_slot->status = SLOT_STATUS_FREE;
                oldest_header_slot = (oldest_header_slot + 1) % MAX_PUT_REQUEST_SLOTS;
                continue;
            }

            switch (replica_ack_type) {
                case REPLICA_ACK_ERROR_OUT_OF_SPACE:
                    put_ack_slot->promise->result = PUT_RESULT_ERROR_OUT_OF_SPACE;
                    put_ack_slot->metadata_slot->status = SLOT_STATUS_FREE;
                    oldest_header_slot = (oldest_header_slot + 1) % MAX_PUT_REQUEST_SLOTS;
                    continue;
                case REPLICA_ACK_SUCCESS:
                case REPLICA_NOT_ACKED:
                default:
                    fprintf(stderr, "Illegal REPLICA_ACK type!\n");
                    exit(EXIT_FAILURE);
            }
        }

        // If we do not have error replies and not a quorum, check for timeout
        struct timespec end_p;
        clock_gettime(CLOCK_MONOTONIC, &end_p);

        if (end_p.tv_sec - put_ack_slot->start_time.tv_sec != 0 ||
            end_p.tv_nsec - put_ack_slot->start_time.tv_nsec >= PUT_TIMEOUT_NS) {
            put_ack_slot->promise->result = PUT_RESULT_ERROR_TIMEOUT;
            put_ack_slot->metadata_slot->status = SLOT_STATUS_FREE;
            oldest_header_slot = (oldest_header_slot + 1) % MAX_PUT_REQUEST_SLOTS;
            continue;
        }

        // Not timeout, let it live on!
    }

    return NULL;
}
