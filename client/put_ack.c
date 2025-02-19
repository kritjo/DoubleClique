#include <sisci_types.h>
#include <stdlib.h>
#include <sisci_api.h>
#include <time.h>

#include "put_ack.h"
#include "sisci_glob_defs.h"
#include "put_request_region.h"

static enum replica_ack_type *replica_ack;
static put_ack_slot_t put_ack_slots[MAX_PUT_REQUEST_SLOTS];

static sci_local_segment_t put_ack_segment;
static sci_map_t put_ack_map;

void init_put_ack_region(sci_desc_t sd) {
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

    for (uint32_t slot_index = 0; slot_index < MAX_PUT_REQUEST_SLOTS; slot_index++) {
        put_ack_slots[slot_index].header_slot_available = true;
    }
}

void block_for_available(uint32_t header_slot) {
    while(!put_ack_slots[header_slot].header_slot_available);
}

void register_new_put(slot_metadata_t *metadata_slot, uint32_t header_slot, enum feedback_type feedback_type) {
    if (feedback_type != FEEDBACK_TYPE_SYNC) {
        fprintf(stderr, "Only SYNC feedback supported yet!\n");
    }

    for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        *(replica_ack + (header_slot * REPLICA_COUNT) + replica_index) = REPLICA_NOT_ACKED;
    }

    put_ack_slot_t *slot = &put_ack_slots[header_slot];
    clock_gettime(CLOCK_MONOTONIC, &slot->start_time);
    slot->metadata_slot = metadata_slot;
    slot->feedback_type = feedback_type;
    slot->header_slot_available = false;
}

void *put_ack_thread_start(__attribute__((unused)) void *_arg) {
    while (1) {
        for (uint32_t header_slot_index = 0; header_slot_index < MAX_PUT_REQUEST_SLOTS; header_slot_index++) {
            put_ack_slot_t *slot = &put_ack_slots[header_slot_index];

            if (slot->header_slot_available) continue;

            uint32_t ack_success_count = 0;
            uint32_t ack_count = 0;
            for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
                if (*(replica_ack + (header_slot_index * REPLICA_COUNT) + replica_index) != REPLICA_NOT_ACKED)
                    ack_count++;

                if (*(replica_ack + (header_slot_index * REPLICA_COUNT) + replica_index) == REPLICA_ACK_SUCCESS)
                    ack_success_count++;
            }

            // If we got a quorum of success acks, count as success
            if (ack_success_count > (REPLICA_COUNT + 1) / 2) {
                // Success!
                slot->metadata_slot->status = SLOT_STATUS_SUCCESS;
                continue;
            }

            if (ack_count == REPLICA_COUNT) {
                // Check what errors we have gotten
                enum replica_ack_type replica_ack_type;
                bool mix = false;
                for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
                    if (replica_index == 0) {
                        replica_ack_type = *(replica_ack + (header_slot_index * REPLICA_COUNT) + replica_index);
                        continue;
                    }
                    if (*(replica_ack + (header_slot_index * REPLICA_COUNT) + replica_index) != replica_ack_type) {
                        mix = true;
                    }
                }

                if (mix) {
                    slot->metadata_slot->status = SLOT_STATUS_ERROR_MIX;
                    continue;
                }

                switch (replica_ack_type) {
                    case REPLICA_ACK_ERROR_OUT_OF_SPACE:
                        slot->metadata_slot->status = SLOT_STATUS_ERROR_OUT_OF_SPACE;
                    case REPLICA_ACK_SUCCESS:
                    case REPLICA_NOT_ACKED:
                    default:
                        fprintf(stderr, "Illegal REPLICA_ACK type!\n");
                        exit(EXIT_FAILURE);
                }
            }

            // If we do not have error replies and not a quorum, check for timeout
            struct timespec end;
            clock_gettime(CLOCK_MONOTONIC, &end);

            if (end.tv_sec - slot->start_time.tv_sec != 0 ||
                end.tv_nsec - slot->start_time.tv_nsec >= PUT_TIMEOUT_NS) {
                slot->metadata_slot->status = SLOT_STATUS_ERROR_TIMEOUT;
            }

            // Not timeout, let it live on!
        }
    }

    return NULL;
}

void client_ack_self(uint32_t header_slot) {
    put_ack_slots[header_slot].header_slot_available = true;
    put_ack_slots[header_slot].metadata_slot->status = SLOT_STATUS_FREE;
}
