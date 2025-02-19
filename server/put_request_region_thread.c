#include "put_request_region_thread.h"

#include <threads.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sisci_api.h>
#include <string.h>
#include "put_request_region.h"
#include "sisci_glob_defs.h"
#include "super_fast_hash.h"
#include "index_data_protocol.h"

#define BUDDY_ALLOC_IMPLEMENTATION
#include "buddy_alloc.h"
#include "put_request_region_utils.h"
#include "../client/put_ack.h"

static put_request_region_t *put_request_segment;
static uint32_t current_head_slot = 0;
static struct buddy *buddy = NULL;

static void send_ack(uint8_t number, volatile enum replica_ack_type *pType, uint32_t slot, sci_sequence_t put_ack_sequence);

static inline put_request_slot_preamble_t *wait_for_new_put(void) {
    while (put_request_segment->header_slots[current_head_slot] == 0);

    size_t slot_offset = put_request_segment->header_slots[current_head_slot];
    return (put_request_slot_preamble_t *) ((char *) put_request_segment + sizeof(put_request_region_t) + slot_offset);
}

static inline void *buddy_wrapper(size_t size) {
    return buddy_malloc(buddy, size);
}

int put_request_region_poller(void *arg) {
    put_request_region_poller_thread_args_t *args = (put_request_region_poller_thread_args_t *) arg;
    init_put_request_region(args->sd, &put_request_segment);
    put_request_segment->status = INACTIVE;

    // Set up buddy allocator
    void *buddy_metadata = malloc(buddy_sizeof(DATA_REGION_SIZE));
    buddy = buddy_init(buddy_metadata, args->data_region, DATA_REGION_SIZE);

    bool connected_to_client = false;

    sci_remote_segment_t put_ack_segment;
    sci_map_t put_ack_map;
    volatile enum replica_ack_type *replica_ack;
    sci_error_t sci_error;
    sci_sequence_t put_ack_sequence;

    struct timespec start;
    struct timespec end;

    //Enter main loop
    while (1) {
        if (put_request_segment->status == INACTIVE) {
            thrd_yield();
            continue;
        }

        // If this is the first time we are entering the loop, connect to the client
        if (!connected_to_client) {
            SEOE(SCIConnectSegment,
                 args->sd,
                 &put_ack_segment,
                 put_request_segment->sisci_node_id,
                 PUT_ACK_SEGMENT_ID,
                 ADAPTER_NO,
                 NO_CALLBACK,
                 NO_ARG,
                 SCI_INFINITE_TIMEOUT,
                 NO_FLAGS);

            replica_ack = (volatile enum replica_ack_type *) SCIMapRemoteSegment(
                    put_ack_segment,
                    &put_ack_map,
                    NO_OFFSET,
                    MAX_PUT_REQUEST_SLOTS * sizeof(enum replica_ack_type) * REPLICA_COUNT,
                    NO_SUGGESTED_ADDRESS,
                    NO_FLAGS,
                    &sci_error);

            SEOE(SCICreateMapSequence,
                 put_ack_map,
                 &put_ack_sequence,
                 NO_FLAGS);

            connected_to_client = true;
        }

        // Wait for new transfer
        put_request_slot_preamble_t *slot_read = wait_for_new_put();
        clock_gettime(CLOCK_MONOTONIC, &start);

        // TODO: We dont really need this, its just nice to have the key for debugging purposes, we could just have a pointer into the slot
        char *key = strndup(((char *) slot_read) + sizeof(put_request_slot_preamble_t), slot_read->key_length);

        uint32_t key_hash = super_fast_hash((void *) key, slot_read->key_length);
        void *data = (void *) ((char *) slot_read + sizeof(put_request_slot_preamble_t) + slot_read->key_length);

        bool update;
        index_entry_t *index_slot = existing_slot_for_key(args->index_region, args->data_region, key_hash, slot_read->key_length, key);

        // If we did not find an existing slot, we are not updating, but fresh inserting
        if (index_slot == NULL) {
            update = false;

            // Try to find an available slot
            index_slot = find_available_index_slot(args->index_region, key_hash);

            // If we do not find one, there is no space left
            if (index_slot == NULL) {
                //TODO: see line below
                fprintf(stderr, "Did not find any available slots for request, should probably handle this somehow\n");

                put_request_segment->header_slots[current_head_slot] = 0; // TODO: figure out if this has some bad implications as we write to and read from a 'read-only' memory right? This is not actually written to the client or broadcasted
                current_head_slot = (current_head_slot + 1) % MAX_PUT_REQUEST_SLOTS;

                continue;
            }
        } else {
            update = true;
        }

        data_entry_preamble_t *data_slot = find_data_slot_for_index_slot(args->data_region,
                                                                         index_slot,
                                                                         update,
                                                                         slot_read->key_length + slot_read->value_length,
                                                                         buddy_wrapper);

        insert_in_table(args->data_region,
                        index_slot,
                        data_slot,
                        key,
                        slot_read->key_length,
                        key_hash,
                        data,
                        slot_read->value_length,
                        slot_read->version_number);

        clock_gettime(CLOCK_MONOTONIC, &end);


        send_ack(args->replica_number, replica_ack, current_head_slot, put_ack_sequence);

        put_request_segment->header_slots[current_head_slot] = 0; // TODO: figure out if this has some bad implications as we write to and read from a 'read-only' memory right? This is not actually written to the client or broadcasted
        current_head_slot = (current_head_slot + 1) % MAX_PUT_REQUEST_SLOTS;

        free(key);
    }

    return 0;
}

// TODO: should probably include the version number to avoid replay attacks, or ensure the timers are set up such that
// replay attacks are impossible
static void send_ack(uint8_t replica_index, volatile enum replica_ack_type *replica_ack, uint32_t header_slot, sci_sequence_t put_ack_sequence) {
    *(replica_ack + (header_slot * REPLICA_COUNT) + replica_index) = REPLICA_ACK_SUCCESS;
    SCIFlush(put_ack_sequence, NO_FLAGS);
}
