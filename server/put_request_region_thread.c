#include "put_request_region_thread.h"

#include <threads.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sisci_api.h>
#include <string.h>
#include <sched.h>
#include "put_request_region.h"
#include "sisci_glob_defs.h"
#include "super_fast_hash.h"

#include "index_data_protocol.h"
#define BUDDY_ALLOC_IMPLEMENTATION
#include "buddy_alloc.h"
#include "put_request_region_utils.h"

#include "../client/put.h"


static put_request_region_t *put_request_region;
static struct buddy *buddy = NULL;

static void send_ack(uint8_t number, volatile replica_ack_t *pType, uint32_t slot, sci_sequence_t put_ack_sequence, uint32_t version_number, enum replica_ack_type ack_type);

static inline void *buddy_wrapper(size_t size) {
    return buddy_malloc(buddy, size);
}

int put_request_region_poller(void *arg) {
    put_request_region_poller_thread_args_t *args = (put_request_region_poller_thread_args_t *) arg;
    init_put_request_region(args->sd, &put_request_region);
    put_request_region->status = PUT_REQUEST_REGION_INACTIVE;

    // Set up buddy allocator
    void *buddy_metadata = malloc(buddy_sizeof(DATA_REGION_SIZE));
    buddy = buddy_init(buddy_metadata, args->data_region, DATA_REGION_SIZE);

    bool connected_to_client = false;

    sci_remote_segment_t put_ack_segment;
    sci_map_t put_ack_map;
    volatile replica_ack_t *replica_ack;
    sci_error_t sci_error;
    sci_sequence_t put_ack_sequence;

    struct timespec start;
    struct timespec end;

    //Enter main loop
    while (1) {
        if (put_request_region->status == PUT_REQUEST_REGION_INACTIVE) {
            thrd_yield();
            continue;
        }

        // If this is the first time we are entering the loop, connect to the client
        if (!connected_to_client) {
            SEOE(SCIConnectSegment,
                 args->sd,
                 &put_ack_segment,
                 put_request_region->sisci_node_id,
                 PUT_ACK_SEGMENT_ID,
                 ADAPTER_NO,
                 NO_CALLBACK,
                 NO_ARG,
                 SCI_INFINITE_TIMEOUT,
                 NO_FLAGS);

            replica_ack = (volatile replica_ack_t *) SCIMapRemoteSegment(
                    put_ack_segment,
                    &put_ack_map,
                    NO_OFFSET,
                    MAX_PUT_REQUEST_SLOTS * sizeof(replica_ack_t) * REPLICA_COUNT,
                    NO_SUGGESTED_ADDRESS,
                    NO_FLAGS,
                    &sci_error);

            SEOE(SCICreateMapSequence,
                 put_ack_map,
                 &put_ack_sequence,
                 NO_FLAGS);

            connected_to_client = true;
        }

        /* It might seem like this will have a lot of overhead to constantly poll on every slot, it might add a little
         * bit of overhead when just waiting for a single put, but when experiencing constant writing, it will implicitly
         * sync up, as when we hit a slot that we actually need to do something with, the next in the loop will be ready
         * when we continue;.
         */
        for (uint32_t current_head_slot = 0; current_head_slot < MAX_PUT_REQUEST_SLOTS; current_head_slot++) {
            header_slot_t slot = put_request_region->header_slots[current_head_slot];
            if (slot.status != HEADER_SLOT_USED) continue;

            char *data_slot_start = ((char *) put_request_region) + sizeof(put_request_region_t);

            size_t offset = slot.offset;

            clock_gettime(CLOCK_MONOTONIC, &start);

            // TODO: We dont really need this, its just nice to have the key for debugging purposes, we could just have a pointer into the slot
            char *key = malloc(slot.key_length + 1);
            if (key == NULL) {
                perror("malloc");
                exit(EXIT_FAILURE);
            }

            for (uint32_t i = 0; i < slot.key_length; i++) {
                key[i] = data_slot_start[offset];
                offset = (offset + 1) % PUT_REQUEST_REGION_DATA_SIZE;
            }
            key[slot.key_length] = '\0';

            uint32_t key_hash = super_fast_hash((void *) key, slot.key_length);

            char *data = malloc(slot.value_length);
            if (data == NULL) {
                perror("malloc");
                exit(EXIT_FAILURE);
            }

            for (uint32_t i = 0; i < slot.value_length; i++) {
                data[i] = data_slot_start[offset];
                offset = (offset + 1) % PUT_REQUEST_REGION_DATA_SIZE;
            }

            uint32_t value_hash = super_fast_hash(data, (int) slot.value_length);

            // TODO: This could be a function, shared with client
            char *hash_data = malloc(sizeof(uint32_t) * 2);
            if (hash_data == NULL) {
                perror("malloc");
                exit(EXIT_FAILURE);
            }

            // Copy key_hash into the first 4 bytes of hash_data
            memcpy(hash_data, &key_hash, sizeof(uint32_t));

            // Copy value_hash into the next 4 bytes of hash_data
            memcpy(hash_data + sizeof(uint32_t), &value_hash, sizeof(uint32_t));

            uint32_t payload_hash = super_fast_hash(hash_data, sizeof(uint32_t) * 2);
            free(hash_data);

            if (payload_hash != slot.payload_hash) {
                // Torn read
                put_request_region->header_slots[current_head_slot].status = HEADER_SLOT_UNUSED; // TODO: figure out if this has some bad implications as we write to and read from a 'read-only' memory right? This is not actually written to the client or broadcasted
                continue;
            }

            bool update;
            index_entry_t *index_slot = existing_slot_for_key(args->index_region, args->data_region, key_hash,
                                                              slot.key_length, key);

            // If we did not find an existing slot, we are not updating, but fresh inserting
            if (index_slot == NULL) {
                update = false;

                // Try to find an available slot
                index_slot = find_available_index_slot(args->index_region, key_hash);

                // If we do not find one, there is no space left
                if (index_slot == NULL) {
                    send_ack(args->replica_number, replica_ack, current_head_slot, put_ack_sequence,
                             slot.version_number, REPLICA_ACK_ERROR_OUT_OF_SPACE);
                    put_request_region->header_slots[current_head_slot].status = HEADER_SLOT_UNUSED; // TODO: figure out if this has some bad implications as we write to and read from a 'read-only' memory right? This is not actually written to the client or broadcasted
                    continue;
                }
            } else {
                update = true;
            }

            data_entry_preamble_t *data_slot = find_data_slot_for_index_slot(args->data_region,
                                                                             index_slot,
                                                                             update,
                                                                             slot.key_length + slot.value_length,
                                                                             buddy_wrapper);

            insert_in_table(args->data_region,
                            index_slot,
                            data_slot,
                            key,
                            slot.key_length,
                            key_hash,
                            data,
                            slot.value_length,
                            slot.version_number);
            free(data);

            // Need to do this before sending ack in case of race
            put_request_region->header_slots[current_head_slot].status = HEADER_SLOT_UNUSED; // TODO: figure out if this has some bad implications as we write to and read from a 'read-only' memory right? This is not actually written to the client or broadcasted

            send_ack(args->replica_number, replica_ack, current_head_slot, put_ack_sequence, slot.version_number,
                     REPLICA_ACK_SUCCESS);
            clock_gettime(CLOCK_MONOTONIC, &end);
            free(key);
        }
    }

    return 0;
}

// TODO: should probably include the version number to avoid replay attacks, or ensure the timers are set up such that
// replay attacks are impossible
static void send_ack(uint8_t replica_index, volatile replica_ack_t *replica_ack, uint32_t header_slot, sci_sequence_t put_ack_sequence, uint32_t version_number, enum replica_ack_type ack_type) {
    volatile replica_ack_t *replica_ack_instance = replica_ack + (header_slot * REPLICA_COUNT) + replica_index;
    replica_ack_instance->version_number = version_number;
    replica_ack_instance->replica_ack_type = ack_type;
    SCIFlush(put_ack_sequence, NO_FLAGS); //TODO: is this needed?
}
