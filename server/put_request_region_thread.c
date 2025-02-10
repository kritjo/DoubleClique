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

static put_request_region_t *put_request_segment_data_read;
static uint32_t current_head_slot = 0;
static struct buddy *buddy = NULL;

static inline put_request_slot_preamble_t *wait_for_new_put(void) {
    while (put_request_segment_data_read->header_slots[current_head_slot] == 0);

    size_t slot_offset = put_request_segment_data_read->header_slots[current_head_slot];
    return (put_request_slot_preamble_t *) ((char *) put_request_segment_data_read + sizeof(put_request_region_t) + slot_offset);
}

int put_request_region_poller(void *arg) {
    put_request_region_poller_thread_args_t *args = (put_request_region_poller_thread_args_t *) arg;

    init_put_request_region(args->sd);

    put_request_segment_data_read->status = INACTIVE;

    // Set up buddy allocator
    void *buddy_metadata = malloc(buddy_sizeof(DATA_REGION_SIZE));
    buddy = buddy_init(buddy_metadata, args->data_region, DATA_REGION_SIZE);

    bool connected_to_client = false;
    sci_remote_data_interrupt_t ack_data_interrupt;

    //Enter main loop
    while (1) {
        if (put_request_segment_data_read->status == INACTIVE) {
            thrd_yield();
            continue;
        }

        if (!connected_to_client) {
            connect_to_put_ack_data_interrupt(args->sd, &ack_data_interrupt, put_request_segment_data_read->sisci_node_id);
            connected_to_client = true;
        }

        // Wait for new transfer
        put_request_slot_preamble_t *slot_read = wait_for_new_put();

        char *key = strndup(((char *) slot_read) + sizeof(put_request_slot_preamble_t), slot_read->key_length);
        uint32_t key_hash = super_fast_hash((void *) key, slot_read->key_length);

        void *data = (void *) ((char *) slot_read + sizeof(put_request_slot_preamble_t) + slot_read->key_length);

        bool update = true;
        index_entry_t *index_slot = existing_slot_for_key(args->index_region, args->data_region, key_hash, slot_read->key_length, key);
        if (index_slot == NULL) update = false;
        index_slot = find_available_index_slot(args->index_region, key_hash);
        if (index_slot == NULL) {
            //TODO: see line below
            fprintf(stderr, "Did not find any available slots for request, should probably handle this somehow\n");

            put_request_segment_data_read->header_slots[current_head_slot] = 0; // TODO: figure out if this has some bad implications as we write to and read from a 'read-only' memory right? This is not actually written to the client or broadcasted
            current_head_slot = (current_head_slot + 1) % MAX_PUT_REQUEST_SLOTS;

            continue;
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


        printf("New put_into_slot request with key %s inserted\n", key);
        send_ack(args->replica_number, ack_data_interrupt, put_request_segment_data_read->header_slots[current_head_slot]);

        put_request_segment_data_read->header_slots[current_head_slot] = 0; // TODO: figure out if this has some bad implications as we write to and read from a 'read-only' memory right? This is not actually written to the client or broadcasted
        current_head_slot = (current_head_slot + 1) % MAX_PUT_REQUEST_SLOTS;

        free(key);
    }

    return 0;
}

static void *buddy_wrapper(size_t size) {
    return buddy_malloc(buddy, size);
}

void init_put_request_region(sci_desc_t sd) {
    sci_error_t sci_error;

    sci_local_segment_t put_request_segment_read;
    sci_map_t put_request_map_read;

    // Set up request region
    SEOE(SCICreateSegment,
         sd,
         &put_request_segment_read,
         PUT_REQUEST_SEGMENT_ID,
         put_region_size(),
         NO_CALLBACK,
         NO_ARG,
         SCI_FLAG_BROADCAST);

    SEOE(SCIPrepareSegment,
         put_request_segment_read,
         ADAPTER_NO,
         NO_FLAGS);

    SEOE(SCISetSegmentAvailable,
         put_request_segment_read,
         ADAPTER_NO,
         NO_FLAGS);

    put_request_segment_data_read = SCIMapLocalSegment(put_request_segment_read,
                                                       &put_request_map_read,
                                                       NO_OFFSET,
                                                       put_region_size(),
                                                       NO_SUGGESTED_ADDRESS,
                                                       NO_FLAGS,
                                                       &sci_error);

    if (sci_error != SCI_ERR_OK) {
        fprintf(stderr, "SCIMapLocalSegment failed: %s\n", SCIGetErrorString(sci_error));
        exit(EXIT_FAILURE);
    }
}
