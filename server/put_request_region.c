#include "put_request_region.h"

#include <threads.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sisci_api.h>
#include <string.h>
#include "put_request_region_protocol.h"
#include "sisci_glob_defs.h"
#include "super_fast_hash.h"
#include "index_data_protocol.h"

#define BUDDY_ALLOC_IMPLEMENTATION
#include "buddy_alloc.h"

int put_request_region_poller(void *arg) {
    put_request_region_poller_thread_args_t *args = (put_request_region_poller_thread_args_t *) arg;

    sci_error_t sci_error;

    union {
        sci_local_segment_t local;
        sci_remote_segment_t remote;
    } put_request_segment;

    sci_map_t put_request_map;

    //TODO: Does both of these need to be volatile?
    volatile put_request_region_t *put_request_segment_data_read;
    volatile put_request_region_t *put_request_segment_data_write;

    // Set up request region
    SEOE(SCICreateSegment,
         args->sd,
         &put_request_segment.local,
         PUT_REQUEST_SEGMENT_ID,
         PUT_REQUEST_SEGMENT_SIZE,
         NO_CALLBACK,
         NO_ARG,
         SCI_FLAG_BROADCAST);

    SEOE(SCIPrepareSegment,
         put_request_segment.local,
         ADAPTER_NO,
         NO_FLAGS);

    SEOE(SCISetSegmentAvailable,
         put_request_segment.local,
         ADAPTER_NO,
         NO_FLAGS);

    //TODO: Should make one pointer for each of the regions using the offset, put it in a array
    put_request_segment_data_read = SCIMapLocalSegment(put_request_segment.local,
                                                  &put_request_map,
                                                  NO_OFFSET,
                                                  PUT_REQUEST_REGION_SIZE,
                                                  NO_SUGGESTED_ADDRESS,
                                                  NO_FLAGS,
                                                  &sci_error);

    put_request_segment_data_read->status = UNUSED;

    SEOE(SCIConnectSegment,
         args->sd,
         &put_request_segment.remote,
         DIS_BROADCAST_NODEID_GROUP_ALL,
         PUT_REQUEST_SEGMENT_ID,
         ADAPTER_NO,
         NO_CALLBACK,
         NO_ARG,
         SCI_INFINITE_TIMEOUT,
         SCI_FLAG_BROADCAST);

    put_request_segment_data_write = SCIMapRemoteSegment(put_request_segment.remote,
                                                   &put_request_map,
                                                   NO_OFFSET,
                                                   PUT_REQUEST_REGION_SIZE,
                                                   NO_SUGGESTED_ADDRESS,
                                                   NO_FLAGS,
                                                   &sci_error);

    // Set up buddy allocator
    void *buddy_metadata = malloc(buddy_sizeof(DATA_REGION_SIZE));
    struct buddy *buddy = buddy_init(buddy_metadata, args->data_region, DATA_REGION_SIZE);

    // Wait until thread is ready
    while (put_request_segment_data_read->status == UNUSED) thrd_yield();

    //Enter main loop
    while (put_request_segment_data_read->status != EXITED) {
        if (put_request_segment_data_read->status == LOCKED) {
            thrd_yield();
            continue;
        }

        // Now, the status is WALKABLE and we should walk it
        for (uint32_t i = 0; i < put_request_segment_data_read->slots_used; i++) { //TODO: is this an issue? slots_used might be updated
            uint32_t slot_start = put_request_segment_data_read->slot_offset_starts[i];
            volatile put_request_slot_preamble_t *slot_read = (volatile put_request_slot_preamble_t *) (put_request_segment_data_read->units + slot_start);
            volatile put_request_slot_preamble_t *slot_write = (volatile put_request_slot_preamble_t *) (put_request_segment_data_write->units + slot_start);
            if (slot_read->status == FREE) continue;

            if (slot_read->replica_ack[args->replica_number]) continue;

            // No we know that we should actually insert this value into our data-structure.
            char *key = strndup((const char *) (slot_start + sizeof(put_request_slot_preamble_t)), slot_read->key_length);
            void *data = (void *) (slot_start + sizeof(put_request_slot_preamble_t) + slot_read->key_length);

            uint32_t key_hash = super_fast_hash((void *) key, slot_read->key_length);
            bool inserted = false;
            for (uint8_t slot = 0; slot < INDEX_SLOTS_PR_BUCKET; slot++) {
                index_entry *index_slot = (index_entry *) GET_SLOT_POINTER((char *) args->index_region, key_hash%INDEX_BUCKETS, slot);
                if (index_slot->status == 0) continue;

                void *data_location_in_table = buddy_malloc(buddy, slot_read->value_length);
                memcpy(data_location_in_table, data, slot_read->value_length);

                inserted = true;
            }
            if (inserted) {
                printf("New put request with key %s inserted\n", key);
            } else {
                //TODO: see line below
                fprintf(stderr, "Did not find any available slots for request, should probably handle this somehow\n");
            }

            slot_write->replica_ack[args->replica_number] = true;
        }
    }

    return 0;
}
