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

    sci_local_segment_t put_request_segment_read;
    sci_map_t put_request_map_read;
    put_request_region_t *put_request_segment_data_read;

    // Set up request region
    SEOE(SCICreateSegment,
         args->sd,
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

    //TODO: Should make one pointer for each of the regions using the offset, put_into_slot it in a array
    //TODO: Right now only a single client region is checked, make that work before progressing
    put_request_segment_data_read = SCIMapLocalSegment(put_request_segment_read,
                                                  &put_request_map_read,
                                                  NO_OFFSET,
                                                  put_region_size(),
                                                  NO_SUGGESTED_ADDRESS,
                                                  NO_FLAGS,
                                                  &sci_error);

    printf("put_request_segment_data_read: %p -> %p\n", (void *) put_request_segment_data_read, (void *) ((char *) put_request_segment_data_read + put_region_size()));

    if (sci_error != SCI_ERR_OK) {
        fprintf(stderr, "SCIMapLocalSegment failed: %s\n", SCIGetErrorString(sci_error));
        exit(EXIT_FAILURE);
    }

    put_request_segment_data_read->status = INACTIVE;

    // Set up buddy allocator
    void *buddy_metadata = malloc(buddy_sizeof(DATA_REGION_SIZE));
    struct buddy *buddy = buddy_init(buddy_metadata, args->data_region, DATA_REGION_SIZE);

    bool inited = false;
    sci_remote_data_interrupt_t ack_data_interrupt;
    uint32_t current_head_slot = 0;

    //Enter main loop
    while (1) {
        if (put_request_segment_data_read->status == INACTIVE) {
            thrd_yield();
            continue;
        }

        if (!inited) {
            printf("Connecting to node id %u\n", put_request_segment_data_read->sisci_node_id);
            SEOE(SCIConnectDataInterrupt,
                 args->sd,
                 &ack_data_interrupt,
                 put_request_segment_data_read->sisci_node_id,
                 ADAPTER_NO,
                 ACK_DATA_INTERRUPT_NO,
                 SCI_INFINITE_TIMEOUT,
                 NO_FLAGS);

            inited = true;
        }

        while (put_request_segment_data_read->header_slots[current_head_slot] == 0);

        size_t slot_offset = put_request_segment_data_read->header_slots[current_head_slot];
        current_head_slot = (current_head_slot + 1) % MAX_PUT_REQUEST_SLOTS;

        printf("a: %zu\n", slot_offset);

        printf("put_request_segment_data_read: %p\n", (void *) put_request_segment_data_read);
        printf("put_request_region_t: %zu\n", sizeof(put_request_region_t));
        printf("slot_offset: %zu\n", slot_offset);

        put_request_slot_preamble_t *slot_read = (put_request_slot_preamble_t *) ((char *) put_request_segment_data_read + sizeof(put_request_region_t) + slot_offset);

        printf("b: %p\n", (void *) &slot_read->status); //TODO: Problem this seems to read outside of put region :/

        if (slot_read->status != PUT) continue;

        printf("cccc\n");

        // No we know that we should actually insert this value into our data-structure.
        char *key = malloc(slot_read->key_length + 1);
        if (key == NULL) {
            perror("malloc");
            exit(EXIT_FAILURE);
        }

        printf("c\n");

        for (uint8_t char_i = 0; char_i < slot_read->key_length; char_i++) {
            key[char_i] = *((char *) slot_read + sizeof(put_request_slot_preamble_t) + char_i);
        }
        key[slot_read->key_length] = '\0';

        printf("d\n");

        void *data = (void *) (slot_read + sizeof(put_request_slot_preamble_t) + slot_read->key_length);

        uint32_t key_hash = super_fast_hash((void *) key, slot_read->key_length);
        bool inserted = false;

        printf("e\n");
        for (uint8_t slot = 0; slot < INDEX_SLOTS_PR_BUCKET; slot++) {
            index_entry_t *index_slot = (index_entry_t *) GET_SLOT_POINTER((char *) args->index_region, key_hash % INDEX_BUCKETS, slot);
            if (index_slot->status == 1) continue;

            // Allocate a new slot, this means that we would need to TODO: garbage collect the old slot and buddy_free
            void *allocated_data_table = buddy_malloc(buddy, slot_read->value_length + slot_read->key_length + sizeof(data_entry_preamble_t));
            ptrdiff_t offset = (char *) args->data_region - (char *) allocated_data_table;

            data_entry_preamble_t *data_entry_preamble = (data_entry_preamble_t *) allocated_data_table;
            data_entry_preamble->key_length = slot_read->key_length;
            data_entry_preamble->data_length = slot_read->value_length;

            char *key_location_in_table = (char *) allocated_data_table + sizeof(*data_entry_preamble);
            strcpy(key_location_in_table, key);

            void *data_location_in_table = (char *) allocated_data_table + sizeof(*data_entry_preamble) + slot_read->key_length;
            memcpy(data_location_in_table, data, slot_read->value_length);

            index_slot->offset = offset;
            index_slot->hash = key_hash;
            index_slot->version_number = slot_read->version_number;
            index_slot->status = 1;

            inserted = true;
            slot_read->status = FREE; // This will not actually be written to the client, but only to continue in
            // the next loop iteration TODO: figure out if this has some bad implications as we write to and read from a 'read-only' memory right?
            break;
        }

        printf("f\n");

        if (inserted) {
            printf("New put_into_slot request with key %s inserted\n", key);
        } else {
            //TODO: see line below
            fprintf(stderr, "Did not find any available slots for request, should probably handle this somehow\n");
        }

        put_ack_t put_ack;
        put_ack.replica_no = args->replica_number;
        put_ack.bucket_no = get_bucket_no_from_offset(slot_offset);
        put_ack.slot_no = (uint32_t) get_slot_no_from_offset(slot_offset, put_ack.bucket_no);

        SEOE(SCITriggerDataInterrupt, ack_data_interrupt, &put_ack, sizeof(put_ack), NO_FLAGS);

        free(key);
    }

    return 0;
}
