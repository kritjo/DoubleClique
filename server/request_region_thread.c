#include "request_region_thread.h"

#include <threads.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sisci_api.h>
#include <string.h>
#include "request_region.h"
#include "sisci_glob_defs.h"
#include "super_fast_hash.h"

#include "index_data_protocol.h"

#define BUDDY_ALLOC_IMPLEMENTATION

#include "buddy_alloc.h"
#include "request_region_utils.h"
#include "sequence.h"
#include "garbage_collection_queue.h"
#include "garbage_collection.h"

static request_region_t *request_region;
static struct buddy *buddy = NULL;

static void send_put_ack(uint8_t replica_index, volatile replica_ack_t *replica_ack_remote_pointer, uint32_t header_slot,
                         sci_sequence_t ack_sequence, uint32_t version_number, enum replica_ack_type ack_type);

static void send_get_ack_phase1(uint8_t replica_index, volatile replica_ack_t *replica_ack_remote_pointer, uint32_t header_slot,
                                uint32_t key_hash, char *index_region, sci_sequence_t ack_sequence, bool write_back,
                                size_t write_back_offset, char *data_region);

static void send_get_ack_phase2(volatile replica_ack_t *replica_ack_remote_pointer, uint32_t header_slot, const char *data_pointer, uint32_t transfer_length, size_t return_offset, sci_sequence_t ack_sequence);

static void put(request_region_poller_thread_args_t *args, header_slot_t slot, uint32_t current_head_slot,
         volatile replica_ack_t *replica_ack, uint32_t key_hash, char *key, sci_sequence_t ack_sequence,
         size_t offset, queue_t *queue) {
    char *data_slot_start = ((char *) request_region) + sizeof(request_region_t);

    char *data = malloc(slot.value_length);
    if (data == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    // TODO: This could be a function, shared with client
    char *hash_data = malloc(slot.key_length + slot.value_length + sizeof(((header_slot_t *) 0)->version_number));
    if (hash_data == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    memcpy(hash_data, key, slot.key_length);

    for (uint32_t i = 0; i < slot.value_length; i++) {
        data[i] = data_slot_start[offset];
        hash_data[i + slot.key_length] = data_slot_start[offset];
        offset = (offset + 1) % REQUEST_REGION_DATA_SIZE;
    }

    memcpy(hash_data + slot.key_length + slot.value_length, &slot.version_number, sizeof(((header_slot_t *) 0)->version_number));

    uint32_t payload_hash = super_fast_hash(hash_data, (int) (slot.key_length + slot.value_length +
                                                              sizeof(((header_slot_t *) 0)->version_number)));
    free(hash_data);

    if (payload_hash != slot.payload_hash) {
        // Torn read
        request_region->header_slots[current_head_slot].status = HEADER_SLOT_UNUSED; // TODO: figure out if this has some bad implications as we write to and read from a 'read-only' memory right? This is not actually written to the client or broadcasted
        free(data);
        return;
    }

    index_entry_t *index_slot = existing_slot_for_key(args->index_region, args->data_region, key_hash,
                                                      slot.key_length, key);

    void *new_data_slot;
    // If we did not find an existing slot, we are not updating, but fresh inserting
    if (index_slot == NULL) {
        new_data_slot = buddy_malloc(buddy, slot.key_length + slot.value_length + sizeof(((header_slot_t *) 0)->version_number));
        if (new_data_slot == NULL) {
            existing_slot_for_key(args->index_region, args->data_region, key_hash,
                                  slot.key_length, key);
            send_put_ack(args->replica_number, replica_ack, current_head_slot, ack_sequence,
                         slot.version_number, REPLICA_ACK_ERROR_OUT_OF_SPACE);
            request_region->header_slots[current_head_slot].status = HEADER_SLOT_UNUSED; // TODO: figure out if this has some bad implications as we write to and read from a 'read-only' memory right? This is not actually written to the client or broadcasted
            free(data);
            return;
        }

        // Try to find an available slot
        index_slot = find_available_index_slot(args->index_region, key_hash);

        // If we do not find one, there is no space left
        if (index_slot == NULL) {
            send_put_ack(args->replica_number, replica_ack, current_head_slot, ack_sequence,
                         slot.version_number, REPLICA_ACK_ERROR_OUT_OF_SPACE);
            request_region->header_slots[current_head_slot].status = HEADER_SLOT_UNUSED; // TODO: figure out if this has some bad implications as we write to and read from a 'read-only' memory right? This is not actually written to the client or broadcasted
            free(data);
            return;
        }
    } else {
        void *old_data_slot = (void *) (((char *) args->data_region) + index_slot->offset);
        size_t new_payload_size = slot.key_length + slot.value_length + sizeof(((header_slot_t *) 0)->version_number);
        new_data_slot = buddy_malloc(buddy, new_payload_size);
        if (new_data_slot == NULL) {
            if (new_payload_size <= index_slot->key_length + index_slot->data_length + sizeof(((header_slot_t *) 0)->version_number)) {
                new_data_slot = old_data_slot;
                // Could not find any place, reuse the old, even though it might lead to fetching the wrong data for
                // in progress 2 phase gets.
            } else {
                send_put_ack(args->replica_number, replica_ack, current_head_slot, ack_sequence,
                             slot.version_number, REPLICA_ACK_ERROR_OUT_OF_SPACE);
                request_region->header_slots[current_head_slot].status = HEADER_SLOT_UNUSED; // TODO: figure out if this has some bad implications as we write to and read from a 'read-only' memory right? This is not actually written to the client or broadcasted
                free(data);
                return;
            }
        } else {
            queue_item_t queue_item;
            queue_item.buddy_allocated_addr = old_data_slot;
            clock_gettime(CLOCK_MONOTONIC, &queue_item.t);
            queue_item.t.tv_nsec += NS_TO_COLLECTION;
            if (queue_item.t.tv_nsec >= 1000000000L) {
                queue_item.t.tv_nsec += queue_item.t.tv_nsec / 1000000000L;
                queue_item.t.tv_nsec %= 1000000000L;
            }
            enqueue(queue, queue_item);
        }
    }

    insert_in_table(args->data_region,
                    index_slot,
                    new_data_slot,
                    key,
                    slot.key_length,
                    key_hash,
                    data,
                    slot.value_length,
                    slot.version_number,
                    slot.payload_hash);
    free(data);

    // Need to do this before sending ack in case of race
    request_region->header_slots[current_head_slot].status = HEADER_SLOT_UNUSED; // TODO: figure out if this has some bad implications as we write to and read from a 'read-only' memory right? This is not actually written to the client or broadcasted

    send_put_ack(args->replica_number, replica_ack, current_head_slot, ack_sequence, slot.version_number,
                 REPLICA_ACK_SUCCESS);
}

int request_region_poller(void *arg) {
    request_region_poller_thread_args_t *args = (request_region_poller_thread_args_t *) arg;
    init_request_region(args->sd, &request_region);
    request_region->status = REQUEST_REGION_INACTIVE;
    queue_t *queue = malloc(sizeof(queue_t));
    if (queue == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    queue_init(queue);

    garbage_collection_thread_args_t *garbage_collection_thread_args = malloc(sizeof(garbage_collection_thread_args_t));
    if (garbage_collection_thread_args == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    garbage_collection_thread_args->queue = queue;
    garbage_collection_thread_args->buddy = buddy;

    pthread_t gc_thread_id;
    pthread_create(
            &gc_thread_id,
            NULL,
            garbage_collection_thread,
            garbage_collection_thread_args);

    // Set up buddy allocator
    void *buddy_metadata = malloc(buddy_sizeof(DATA_REGION_SIZE));
    buddy = buddy_init(buddy_metadata, args->data_region, DATA_REGION_SIZE);

    bool connected_to_client = false;

    sci_remote_segment_t ack_segment;
    sci_map_t ack_map;
    volatile replica_ack_t *replica_ack;
    sci_error_t sci_error;
    sci_sequence_t ack_sequence;

    //Enter main loop
    while (1) {
        if (request_region->status == REQUEST_REGION_INACTIVE) {
            thrd_yield();
            continue;
        }

        // If this is the first time we are entering the loop, connect to the client
        if (!connected_to_client) {
            SEOE(SCIConnectSegment,
                 args->sd,
                 &ack_segment,
                 request_region->sisci_node_id,
                 ACK_SEGMENT_ID,
                 ADAPTER_NO,
                 NO_CALLBACK,
                 NO_ARG,
                 SCI_INFINITE_TIMEOUT,
                 NO_FLAGS);

            replica_ack = (volatile replica_ack_t *) SCIMapRemoteSegment(
                    ack_segment,
                    &ack_map,
                    NO_OFFSET,
                    ACK_REGION_SIZE,
                    NO_SUGGESTED_ADDRESS,
                    NO_FLAGS,
                    &sci_error);

            if (sci_error != SCI_ERR_OK) {
                fprintf(stderr, "Error mapping remote segment: %s\n", SCIGetErrorString(sci_error));
                exit(EXIT_FAILURE);
            }

            SEOE(SCICreateMapSequence,
                 ack_map,
                 &ack_sequence,
                 NO_FLAGS);

            sci_error_t error;
            sci_sequence_status_t status;

            status = SCIStartSequence(ack_sequence, NO_FLAGS, &error);
            if (error != SCI_ERR_OK) {
                fprintf(stderr, "SCIStartSequence returned non SCI_ERR_OK, which should not be possible: %s\n", SCIGetErrorString(error));
                exit(EXIT_FAILURE);
            }
            if (status != SCI_SEQ_OK) {
                fprintf(stderr, "SCIStartSequence returned non SCI_SEQ_OK: %d\n", status);
                exit(EXIT_FAILURE);
            }

            connected_to_client = true;
        }

        /* It might seem like this will have a lot of overhead to constantly poll on every slot, it might add a little
         * bit of overhead when just waiting for a single put, but when experiencing constant writing, it will implicitly
         * sync up, as when we hit a slot that we actually need to do something with, the next in the loop will be ready
         * when we continue;.
         */
        for (uint32_t current_head_slot = 0; current_head_slot < MAX_REQUEST_SLOTS; current_head_slot++) {
            header_slot_t slot = request_region->header_slots[current_head_slot];
            if (slot.status == HEADER_SLOT_UNUSED) continue;

            char *data_slot_start = ((char *) request_region) + sizeof(request_region_t);

            size_t offset = slot.offset;

            // TODO: We dont really need this, its just nice to have the key for debugging purposes, we could just have a pointer into the slot
            char *key = malloc(slot.key_length + 1);
            if (key == NULL) {
                perror("malloc");
                exit(EXIT_FAILURE);
            }

            for (uint32_t i = 0; i < slot.key_length; i++) {
                key[i] = data_slot_start[offset];
                offset = (offset + 1) % REQUEST_REGION_DATA_SIZE;
            }
            key[slot.key_length] = '\0';

            uint32_t key_hash = super_fast_hash((void *) key, slot.key_length);

            // UP until this point is equal for both request types.

            switch (slot.status) {
                case HEADER_SLOT_USED_PUT:
                    put(args, slot, current_head_slot, replica_ack, key_hash, key, ack_sequence, offset, queue);
                    break;
                case HEADER_SLOT_USED_GET_PHASE1:
                    // Now we need to ship our index entries for this keyhash back
                    if (key_hash != slot.payload_hash) {
                        break;
                    }
                    send_get_ack_phase1(args->replica_number, replica_ack, current_head_slot, key_hash,
                                        args->index_region, ack_sequence, args->replica_number == slot.replica_write_back_hint,
                                        slot.return_offset, args->data_region);
                    break;
                case HEADER_SLOT_USED_GET_PHASE2:
                    if (slot.replica_write_back_hint == args->replica_number) {
                        send_get_ack_phase2(replica_ack, current_head_slot, ((char *) args->data_region) + slot.offset,
                                            slot.key_length + slot.value_length + sizeof(uint32_t), slot.return_offset, ack_sequence);
                    } else {
                        request_region->header_slots[current_head_slot].status = HEADER_SLOT_UNUSED;
                    }
                    break;
                case HEADER_SLOT_UNUSED:
                    fprintf(stderr, "Illegal state, but should be recoverable\n");
                    break;
                default:
                    fprintf(stderr, "Illegal state in request region thread\n");
                    exit(EXIT_FAILURE);
            }
            free(key);
        }
    }

    free(queue);
    return 0;
}

static void send_put_ack(uint8_t replica_index, volatile replica_ack_t *replica_ack_remote_pointer, uint32_t header_slot,
                         sci_sequence_t ack_sequence, uint32_t version_number, enum replica_ack_type ack_type) {
    volatile replica_ack_t *replica_ack_instance = replica_ack_remote_pointer + (header_slot * REPLICA_COUNT) + replica_index;
    replica_ack_instance->version_number = version_number;
    replica_ack_instance->index_entry_written = -1;
    SCIStoreBarrier(ack_sequence, NO_FLAGS);
    replica_ack_instance->replica_ack_type = ack_type;
    SCIFlush(ack_sequence, NO_FLAGS); //TODO: is this needed?
}

static void send_get_ack_phase1(uint8_t replica_index, volatile replica_ack_t *replica_ack_remote_pointer, uint32_t header_slot,
                                uint32_t key_hash, char *index_region, sci_sequence_t ack_sequence, bool write_back,
                                size_t write_back_offset, char *data_region) {
    volatile replica_ack_t *replica_ack_instance = replica_ack_remote_pointer + (header_slot * REPLICA_COUNT) + replica_index;

    // Fast code is ugly
    uint32_t i;
    if (write_back) {
        for (i = 0; i < INDEX_SLOTS_PR_BUCKET; i++) {
            volatile index_entry_t *entry = &replica_ack_instance->bucket[i];
            *entry = *((index_entry_t *) GET_SLOT_POINTER(index_region, key_hash % INDEX_BUCKETS, i));
            uint32_t transfer_length = entry->key_length + entry->data_length + sizeof(uint32_t);

            if (entry->hash == key_hash &&
                transfer_length <= SPECULATIVE_SIZE) {

                char *data_pointer = data_region + entry->offset;
                volatile char *dest = ((volatile char *) replica_ack_remote_pointer) + ACK_REGION_SLOT_SIZE + write_back_offset;

                for (uint32_t j = 0; j < transfer_length; j++) {
                    dest[j] = data_pointer[j];
                }

                replica_ack_instance->index_entry_written = (int) i;

                goto found; // Only write back the first matching entry
            }
        }
    } else {
        for (i = 0; i < INDEX_SLOTS_PR_BUCKET; i++) {
            replica_ack_instance->bucket[i] = *((index_entry_t *) GET_SLOT_POINTER(index_region, key_hash % INDEX_BUCKETS, i));
            found:
            continue;
        }
    }

    request_region->header_slots[header_slot].status = HEADER_SLOT_UNUSED;
    check_for_errors(ack_sequence);
    replica_ack_instance->replica_ack_type = REPLICA_ACK_SUCCESS;
}

static void send_get_ack_phase2(volatile replica_ack_t *replica_ack_remote_pointer, uint32_t header_slot, const char *data_pointer, uint32_t transfer_length, size_t return_offset, sci_sequence_t ack_sequence) {
    volatile replica_ack_t *replica_ack_instance = replica_ack_remote_pointer + (header_slot * REPLICA_COUNT);
    for (uint32_t i = 0; i < transfer_length; i++) {
        *(((volatile char *) replica_ack_remote_pointer) + ACK_REGION_SLOT_SIZE + return_offset + i) = *(data_pointer + i);
    }
    replica_ack_instance->index_entry_written = -1;
    request_region->header_slots[header_slot].status = HEADER_SLOT_UNUSED;
    check_for_errors(ack_sequence);
    replica_ack_instance->replica_ack_type = REPLICA_ACK_SUCCESS;
}
