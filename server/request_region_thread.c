#include "request_region_thread.h"

#include <threads.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <sisci_api.h>
#include <string.h>
#include <immintrin.h>
#include "request_region.h"
#include "sisci_glob_defs.h"
#include "super_fast_hash.h"
#include "avx_cpy.h"

#include "index_data_protocol.h"

#define BUDDY_ALLOC_IMPLEMENTATION

#include "buddy_alloc.h"
#include "request_region_utils.h"
#include "garbage_collection_queue.h"
#include "garbage_collection.h"
#include "profiler.h"
#include "profiler_metrics.h"
#include "profiler_report_layout.h"

static request_region_t *request_region;
static struct buddy *buddy = NULL;

static void send_put_ack(uint8_t replica_index, volatile replica_ack_t *replica_ack_remote_pointer, uint32_t header_slot,
                         uint32_t version_number, enum replica_ack_type ack_type);

static void send_get_ack_phase1(uint8_t replica_index, volatile replica_ack_t *replica_ack_remote_pointer, uint32_t header_slot,
                                uint32_t key_hash, char *index_region, bool write_back, size_t write_back_offset, char *data_region);

static void send_get_ack_phase2(volatile replica_ack_t *replica_ack_remote_pointer, uint32_t header_slot, const char *data_pointer,
                                uint32_t transfer_length, size_t return_offset);

static void put(request_region_poller_thread_args_t *args, header_slot_t slot, uint32_t current_head_slot,
         volatile replica_ack_t *replica_ack, uint32_t key_hash, char *key, size_t offset, queue_t *queue) {
    uint64_t put_total_start_ns = perf_now_ns();
#define FINISH_PUT_RETURN() \
    do { \
        perf_record_ns(PROF_SERVER_PUT_TOTAL, perf_now_ns() - put_total_start_ns); \
        return; \
    } while (0)

    char *data_slot_start = ((char *) request_region) + sizeof(request_region_t);

    // Use a single allocation for both data and hash_data
    size_t total_size = slot.value_length + slot.key_length + slot.value_length + sizeof(uint32_t);
    char *combined_buffer = malloc(total_size);
    if (combined_buffer == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    char *data = combined_buffer;
    char *hash_data = combined_buffer + slot.value_length;

    uint64_t read_copy_start_ns = perf_now_ns();

    // Copy key to hash_data
    memcpy(hash_data, key, slot.key_length);

    // Copy value to both data and hash_data
    for (uint32_t i = 0; i < slot.value_length; i++) {
        data[i] = data_slot_start[offset];
        hash_data[i + slot.key_length] = data[i];  // Reuse the value we just read
        offset = (offset + 1) % REQUEST_REGION_DATA_SIZE;
    }

    memcpy(hash_data + slot.key_length + slot.value_length, &slot.version_number, sizeof(uint32_t));
    perf_record_ns_bytes(
        PROF_SERVER_PUT_READ_COPY,
        perf_now_ns() - read_copy_start_ns,
        (uint64_t) slot.key_length + slot.value_length + sizeof(uint32_t)
    );

    uint64_t hash_verify_start_ns = perf_now_ns();
    uint32_t payload_hash = super_fast_hash(hash_data, (uint32_t) (slot.key_length + slot.value_length + sizeof(uint32_t)));
    perf_record_ns_bytes(
        PROF_SERVER_PUT_HASH_VERIFY,
        perf_now_ns() - hash_verify_start_ns,
        (uint64_t) slot.key_length + slot.value_length + sizeof(uint32_t)
    );

    if (payload_hash != slot.payload_hash) {
        uint64_t mark_unused_start_ns = perf_now_ns();
        request_region->header_slots[current_head_slot].status = HEADER_SLOT_UNUSED;
        perf_record_ns(PROF_SERVER_PUT_MARK_HEADER_UNUSED, perf_now_ns() - mark_unused_start_ns);
        free(combined_buffer);
        FINISH_PUT_RETURN();
    }

    uint64_t index_lookup_start_ns = perf_now_ns();
    index_entry_t *index_slot = existing_slot_for_key(args->index_region, args->data_region, key_hash,
                                                      slot.key_length, key);
    perf_record_ns(PROF_SERVER_PUT_INDEX_LOOKUP, perf_now_ns() - index_lookup_start_ns);

    void *new_data_slot;
    uint64_t alloc_start_ns = perf_now_ns();
    // If we did not find an existing slot, we are not updating, but fresh inserting
    if (index_slot == NULL) {
        new_data_slot = buddy_malloc(buddy, slot.key_length + slot.value_length + sizeof(((header_slot_t *) 0)->version_number));
        if (new_data_slot == NULL) {
            perf_record_ns(PROF_SERVER_PUT_ALLOC, perf_now_ns() - alloc_start_ns);
            uint64_t retry_lookup_start_ns = perf_now_ns();
            existing_slot_for_key(args->index_region, args->data_region, key_hash,
                                  slot.key_length, key);
            perf_record_ns(PROF_SERVER_PUT_RETRY_INDEX_LOOKUP, perf_now_ns() - retry_lookup_start_ns);
            uint64_t ack_start_ns = perf_now_ns();
            send_put_ack(args->replica_number, replica_ack, current_head_slot, slot.version_number, REPLICA_ACK_ERROR_OUT_OF_SPACE);
            perf_record_ns(PROF_SERVER_PUT_ACK, perf_now_ns() - ack_start_ns);
            uint64_t mark_unused_start_ns = perf_now_ns();
            request_region->header_slots[current_head_slot].status = HEADER_SLOT_UNUSED;
            perf_record_ns(PROF_SERVER_PUT_MARK_HEADER_UNUSED, perf_now_ns() - mark_unused_start_ns);
            free(data);
            FINISH_PUT_RETURN();
        }

        // Try to find an available slot
        uint64_t find_slot_start_ns = perf_now_ns();
        index_slot = find_available_index_slot(args->index_region, key_hash);
        perf_record_ns(PROF_SERVER_PUT_FIND_FREE_INDEX_SLOT, perf_now_ns() - find_slot_start_ns);

        // If we do not find one, there is no space left
        if (index_slot == NULL) {
            perf_record_ns(PROF_SERVER_PUT_ALLOC, perf_now_ns() - alloc_start_ns);
            uint64_t ack_start_ns = perf_now_ns();
            send_put_ack(args->replica_number, replica_ack, current_head_slot, slot.version_number, REPLICA_ACK_ERROR_OUT_OF_SPACE);
            perf_record_ns(PROF_SERVER_PUT_ACK, perf_now_ns() - ack_start_ns);
            uint64_t mark_unused_start_ns = perf_now_ns();
            request_region->header_slots[current_head_slot].status = HEADER_SLOT_UNUSED;
            perf_record_ns(PROF_SERVER_PUT_MARK_HEADER_UNUSED, perf_now_ns() - mark_unused_start_ns);
            free(data);
            FINISH_PUT_RETURN();
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
                perf_record_ns(PROF_SERVER_PUT_ALLOC, perf_now_ns() - alloc_start_ns);
                uint64_t ack_start_ns = perf_now_ns();
                send_put_ack(args->replica_number, replica_ack, current_head_slot, slot.version_number, REPLICA_ACK_ERROR_OUT_OF_SPACE);
                perf_record_ns(PROF_SERVER_PUT_ACK, perf_now_ns() - ack_start_ns);
                uint64_t mark_unused_start_ns = perf_now_ns();
                request_region->header_slots[current_head_slot].status = HEADER_SLOT_UNUSED;
                perf_record_ns(PROF_SERVER_PUT_MARK_HEADER_UNUSED, perf_now_ns() - mark_unused_start_ns);
                free(data);
                FINISH_PUT_RETURN();
            }
        } else {
            uint64_t gc_enqueue_start_ns = perf_now_ns();
            queue_item_t queue_item;
            queue_item.buddy_allocated_addr = old_data_slot;

            uint64_t gc_get_clock_start_ns = perf_now_ns();
            clock_gettime(CLOCK_MONOTONIC, &queue_item.t);
            perf_record_ns(PROF_SERVER_PUT_GC_GET_CLOCK, perf_now_ns() - gc_get_clock_start_ns);
            long long nsec =
                (long long)queue_item.t.tv_nsec + NS_TO_COLLECTION;

            if (nsec >= 1000000000LL) {
                nsec -= 1000000000LL;
                queue_item.t.tv_sec++;
            }

            queue_item.t.tv_nsec = (long)nsec;

            uint64_t gc_enqueue_call_start_ns = perf_now_ns();
            enqueue(queue, queue_item);
            perf_record_ns(PROF_SERVER_PUT_GC_ENQUEUE_CALL, perf_now_ns() - gc_enqueue_call_start_ns);
            perf_record_ns(PROF_SERVER_PUT_GC_ENQUEUE, perf_now_ns() - gc_enqueue_start_ns);
        }
    }
    perf_record_ns(PROF_SERVER_PUT_ALLOC, perf_now_ns() - alloc_start_ns);

    uint64_t insert_start_ns = perf_now_ns();
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
    perf_record_ns_bytes(
        PROF_SERVER_PUT_INSERT,
        perf_now_ns() - insert_start_ns,
        (uint64_t) slot.key_length + slot.value_length + sizeof(uint32_t)
    );
    free(data);

    // Need to do this before sending ack in case of race
    uint64_t mark_unused_start_ns = perf_now_ns();
    request_region->header_slots[current_head_slot].status = HEADER_SLOT_UNUSED;
    perf_record_ns(PROF_SERVER_PUT_MARK_HEADER_UNUSED, perf_now_ns() - mark_unused_start_ns);

    uint64_t ack_start_ns = perf_now_ns();
    send_put_ack(args->replica_number, replica_ack, current_head_slot, slot.version_number, REPLICA_ACK_SUCCESS);
    perf_record_ns(PROF_SERVER_PUT_ACK, perf_now_ns() - ack_start_ns);
    FINISH_PUT_RETURN();

#undef FINISH_PUT_RETURN
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
    uint64_t last_report_ns = perf_now_ns();
    size_t server_report_root_count = 0;
    size_t server_metric_name_count = 0;
    const perf_report_node_t *server_report_roots = server_profiler_report_roots(&server_report_root_count);
    const char *const *server_metric_names = server_profiler_metric_names(&server_metric_name_count);

    //Enter main loop
    while (1) {
        if (request_region->status == REQUEST_REGION_INACTIVE) {
            _mm_pause();
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

            connected_to_client = true;
        }

        /* It might seem like this will have a lot of overhead to constantly poll on every slot, it might add a little
         * bit of overhead when just waiting for a single put, but when experiencing constant writing, it will implicitly
         * sync up, as when we hit a slot that we actually need to do something with, the next in the loop will be ready
         * when we continue;.
         */
        for (uint32_t current_head_slot = 0; current_head_slot < REQUEST_SLOTS; current_head_slot++) {
            header_slot_t slot = request_region->header_slots[current_head_slot];
            if (slot.status == HEADER_SLOT_UNUSED) continue;
            uint64_t slot_total_start_ns = perf_now_ns();

            char *data_slot_start = ((char *) request_region) + sizeof(request_region_t);

            size_t offset = slot.offset;

            char *key = malloc(slot.key_length + 1);
            if (key == NULL) {
                perror("malloc");
                exit(EXIT_FAILURE);
            }

            uint64_t key_copy_start_ns = perf_now_ns();
            for (uint32_t i = 0; i < slot.key_length; i++) {
                key[i] = data_slot_start[offset];
                offset = (offset + 1) % REQUEST_REGION_DATA_SIZE;
            }
            perf_record_ns_bytes(PROF_SERVER_POLL_KEY_COPY, perf_now_ns() - key_copy_start_ns, slot.key_length);
            key[slot.key_length] = '\0';

            uint64_t key_hash_start_ns = perf_now_ns();
            uint32_t key_hash = super_fast_hash((void *) key, slot.key_length);
            perf_record_ns_bytes(PROF_SERVER_POLL_KEY_HASH, perf_now_ns() - key_hash_start_ns, slot.key_length);

            // UP until this point is equal for both request types.

            switch (slot.status) {
                case HEADER_SLOT_USED_PUT:
                    put(args, slot, current_head_slot, replica_ack, key_hash, key, offset, queue);
                    break;
                case HEADER_SLOT_USED_GET_PHASE1:
                    // Now we need to ship our index entries for this keyhash back
                    if (key_hash != slot.payload_hash) {
                        break;
                    }
                    send_get_ack_phase1(args->replica_number, replica_ack, current_head_slot, key_hash,
                                        args->index_region, args->replica_number == slot.replica_write_back_hint,
                                        slot.return_offset, args->data_region);
                    break;
                case HEADER_SLOT_USED_GET_PHASE2:
                    if (slot.replica_write_back_hint == args->replica_number) {
                        send_get_ack_phase2(replica_ack, current_head_slot, ((char *) args->data_region) + slot.offset,
                                            slot.key_length + slot.value_length + sizeof(uint32_t), slot.return_offset);
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
            perf_increment(PROF_SERVER_REQUESTS_PROCESSED, 1);
            perf_record_ns(PROF_SERVER_POLL_SLOT_TOTAL, perf_now_ns() - slot_total_start_ns);
        }

        uint64_t now_ns = perf_now_ns();
        if (now_ns - last_report_ns >= 5000000000ULL) {
            perf_print_report(server_profiler_report_title(),
                              server_report_roots,
                              server_report_root_count,
                              server_metric_names,
                              server_metric_name_count,
                              true);
            last_report_ns = now_ns;
        }
    }

    free(queue);
    return 0;
}

static void send_put_ack(uint8_t replica_index, volatile replica_ack_t *replica_ack_remote_pointer, uint32_t header_slot,
                         uint32_t version_number, enum replica_ack_type ack_type) {
    volatile replica_ack_t *replica_ack_instance = replica_ack_remote_pointer + (header_slot * REPLICA_COUNT) + replica_index;
    replica_ack_instance->version_number = version_number;
    replica_ack_instance->index_entry_written = -1;
    replica_ack_instance->replica_ack_type = ack_type;
    _mm_sfence();
}

static void send_get_ack_phase1(uint8_t replica_index, volatile replica_ack_t *replica_ack_remote_pointer, uint32_t header_slot,
                                uint32_t key_hash, char *index_region, bool write_back,
                                size_t write_back_offset, char *data_region) {
    uint64_t total_start_ns = perf_now_ns();
    uint64_t copy_bucket_ns = 0;
    uint64_t copy_bucket_plain_ns = 0;
    uint64_t copy_bucket_writeback_scan_ns = 0;
    uint64_t copy_bucket_writeback_copy_to_ack_ns = 0;
    uint64_t copy_bucket_writeback_match_check_ns = 0;
    uint64_t writeback_copy_ns = 0;
    uint64_t copy_bucket_bytes = 0;
    uint64_t copy_bucket_plain_bytes = 0;
    uint64_t copy_bucket_writeback_scan_bytes = 0;
    uint64_t copy_bucket_writeback_copy_to_ack_bytes = 0;
    uint64_t writeback_bytes = 0;

    volatile replica_ack_t *replica_ack_instance = replica_ack_remote_pointer + (header_slot * REPLICA_COUNT) + replica_index;
    index_entry_t *bucket_base = (index_entry_t *) GET_SLOT_POINTER(index_region, key_hash % INDEX_BUCKETS, 0);

    if (!write_back) {
        // Fast path - just copy
        uint64_t copy_start_ns = perf_now_ns();
        for (uint32_t i = 0; i < INDEX_SLOTS_PR_BUCKET; i++) {
            replica_ack_instance->bucket[i] = *((index_entry_t *) GET_SLOT_POINTER(index_region, key_hash % INDEX_BUCKETS, i));
        }
        uint64_t copy_elapsed_ns = perf_now_ns() - copy_start_ns;
        uint64_t copied_bytes = (uint64_t) INDEX_SLOTS_PR_BUCKET * sizeof(index_entry_t);
        copy_bucket_ns += copy_elapsed_ns;
        copy_bucket_plain_ns += copy_elapsed_ns;
        copy_bucket_bytes += copied_bytes;
        copy_bucket_plain_bytes += copied_bytes;
    } else {
        // Keep local copies to avoid volatile read-after-write
        replica_ack_instance->index_entry_written = -1;

        uint64_t copy_start_ns = perf_now_ns();
        for (int i = 0; i < INDEX_SLOTS_PR_BUCKET; i++) {
            uint64_t copy_to_ack_start_ns = perf_now_ns();
            index_entry_t local_entry = bucket_base[i];
            replica_ack_instance->bucket[i] = local_entry;
            copy_bucket_bytes += sizeof(index_entry_t);
            copy_bucket_writeback_scan_bytes += sizeof(index_entry_t);
            copy_bucket_writeback_copy_to_ack_bytes += sizeof(index_entry_t);
            copy_bucket_writeback_copy_to_ack_ns += perf_now_ns() - copy_to_ack_start_ns;

            // Now use local_entry instead of reading from volatile memory
            uint64_t match_check_start_ns = perf_now_ns();
            uint32_t transfer_length = local_entry.key_length + local_entry.data_length + sizeof(uint32_t);
            bool should_writeback =
                local_entry.hash == key_hash &&
                transfer_length <= SPECULATIVE_SIZE &&
                replica_ack_instance->index_entry_written == -1;
            copy_bucket_writeback_match_check_ns += perf_now_ns() - match_check_start_ns;

            if (should_writeback) {

                char *data_pointer = data_region + local_entry.offset;
                volatile char *dest = ((volatile char *) replica_ack_remote_pointer) + ACK_REGION_SLOT_SIZE + write_back_offset;

                uint64_t wb_start_ns = perf_now_ns();
                memcpy_nt_avx2(dest, data_pointer, transfer_length, CHUNK_SIZE);
                writeback_copy_ns += perf_now_ns() - wb_start_ns;
                writeback_bytes += transfer_length;
                replica_ack_instance->index_entry_written = i;
                break;  // Early exit
            }
        }
        uint64_t copy_elapsed_ns = perf_now_ns() - copy_start_ns;
        copy_bucket_ns += copy_elapsed_ns;
        copy_bucket_writeback_scan_ns += copy_elapsed_ns;
    }

    uint64_t ack_finalize_start_ns = perf_now_ns();
    request_region->header_slots[header_slot].status = HEADER_SLOT_UNUSED;
    replica_ack_instance->replica_ack_type = REPLICA_ACK_SUCCESS;
    perf_record_ns(PROF_SERVER_GET1_ACK_FINALIZE, perf_now_ns() - ack_finalize_start_ns);

    perf_record_ns_bytes(PROF_SERVER_GET1_COPY_BUCKET, copy_bucket_ns, copy_bucket_bytes);
    perf_record_ns_bytes(PROF_SERVER_GET1_COPY_BUCKET_PLAIN, copy_bucket_plain_ns, copy_bucket_plain_bytes);
    perf_record_ns_bytes(PROF_SERVER_GET1_COPY_BUCKET_WRITEBACK_SCAN, copy_bucket_writeback_scan_ns, copy_bucket_writeback_scan_bytes);
    perf_record_ns_bytes(PROF_SERVER_GET1_COPY_BUCKET_WRITEBACK_COPY_TO_ACK, copy_bucket_writeback_copy_to_ack_ns, copy_bucket_writeback_copy_to_ack_bytes);
    perf_record_ns(PROF_SERVER_GET1_COPY_BUCKET_WRITEBACK_MATCH_CHECK, copy_bucket_writeback_match_check_ns);
    perf_record_ns_bytes(PROF_SERVER_GET1_WRITEBACK_COPY, writeback_copy_ns, writeback_bytes);
    perf_record_ns(PROF_SERVER_GET1_ACK_TOTAL, perf_now_ns() - total_start_ns);
}

static void send_get_ack_phase2(volatile replica_ack_t *replica_ack_remote_pointer, uint32_t header_slot,
                                const char *data_pointer, uint32_t transfer_length, size_t return_offset) {
    uint64_t total_start_ns = perf_now_ns();
    volatile replica_ack_t *replica_ack_instance = replica_ack_remote_pointer + (header_slot * REPLICA_COUNT);

    volatile char *dest = ((volatile char *) replica_ack_remote_pointer) + ACK_REGION_SLOT_SIZE + return_offset;
    uint64_t copy_start_ns = perf_now_ns();
    memcpy_nt_avx2(dest, data_pointer, transfer_length, CHUNK_SIZE);
    perf_record_ns_bytes(PROF_SERVER_GET2_COPY, perf_now_ns() - copy_start_ns, transfer_length);
    replica_ack_instance->index_entry_written = -1;
    request_region->header_slots[header_slot].status = HEADER_SLOT_UNUSED;
    replica_ack_instance->replica_ack_type = REPLICA_ACK_SUCCESS;
    perf_record_ns(PROF_SERVER_GET2_ACK_TOTAL, perf_now_ns() - total_start_ns);
}
