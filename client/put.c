#include <sisci_api.h>
#include <stdlib.h>
#include <sched.h>
#include <string.h>
#include "put.h"
#include "sisci_glob_defs.h"
#include "request_region.h"
#include "get_node_id.h"
#include "super_fast_hash.h"
#include "index_data_protocol.h"
#include "request_region_connection.h"

static volatile _Atomic uint32_t free_header_slot = 0;
static volatile _Atomic uint32_t oldest_header_slot = 0;

// wraparound version_number, large enough to avoid replay attacks
static volatile _Atomic uint32_t version_number = 0;

static replica_ack_t *replica_ack;
static ack_slot_t ack_slots[MAX_REQUEST_SLOTS];

static sci_local_segment_t ack_segment;
static sci_map_t ack_map;

static uint8_t client_id;

void init_put(sci_desc_t sd) {
    sci_error_t sci_error;
    SEOE(SCICreateSegment,
         sd,
         &ack_segment,
         ACK_SEGMENT_ID,
         MAX_REQUEST_SLOTS * sizeof(replica_ack_t) * REPLICA_COUNT,
         NO_CALLBACK,
         NO_ARG,
         NO_FLAGS
    );

    SEOE(SCIPrepareSegment,
         ack_segment,
         ADAPTER_NO,
         NO_FLAGS);

    SEOE(SCISetSegmentAvailable,
         ack_segment,
         ADAPTER_NO,
         NO_FLAGS);

    replica_ack = (replica_ack_t *) SCIMapLocalSegment(
            ack_segment,
            &ack_map,
            NO_OFFSET,
            MAX_REQUEST_SLOTS * sizeof(replica_ack_t) * REPLICA_COUNT,
            NO_SUGGESTED_ADDRESS,
            NO_FLAGS,
            &sci_error);

    if (sci_error != SCI_ERR_OK) {
        fprintf(stderr, "Could not map local segment: %s\n", SCIGetErrorString(sci_error));
        exit(EXIT_FAILURE);
    }

    unsigned int node_id = get_node_id();
    if (node_id > UINT8_MAX) {
        fprintf(stderr, "node_id too large!\n");
        exit(EXIT_FAILURE);
    }
    client_id = (uint8_t) node_id;
}

// Critical region function
put_promise_t *put_blocking_until_available_put_request_region_slot(const char *key, uint8_t key_len, void *value, uint32_t value_len) {
    // Check if it is actually in-flight
    // TODO: sched_yield optimize?
    while ((free_header_slot + 1) % MAX_REQUEST_SLOTS == oldest_header_slot); // This complains but I think using atomics should work TODO: try removing volatile

    uint32_t my_header_slot = free_header_slot;
    ack_slot_t *ack_slot = &ack_slots[my_header_slot];

    ack_slot->header_slot_WRITE_ONLY = &request_region->header_slots[my_header_slot];

    clock_gettime(CLOCK_MONOTONIC, &ack_slot->start_time);

    put_promise_t *promise = malloc(sizeof(put_promise_t));
    if (promise == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    promise->result = PUT_PENDING;

    ack_slot->promise = promise;
    ack_slot->key_len = key_len;
    ack_slot->value_len = value_len;
    ack_slot->version_number = ((uint32_t) client_id) << 24 | version_number;
    version_number = (version_number + 1) % MAX_VERSION_NUMBER;

    free_header_slot = (free_header_slot + 1) % MAX_REQUEST_SLOTS;

    if (key_len + value_len > REQUEST_REGION_DATA_SIZE) {
        fprintf(stderr, "illegally large data\n");
        exit(EXIT_FAILURE);
    }

    // Wait for enough space
    while (1) {
        uint32_t used = (free_data_offset + REQUEST_REGION_DATA_SIZE
                         - oldest_data_offset)
                        % REQUEST_REGION_DATA_SIZE;

        uint32_t free_space = REQUEST_REGION_DATA_SIZE - used;

        if (free_space >= (key_len + value_len)) {
            // There's enough space
            break;
        }
    }

    for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        replica_ack_t *replica_ack_instance = replica_ack + (free_header_slot * REPLICA_COUNT) + replica_index;
        replica_ack_instance->replica_ack_type = REPLICA_NOT_ACKED;
        replica_ack_instance->version_number = 0;
    }

    uint32_t starting_offset = free_data_offset;
    volatile char *data_region_start = ((volatile char *) request_region) + sizeof(request_region_t);

    char *hash_data = malloc(key_len + value_len + sizeof(((header_slot_t *) 0)->version_number));
    if (hash_data == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    // First copy the key
    for (uint32_t i = 0; i < key_len; i++) {
        hash_data[i] = key[i];
        data_region_start[free_data_offset] = key[i];
        free_data_offset = (free_data_offset + 1) % REQUEST_REGION_DATA_SIZE;
    }

    // Next copy the data
    for (uint32_t i = 0; i < value_len; i++) {
        hash_data[i + key_len] = ((char *) value)[i];
        data_region_start[free_data_offset] = ((char *) value)[i];
        free_data_offset = (free_data_offset + 1) % REQUEST_REGION_DATA_SIZE;
    }

    // Copy key_hash into the first 4 bytes of hash_data
    memcpy(hash_data + key_len + value_len, &ack_slot->version_number, sizeof(((header_slot_t *) 0)->version_number));

    uint32_t payload_hash = super_fast_hash(hash_data,
                                            (int) (key_len + value_len + sizeof(((header_slot_t *) 0)->version_number)));
    free(hash_data);

    request_region->header_slots[my_header_slot].payload_hash = payload_hash;
    request_region->header_slots[my_header_slot].offset = (size_t) starting_offset;
    request_region->header_slots[my_header_slot].key_length = key_len;
    request_region->header_slots[my_header_slot].value_length = value_len;
    request_region->header_slots[my_header_slot].version_number = ack_slot->version_number;
    request_region->header_slots[my_header_slot].status = HEADER_SLOT_USED_PUT;

    return promise;
}

void *ack_thread(__attribute__((unused)) void *_args) {
    while (1) {
        if (oldest_header_slot == free_header_slot) { continue; }

        ack_slot_t *ack_slot = &ack_slots[oldest_header_slot];

        uint32_t ack_success_count = 0;
        uint32_t ack_count = 0;
        for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
            replica_ack_t *replica_ack_instance = (replica_ack_t *) (replica_ack + (oldest_header_slot * REPLICA_COUNT) + replica_index);
            enum replica_ack_type ack_type = replica_ack_instance->replica_ack_type;
            uint32_t actual = replica_ack_instance->version_number;
            uint32_t expected = ack_slot->version_number;

            // If wrong version number, we do not count as ack
            if (expected != actual) continue;

            if (ack_type != REPLICA_NOT_ACKED)
                ack_count++;

            if (ack_type == REPLICA_ACK_SUCCESS)
                ack_success_count++;
        }

        // If we got a quorum of success acks, count as success
        if (ack_success_count >= (REPLICA_COUNT + 1) / 2) {
            // Success!
            ack_slot->promise->result = PUT_RESULT_SUCCESS;
            goto walk_to_next_slot;
        }

        if (ack_count == REPLICA_COUNT) {
            // Check what errors we have gotten
            enum replica_ack_type replica_ack_type;
            bool mix = false;
            for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
                replica_ack_t *replica_ack_instance = replica_ack + (oldest_header_slot * REPLICA_COUNT) + replica_index;

                if (replica_index == 0) {
                    replica_ack_type = replica_ack_instance->replica_ack_type;
                    continue;
                }
                if (replica_ack_instance->replica_ack_type != replica_ack_type) {
                    mix = true;
                }
            }

            if (mix) {
                ack_slot->promise->result = PUT_RESULT_ERROR_MIX;
                goto walk_to_next_slot;
            }

            switch (replica_ack_type) {
                case REPLICA_ACK_ERROR_OUT_OF_SPACE:
                    ack_slot->promise->result = PUT_RESULT_ERROR_OUT_OF_SPACE;
                    goto walk_to_next_slot;
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

        if (((end_p.tv_sec - ack_slot->start_time.tv_sec) * 1000000000L + (end_p.tv_nsec - ack_slot->start_time.tv_nsec)) >= PUT_TIMEOUT_NS) {
            ack_slot->promise->result = PUT_RESULT_ERROR_TIMEOUT;
            printf("TIMEOUT!\n");
            goto walk_to_next_slot;
        }

        // Not timeout, let it live on!
        continue;

        walk_to_next_slot:
        ack_slot->header_slot_WRITE_ONLY->status = HEADER_SLOT_UNUSED;
        oldest_header_slot = (oldest_header_slot + 1) % MAX_REQUEST_SLOTS;
        oldest_data_offset = (oldest_data_offset + ack_slot->value_len + ack_slot->key_len) % REQUEST_REGION_DATA_SIZE;
    }

    return NULL;
}
