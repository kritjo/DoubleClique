#include <sisci_api.h>
#include <stdlib.h>
#include <sched.h>
#include "put.h"
#include "sisci_glob_defs.h"
#include "put_request_region.h"
#include "get_node_id.h"

static volatile _Atomic uint32_t free_header_slot = 0;
static volatile _Atomic uint32_t oldest_header_slot = 0;

static volatile _Atomic uint32_t free_data_offset = 0;
static volatile _Atomic uint32_t oldest_data_offset = 0;

static enum replica_ack_type *replica_ack;
static put_ack_slot_t put_ack_slots[MAX_PUT_REQUEST_SLOTS];

static sci_local_segment_t put_ack_segment;
static sci_map_t put_ack_map;

static volatile put_request_region_t *put_request_region;

static void connect_to_put_request_region(sci_desc_t sd);

void init_put(sci_desc_t sd) {
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

    connect_to_put_request_region(sd);
}

static void connect_to_put_request_region(sci_desc_t sd) {
    sci_error_t sci_error;

    sci_remote_segment_t put_request_segment;
    sci_map_t put_request_map;

    SEOE(SCIConnectSegment,
         sd,
         &put_request_segment,
         DIS_BROADCAST_NODEID_GROUP_ALL,
         PUT_REQUEST_SEGMENT_ID,
         ADAPTER_NO,
         NO_CALLBACK,
         NO_ARG,
         SCI_INFINITE_TIMEOUT,
         SCI_FLAG_BROADCAST);

    put_request_region = (volatile put_request_region_t*) SCIMapRemoteSegment(put_request_segment,
                                                                               &put_request_map,
                                                                               NO_OFFSET,
                                                                               PUT_REQUEST_REGION_SIZE,
                                                                               NO_SUGGESTED_ADDRESS,
                                                                               NO_FLAGS,
                                                                               &sci_error);

    if (sci_error != SCI_ERR_OK) {
        fprintf(stderr, "SCIMapLocalSegment failed: %s\n", SCIGetErrorString(sci_error));
        exit(EXIT_FAILURE);
    }

    unsigned int node_id = get_node_id();
    if (node_id > UINT8_MAX) {
        fprintf(stderr, "node_id too large!\n");
        exit(EXIT_FAILURE);
    }
    put_request_region->sisci_node_id = (uint8_t) node_id;

    for (uint32_t i = 0; i < MAX_PUT_REQUEST_SLOTS; i++) {
        put_request_region->header_slots[i].status = HEADER_SLOT_UNUSED;
    }

    put_request_region->status = PUT_REQUEST_REGION_ACTIVE;
}

// Critical region function
put_promise_t *put_blocking(const char *key, uint8_t key_len, void *value, uint32_t value_len) {
    // Check if it is actually in-flight
    // TODO: sched_yield optimize?
    while ((free_header_slot + 1) % MAX_PUT_REQUEST_SLOTS == oldest_header_slot); // This complains but I think using atomics should work TODO: try removing volatile


    for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        *(replica_ack + (free_header_slot * REPLICA_COUNT) + replica_index) = REPLICA_NOT_ACKED;
    }

    uint32_t my_header_slot = free_header_slot;
    put_ack_slot_t *put_ack_slot = &put_ack_slots[my_header_slot];

    put_ack_slot->header_slot = &put_request_region->header_slots[my_header_slot];

    clock_gettime(CLOCK_MONOTONIC, &put_ack_slot->start_time);

    put_promise_t *promise = malloc(sizeof(put_promise_t));
    if (promise == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    promise->result = PUT_PENDING;
    promise->header_slot = my_header_slot;

    put_ack_slot->promise = promise;
    put_ack_slot->key_len = key_len;
    put_ack_slot->value_len = value_len;

    free_header_slot = (free_header_slot + 1) % MAX_PUT_REQUEST_SLOTS;

    if (key_len + value_len > PUT_REQUEST_REGION_DATA_SIZE) {
        fprintf(stderr, "illegally large data\n");
        exit(EXIT_FAILURE);
    }

    // Wait for enough space
    while (1) {
        uint32_t used = (free_data_offset + PUT_REQUEST_REGION_DATA_SIZE
                         - oldest_data_offset)
                        % PUT_REQUEST_REGION_DATA_SIZE;

        uint32_t free_space = PUT_REQUEST_REGION_DATA_SIZE - used;

        if (free_space >= (key_len + value_len)) {
            // There's enough space
            break;
        }
    }

    put_request_region->header_slots[my_header_slot].offset = (size_t) free_data_offset;
    put_request_region->header_slots[my_header_slot].key_length = key_len;
    put_request_region->header_slots[my_header_slot].value_length = value_len;
    put_request_region->header_slots[my_header_slot].version_number = 1; //TODO: compute vnr

    uint32_t offset = free_data_offset;
    volatile char *data_region_start = ((volatile char *) put_request_region) + sizeof(put_request_region_t);

    // First copy the key
    for (uint32_t i = 0; i < key_len; i++) {
        data_region_start[offset] = key[i];
        offset = (offset + 1) % PUT_REQUEST_REGION_DATA_SIZE;
    }

    // Next copy the data
    for (uint32_t i = 0; i < value_len; i++) {
        data_region_start[offset] = ((char *) value)[i];
        offset = (offset + 1) % PUT_REQUEST_REGION_DATA_SIZE;
    }

    free_data_offset = (free_data_offset + key_len + value_len) % PUT_REQUEST_REGION_DATA_SIZE;

    // Do not set until now, as we first want the replica to poll now!
    put_request_region->header_slots[my_header_slot].status = HEADER_SLOT_USED;
    return promise;
}

void *put_ack_thread(__attribute__((unused)) void *_args) {
    while (1) {
        if (oldest_header_slot == free_header_slot) { continue; }

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
        if (ack_success_count >= (REPLICA_COUNT + 1) / 2) {
            // Success!
            put_ack_slot->promise->result = PUT_RESULT_SUCCESS;
            goto walk_to_next_slot;
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
                goto walk_to_next_slot;
            }

            switch (replica_ack_type) {
                case REPLICA_ACK_ERROR_OUT_OF_SPACE:
                    put_ack_slot->promise->result = PUT_RESULT_ERROR_OUT_OF_SPACE;
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

        if (end_p.tv_sec - put_ack_slot->start_time.tv_sec != 0 ||
            end_p.tv_nsec - put_ack_slot->start_time.tv_nsec >= PUT_TIMEOUT_NS) {
            put_ack_slot->promise->result = PUT_RESULT_ERROR_TIMEOUT;
            goto walk_to_next_slot;
        }

        // Not timeout, let it live on!
        continue;

        walk_to_next_slot:
        put_ack_slot->header_slot->status = HEADER_SLOT_UNUSED;
        oldest_header_slot = (oldest_header_slot + 1) % MAX_PUT_REQUEST_SLOTS;
        oldest_data_offset = (oldest_data_offset + put_ack_slot->value_len + put_ack_slot->key_len) % PUT_REQUEST_REGION_DATA_SIZE;
    }

    return NULL;
}
