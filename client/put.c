#include <sisci_api.h>
#include <stdlib.h>
#include <sched.h>
#include <string.h>
#include "put.h"
#include "profiling.h"
#include "sisci_glob_defs.h"
#include "request_region.h"
#include "get_node_id.h"
#include "super_fast_hash.h"
#include "index_data_protocol.h"
#include "request_region_connection.h"
#include "ack_region.h"
#include "sequence.h"

// wraparound version_number, large enough to avoid replay attacks
static volatile _Atomic uint32_t version_number = 0;

static uint8_t client_id;

void init_put(void) {
    unsigned int node_id = get_node_id();
    if (node_id > UINT8_MAX) {
        fprintf(stderr, "node_id too large!\n");
        exit(EXIT_FAILURE);
    }
    client_id = (uint8_t) node_id;
}

request_promise_t *put_blocking_until_available_put_request_region_slot(const char *key, uint8_t key_len, void *value, uint32_t value_len) {
    if (key_len + value_len > REQUEST_REGION_DATA_SIZE) {
        fprintf(stderr, "illegally large data\n");
        exit(EXIT_FAILURE);
    }

    PROFILE_START("put_blocking");
    uint32_t my_version_number = ((uint32_t) client_id) << 24 | version_number;
    version_number = (version_number + 1) % MAX_VERSION_NUMBER;

    PROFILE_START("get_ack_slot");
    ack_slot_t *ack_slot = get_ack_slot_blocking(PUT, key_len, value_len, key_len+value_len, 0, my_version_number, NULL);
    PROFILE_END("get_ack_slot");

    uint32_t starting_offset = ack_slot->starting_data_offset;
    uint32_t current_offset = starting_offset;
    volatile char *data_region_start = ((volatile char *) request_region) + sizeof(request_region_t);

    char *hash_data = malloc(key_len + value_len + sizeof(((header_slot_t *) 0)->version_number));
    if (hash_data == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    PROFILE_START("copying");
    // First copy the key
    for (uint32_t i = 0; i < key_len; i++) {
        hash_data[i] = key[i];
        data_region_start[current_offset] = key[i];
        current_offset = (current_offset + 1) % REQUEST_REGION_DATA_SIZE;
    }

    // Next copy the data
    for (uint32_t i = 0; i < value_len; i++) {
        hash_data[i + key_len] = ((char *) value)[i];
        data_region_start[current_offset] = ((char *) value)[i];
        current_offset = (current_offset + 1) % REQUEST_REGION_DATA_SIZE;
    }

    // Copy version number into the first 4 bytes of hash_data
    memcpy(hash_data + key_len + value_len, &ack_slot->version_number, sizeof(((header_slot_t *) 0)->version_number));
    PROFILE_END("copying");

    PROFILE_START("hash");
    uint32_t payload_hash = super_fast_hash(hash_data,
                                            (uint32_t) (key_len + value_len + sizeof(((header_slot_t *) 0)->version_number)));
    PROFILE_END("hash");
    free(hash_data);

    PROFILE_START("send_request_region");
    send_request_region_slot(
        ack_slot->header_slot_WRITE_ONLY,
        key_len,
        value_len,
        ack_slot->version_number,
        (size_t) starting_offset,
        0,
        0,
        payload_hash,
        HEADER_SLOT_USED_PUT
    );
    PROFILE_END("send_request_region");

    PROFILE_END("put_blocking");
    print_profile_report(stdout);
    return ack_slot->promise;
}

bool consume_put_ack_slot(ack_slot_t *ack_slot) {
    uint32_t ack_success_count = 0;
    uint32_t ack_count = 0;
    for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        replica_ack_t *replica_ack_instance = ack_slot->replica_ack_instances[replica_index];
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
        ack_slot->promise->result = PROMISE_SUCCESS;
        return true;
    }

    if (ack_count == REPLICA_COUNT) {
        // Check what errors we have gotten
        enum replica_ack_type replica_ack_type;
        bool mix = false;
        for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
            replica_ack_t *replica_ack_instance = ack_slot->replica_ack_instances[replica_index];

            if (replica_index == 0) {
                replica_ack_type = replica_ack_instance->replica_ack_type;
                continue;
            }
            if (replica_ack_instance->replica_ack_type != replica_ack_type) {
                mix = true;
            }
        }

        if (mix) {
            ack_slot->promise->result = PROMISE_ERROR_MIX;
            return true;
        }

        switch (replica_ack_type) {
            case REPLICA_ACK_ERROR_OUT_OF_SPACE:
                ack_slot->promise->result = PROMISE_ERROR_OUT_OF_SPACE;
                return true;
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
        ack_slot->promise->result = PROMISE_TIMEOUT;
        return true;
    }

    return false;
}
