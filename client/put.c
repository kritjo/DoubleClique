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
#include "ack_region.h"

// wraparound version_number, large enough to avoid replay attacks
static volatile _Atomic uint32_t version_number = 0;

static uint8_t client_id;

void init_put(sci_desc_t sd) {
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

    uint32_t my_version_number = ((uint32_t) client_id) << 24 | version_number;
    version_number = (version_number + 1) % MAX_VERSION_NUMBER;

    ack_slot_t *ack_slot = get_ack_slot_blocking(PUT_PENDING, key_len, value_len, my_version_number);

    for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        replica_ack_t *replica_ack_instance = replica_ack + (free_header_slot * REPLICA_COUNT) + replica_index;
        replica_ack_instance->replica_ack_type = REPLICA_NOT_ACKED;
        replica_ack_instance->version_number = 0;
    }

    uint32_t starting_offset = ack_slot->starting_data_offset;
    uint32_t current_offset = starting_offset;
    volatile char *data_region_start = ((volatile char *) request_region) + sizeof(request_region_t);

    char *hash_data = malloc(key_len + value_len + sizeof(((header_slot_t *) 0)->version_number));
    if (hash_data == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

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

    // Copy key_hash into the first 4 bytes of hash_data
    memcpy(hash_data + key_len + value_len, &ack_slot->version_number, sizeof(((header_slot_t *) 0)->version_number));

    uint32_t payload_hash = super_fast_hash(hash_data,
                                            (int) (key_len + value_len + sizeof(((header_slot_t *) 0)->version_number)));
    free(hash_data);

    ack_slot->header_slot_WRITE_ONLY->payload_hash = payload_hash;
    ack_slot->header_slot_WRITE_ONLY->offset = (size_t) starting_offset;
    ack_slot->header_slot_WRITE_ONLY->key_length = key_len;
    ack_slot->header_slot_WRITE_ONLY->value_length = value_len;
    ack_slot->header_slot_WRITE_ONLY->version_number = ack_slot->version_number;
    ack_slot->header_slot_WRITE_ONLY->status = HEADER_SLOT_USED_PUT;

    return ack_slot->promise;
}
