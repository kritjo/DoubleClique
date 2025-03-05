#ifndef DOUBLECLIQUE_REQUEST_REGION_H
#define DOUBLECLIQUE_REQUEST_REGION_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <sisci_types.h>
#include "sisci_glob_defs.h"
#include "index_data_protocol.h"

#define PIPE_SIZE 20000 //bytes
#define MAX_REQUEST_SLOTS (PIPE_SIZE/MIN_SIZE_ELEMENT)
#define REQUEST_SEGMENT_ID 1

typedef enum {
    REQUEST_REGION_INACTIVE,
    PUT_REQUEST_REGION_ACTIVE
} put_request_region_status_t;

enum replica_ack_type {
    REPLICA_NOT_ACKED,
    REPLICA_ACK_SUCCESS,
    REPLICA_ACK_ERROR_OUT_OF_SPACE
};

typedef struct {
    enum replica_ack_type replica_ack_type;
    uint32_t version_number;
} replica_ack_t;

enum header_slot_status {
    HEADER_SLOT_UNUSED,
    HEADER_SLOT_USED_PUT,
    HEADER_SLOT_USED_GET
};

#define MAX_VERSION_NUMBER 0x1000000
// The largest version number is 2**24 as the top 8 bits are used for the replica node id

typedef struct {
    uint8_t key_length;
    uint32_t value_length;
    uint32_t version_number;
    size_t offset;
    uint32_t payload_hash; // The payload hash is just hash(keyhash valuehash)
    enum header_slot_status status;
} header_slot_t;

typedef struct {
    uint8_t sisci_node_id; // Only valid when status != 0
    header_slot_t header_slots[MAX_REQUEST_SLOTS];
    put_request_region_status_t status;
} request_region_t;

#define REQUEST_REGION_DATA_SIZE 2017136
#define REQUEST_REGION_SIZE (sizeof(request_region_t) + REQUEST_REGION_DATA_SIZE)
#define ACK_REGION_SIZE (MAX_REQUEST_SLOTS * sizeof(replica_ack_t) * REPLICA_COUNT)
#define ACK_SEGMENT_ID 2

#endif //DOUBLECLIQUE_REQUEST_REGION_H
