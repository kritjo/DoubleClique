#ifndef DOUBLECLIQUE_PUT_REQUEST_REGION_H
#define DOUBLECLIQUE_PUT_REQUEST_REGION_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <sisci_types.h>
#include "sisci_glob_defs.h"

#define PIPE_SIZE 20000 //bytes
#define MAX_PUT_REQUEST_SLOTS (PIPE_SIZE/MIN_SIZE_ELEMENT)

typedef enum {
    PUT_REQUEST_REGION_INACTIVE,
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
    HEADER_SLOT_USED
};

typedef struct {
    enum header_slot_status status;
    uint8_t key_length;
    uint32_t value_length;
    uint32_t version_number;
    size_t offset;
} header_slot_t;

typedef struct {
    uint8_t sisci_node_id; // Only valid when status != 0
    header_slot_t header_slots[MAX_PUT_REQUEST_SLOTS];
    put_request_region_status_t status;
} put_request_region_t;

#define PUT_REQUEST_REGION_DATA_SIZE 0x200000
#define PUT_REQUEST_REGION_SIZE sizeof(put_request_region_t) + PUT_REQUEST_REGION_DATA_SIZE

#endif //DOUBLECLIQUE_PUT_REQUEST_REGION_H
