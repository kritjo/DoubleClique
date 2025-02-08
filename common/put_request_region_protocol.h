#ifndef DOUBLECLIQUE_PUT_REQUEST_REGION_PROTOCOL_H
#define DOUBLECLIQUE_PUT_REQUEST_REGION_PROTOCOL_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "sisci_glob_defs.h"

typedef enum {
    UNUSED,
    LOCKED,
    WALKABLE,
    EXITED
} put_request_region_status_t;

#define PUT_REQUEST_REGION_SIZE 100000
#define MAX_PUT_REQUEST_SLOTS (PUT_REQUEST_REGION_SIZE/MIN_SIZE_ELEMENT)

#define ACK_DATA_INTERRUPT_NO 6

typedef struct {
    uint32_t replica_no;
    uint32_t slot_no;
} put_ack_t;

typedef struct {
    uint8_t sisci_node_id; // Only valid when status != 0
    uint32_t slots_used;
    ptrdiff_t slot_offset_starts[MAX_PUT_REQUEST_SLOTS];
    uint8_t units[PUT_REQUEST_REGION_SIZE];
    put_request_region_status_t status;
} put_request_region_t;

typedef enum {
    FREE,
    PUT,
} put_request_slot_status_t;

/*
 * The structure of the request slot is as follows:
 * put_request_slot_preamble_t
 *
 */
typedef struct {
    uint8_t key_length; // NOT including null byte - just like strlen
    uint32_t value_length;
    uint32_t version_number;
    put_request_slot_status_t status;
} put_request_slot_preamble_t;

#endif //DOUBLECLIQUE_PUT_REQUEST_REGION_PROTOCOL_H
