#ifndef DOUBLECLIQUE_PUT_REQUEST_REGION_PROTOCOL_H
#define DOUBLECLIQUE_PUT_REQUEST_REGION_PROTOCOL_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "sisci_glob_defs.h"

typedef enum {
    INACTIVE,
    ACTIVE
} put_request_region_status_t;

#define PIPE_SIZE 20000 //bytes

#define MAX_PUT_REQUEST_SLOTS (PIPE_SIZE/MIN_SIZE_ELEMENT)

#define ACK_DATA_INTERRUPT_NO 6

typedef struct {
    uint32_t replica_no;
    uint32_t bucket_no;
    uint32_t slot_no;
} put_ack_t;

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

typedef struct {
    size_t slot_size;
    size_t count;
    size_t offset;
} slot_bucket_t;

extern slot_bucket_t put_region_bucket_desc[BUCKET_COUNT];

//Slot utils
#define COMPUTE_SLOT_COUNT(slot_size) \
    (((slot_size) >= ((PIPE_SIZE) / 2)) ? \
     2 : \
     (((PIPE_SIZE) + (slot_size) - 1) / (slot_size)))

#define BUCKET_SIZE(slot_size) ((slot_size + sizeof(put_request_slot_preamble_t)) * COMPUTE_SLOT_COUNT(slot_size))

typedef struct {
    uint8_t sisci_node_id; // Only valid when status != 0
    size_t header_slots[MAX_PUT_REQUEST_SLOTS];
    put_request_region_status_t status;
} put_request_region_t;

size_t put_region_buckets_size(void);
uint32_t total_slots(void);
void init_bucket_desc(void);
size_t get_slot_no_from_offset(size_t offset, uint32_t bucket_no);
uint32_t get_bucket_no_from_offset(size_t offset);
size_t put_region_size(void);

#endif //DOUBLECLIQUE_PUT_REQUEST_REGION_PROTOCOL_H
