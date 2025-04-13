#ifndef DOUBLECLIQUE_ACK_REGION_H
#define DOUBLECLIQUE_ACK_REGION_H

#include <stdint.h>
#include "request_region.h"

enum request_promise_status {
    PROMISE_PENDING,
    PROMISE_SUCCESS,
    PROMISE_TIMEOUT,
    PROMISE_ERROR_MIX,
    PROMISE_ERROR_OUT_OF_SPACE,
    PROMISE_ERROR_NO_MATCH,
    PROMISE_ERROR_TRANSFER,
    REQUEST_PROMISE_STATUS_COUNT
};

enum request_operation {
    OP_GET,
    OP_PUT
};

typedef struct {
    _Atomic enum request_promise_status result;
    _Atomic enum request_operation operation;
    _Atomic uint32_t data_len;
    _Atomic(void *) data;
} request_promise_t;


enum request_type {
    PUT,
    GET_PHASE1,
    GET_PHASE2
};

typedef struct {
    struct timespec start_time;
    volatile header_slot_t *header_slot_WRITE_ONLY;
    enum request_type request_type;
    request_promise_t *promise;
    uint8_t key_len;
    uint32_t value_len;
    uint32_t version_number;
    uint32_t starting_data_offset;
    uint32_t data_size;
    uint32_t starting_ack_data_offset;
    uint32_t ack_data_size;
    uint32_t key_hash; // Only valid for GET_PHASE1 request types
    replica_ack_t *replica_ack_instances[REPLICA_COUNT];
} ack_slot_t;

extern replica_ack_t *replica_ack;
extern ack_slot_t ack_slots[MAX_REQUEST_SLOTS];

extern sci_local_segment_t ack_segment;
extern sci_map_t ack_map;

void init_ack_region(sci_desc_t sd);
ack_slot_t *get_ack_slot_blocking(enum request_type request_type, uint8_t key_len, uint32_t value_len, uint32_t header_data_length, uint32_t ack_data_length, uint32_t version_number, request_promise_t *promise);

void *ack_thread(__attribute__((unused)) void *_args);

#endif //DOUBLECLIQUE_ACK_REGION_H
