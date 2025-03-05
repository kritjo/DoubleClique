#ifndef DOUBLECLIQUE_ACK_REGION_H
#define DOUBLECLIQUE_ACK_REGION_H

#include <stdint.h>
#include "request_region.h"

extern pthread_mutex_t ack_mutex;
extern uint32_t oldest_ack_offset;

enum put_request_promise_status {
    PUT_PENDING,
    PUT_RESULT_SUCCESS,
    PUT_RESULT_ERROR_TIMEOUT,
    PUT_RESULT_ERROR_OUT_OF_SPACE,
    PUT_RESULT_ERROR_MIX,
};

enum get_request_promise_status {
    GET_PENDING,
    GET_RESULT_SUCCESS,
    GET_RESULT_ERROR_NO_MATCH,
    GET_RESULT_ERROR_TIMEOUT
};

typedef struct {
    _Atomic enum put_request_promise_status put_result;
    _Atomic enum get_request_promise_status get_result;
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
    uint32_t starting_ack_data_offset;
    uint32_t key_hash; // Only valid for GET_PHASE1 request types
    char *key; // Only valid for GET requests -- ALLOCATED UNTIL PROMISE IS RESOLVED
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
