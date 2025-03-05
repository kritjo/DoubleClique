#ifndef DOUBLECLIQUE_PUT_H
#define DOUBLECLIQUE_PUT_H

#include <time.h>
#include "sisci_glob_defs.h"
#include "request_region.h"

#define PUT_TIMEOUT_NS 1000000000 //TODO: This should probably be a factor of queue length

enum put_request_promise_status {
    PUT_PENDING,
    PUT_RESULT_SUCCESS,
    PUT_RESULT_ERROR_TIMEOUT,
    PUT_RESULT_ERROR_OUT_OF_SPACE,
    PUT_RESULT_ERROR_MIX,
};

enum get_request_promise_status {
    GET_PENDING,
};

typedef union {
    _Atomic enum put_request_promise_status put_result;
    _Atomic enum get_request_promise_status get_result;
} request_promise_t;

enum request_type {
    PUT,
    GET
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
    replica_ack_t *replica_ack_instances[REPLICA_COUNT];
} ack_slot_t;

void init_put(sci_desc_t sd);
request_promise_t *put_blocking_until_available_put_request_region_slot(const char *key, uint8_t key_len, void *value, uint32_t value_len);
bool consume_put_ack_slot(ack_slot_t *ack_slot);

#endif //DOUBLECLIQUE_PUT_H
