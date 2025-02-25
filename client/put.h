#ifndef DOUBLECLIQUE_PUT_H
#define DOUBLECLIQUE_PUT_H

#include <time.h>
#include "sisci_glob_defs.h"
#include "put_request_region.h"

#define PUT_ACK_SEGMENT_ID 2
#define PUT_TIMEOUT_NS 100000000

enum put_promise_status {
    PUT_NOT_POSTED, // Retry
    PUT_PENDING,
    PUT_RESULT_SUCCESS,
    PUT_RESULT_ERROR_TIMEOUT,
    PUT_RESULT_ERROR_OUT_OF_SPACE,
    PUT_RESULT_ERROR_MIX
};

typedef struct {
    _Atomic enum put_promise_status result;
    uint32_t header_slot;
} put_promise_t;

typedef struct {
    struct timespec start_time;
    volatile header_slot_t *header_slot;
    put_promise_t *promise;
} put_ack_slot_t;

void init_put(sci_desc_t sd);
put_promise_t *put_blocking(const char *key, uint8_t key_len, void *value, uint32_t value_len);
void *put_ack_thread(__attribute__((unused)) void *_args);

#endif //DOUBLECLIQUE_PUT_H
