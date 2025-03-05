#ifndef DOUBLECLIQUE_PUT_H
#define DOUBLECLIQUE_PUT_H

#include <time.h>
#include "sisci_glob_defs.h"
#include "request_region.h"

#define PUT_TIMEOUT_NS 1000000000 //TODO: This should probably be a factor of queue length

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
} put_promise_t;

typedef struct {
    struct timespec start_time;
    volatile header_slot_t *header_slot_WRITE_ONLY;
    put_promise_t *promise;
    uint8_t key_len;
    uint32_t value_len;
    uint32_t version_number;
} ack_slot_t;

void init_put(sci_desc_t sd);
put_promise_t *put_blocking_until_available_put_request_region_slot(const char *key, uint8_t key_len, void *value, uint32_t value_len);
void *ack_thread(__attribute__((unused)) void *_args);

#endif //DOUBLECLIQUE_PUT_H
