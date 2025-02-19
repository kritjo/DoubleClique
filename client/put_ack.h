#ifndef DOUBLECLIQUE_PUT_ACK_H
#define DOUBLECLIQUE_PUT_ACK_H

#include <time.h>
#include "sisci_glob_defs.h"
#include "slots.h"

#define PUT_ACK_SEGMENT_ID 2
#define PUT_TIMEOUT_NS 100000000

enum put_promise_status {
    PUT_PENDING,
    PUT_RESULT_SUCCESS,
    PUT_RESULT_ERROR_TIMEOUT,
    PUT_RESULT_ERROR_OUT_OF_SPACE,
    PUT_RESULT_ERROR_MIX
};

typedef struct {
    enum put_promise_status result;
    uint32_t header_slot;
} put_promise_t;

typedef struct {
    struct timespec start_time;
    slot_metadata_t *metadata_slot;
    uint32_t header_slot_index;
    put_promise_t *promise;
} put_ack_slot_t;

void init_put_ack(sci_desc_t sd);
put_promise_t *acquire_header_slot_blocking(slot_metadata_t *metadata_slot);
void *put_ack_thread(__attribute__((unused)) void *_args);

#endif //DOUBLECLIQUE_PUT_ACK_H
