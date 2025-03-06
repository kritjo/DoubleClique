#ifndef DOUBLECLIQUE_PHASE_2_QUEUE_H
#define DOUBLECLIQUE_PHASE_2_QUEUE_H

#include "request_region.h"
#include "ack_region.h"

#define QUEUE_CAPACITY (MAX_REQUEST_SLOTS * INDEX_SLOTS_PR_BUCKET)

typedef struct {
    uint32_t version_number;
    uint32_t replica_index;
    uint8_t key_len;
    uint32_t value_len;
    ptrdiff_t server_data_offset;
    request_promise_t *promise;
} queue_item_t;

void enqueue(queue_item_t item);
queue_item_t dequeue(void);
void queue_init(void);

#endif //DOUBLECLIQUE_PHASE_2_QUEUE_H
