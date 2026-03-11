#ifndef DOUBLECLIQUE_GARBAGE_COLLECTION_QUEUE_H
#define DOUBLECLIQUE_GARBAGE_COLLECTION_QUEUE_H

#include <stddef.h>
#include <stdatomic.h>
#include <stdint.h>
#include <time.h>

#define QUEUE_SPACE 100000
#define NS_TO_COLLECTION 1000

typedef struct {
    uint64_t ready_at_ns;
    void *buddy_allocated_addr;
} queue_item_t;

typedef struct {
    queue_item_t *buffer;
    _Atomic size_t head;
    _Atomic size_t tail;
} queue_t;

void enqueue(queue_t *queue, queue_item_t item);
queue_item_t dequeue(queue_t *queue);
void queue_init(queue_t *queue);

#endif //DOUBLECLIQUE_GARBAGE_COLLECTION_QUEUE_H
