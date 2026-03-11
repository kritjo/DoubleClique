#include <immintrin.h>
#include <malloc.h>
#include "garbage_collection_queue.h"


void enqueue(queue_t *queue, queue_item_t item) {
    // Single producer: publish head only after the item payload is fully written.
    size_t local_head = atomic_load_explicit(&queue->head, memory_order_relaxed);
    size_t next_head = (local_head + 1) % QUEUE_SPACE;
    while (next_head == atomic_load_explicit(&queue->tail, memory_order_acquire)) {
        _mm_pause();
    }

    queue->buffer[local_head] = item;
    atomic_store_explicit(&queue->head, next_head, memory_order_release);
}

queue_item_t dequeue(queue_t *queue) {
    // Single consumer: observe published head before reading the item payload.
    size_t local_tail = atomic_load_explicit(&queue->tail, memory_order_relaxed);
    while (atomic_load_explicit(&queue->head, memory_order_acquire) == local_tail) {
        _mm_pause();
    }

    queue_item_t item = queue->buffer[local_tail];
    atomic_store_explicit(&queue->tail, (local_tail + 1) % QUEUE_SPACE, memory_order_release);
    return item;
}

void queue_init(queue_t *queue) {
    queue->buffer = malloc(QUEUE_SPACE * sizeof(queue_item_t));
    atomic_init(&queue->head, 0);
    atomic_init(&queue->tail, 0);
}
