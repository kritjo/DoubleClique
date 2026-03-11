#include <immintrin.h>
#include <malloc.h>
#include <stdatomic.h>
#include <time.h>
#include "phase_2_queue.h"

static queue_item_t *buffer;
static _Atomic size_t head;
static _Atomic size_t tail;

void enqueue(queue_item_t item) {
    // Single producer: publish the element only after the payload write is visible.
    size_t local_head = atomic_load_explicit(&head, memory_order_relaxed);
    size_t next_head = (local_head + 1) % QUEUE_SPACE;
    while (next_head == atomic_load_explicit(&tail, memory_order_acquire)) {
        _mm_pause();
    }

    buffer[local_head] = item;
    atomic_store_explicit(&head, next_head, memory_order_release);
}

queue_item_t dequeue(void) {
    // Single consumer: read the payload only after observing published head.
    size_t local_tail = atomic_load_explicit(&tail, memory_order_relaxed);
    while (atomic_load_explicit(&head, memory_order_acquire) == local_tail) {
        _mm_pause();
    }

    queue_item_t item = buffer[local_tail];
    atomic_store_explicit(&tail, (local_tail + 1) % QUEUE_SPACE, memory_order_release);
    return item;
}

void queue_init(void) {
    buffer = malloc(QUEUE_SPACE * sizeof(queue_item_t));
    atomic_init(&head, 0);
    atomic_init(&tail, 0);
}
