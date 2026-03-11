#include <malloc.h>
#include <time.h>
#include "garbage_collection.h"
#include "garbage_collection_queue.h"
#include "buddy_alloc.h"

void *garbage_collection_thread(void *arg) {
    garbage_collection_thread_args_t *args = (garbage_collection_thread_args_t *) arg;
    while (1) {
        queue_item_t queue_item = dequeue(args->queue);
        struct timespec ready_at = {
            .tv_sec = (time_t) (queue_item.ready_at_ns / 1000000000ULL),
            .tv_nsec = (long) (queue_item.ready_at_ns % 1000000000ULL),
        };
        clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &ready_at, NULL);
        buddy_free(args->buddy, queue_item.buddy_allocated_addr);
    }
    free(args);
}
