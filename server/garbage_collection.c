#include <malloc.h>
#include <time.h>
#include "garbage_collection.h"
#include "garbage_collection_queue.h"
#include "buddy_alloc.h"

void *garbage_collection_thread(void *arg) {
    garbage_collection_thread_args_t *args = (garbage_collection_thread_args_t *) arg;
    while (1) {
        queue_item_t queue_item = dequeue(args->queue);
        clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &queue_item.t, NULL);
        buddy_free(args->buddy, queue_item.buddy_allocated_addr);
    }
    free(args);
}
