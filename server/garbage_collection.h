#ifndef DOUBLECLIQUE_GARBAGE_COLLECTION_H
#define DOUBLECLIQUE_GARBAGE_COLLECTION_H

#include "garbage_collection_queue.h"

typedef struct {
    queue_t *queue;
    struct buddy *buddy;
} garbage_collection_thread_args_t;

void *garbage_collection_thread(void *arg);

#endif //DOUBLECLIQUE_GARBAGE_COLLECTION_H
