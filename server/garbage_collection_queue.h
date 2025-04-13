#ifndef DOUBLECLIQUE_GARBAGE_COLLECTION_QUEUE_H
#define DOUBLECLIQUE_GARBAGE_COLLECTION_QUEUE_H

#include <stddef.h>
#include <pthread.h>

#define QUEUE_SPACE 2000000
#define NS_TO_COLLECTION 100000

typedef struct {
    struct timespec t;
    void *buddy_allocated_addr;
} queue_item_t;

typedef struct {
    queue_item_t *buffer;
    size_t head;
    size_t tail;
    pthread_mutex_t mutex;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
} queue_t;

void enqueue(queue_t *queue, queue_item_t item);
queue_item_t dequeue(queue_t *queue);
void queue_init(queue_t *queue);

#endif //DOUBLECLIQUE_GARBAGE_COLLECTION_QUEUE_H
