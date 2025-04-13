#include <pthread.h>
#include <malloc.h>
#include "garbage_collection_queue.h"


void enqueue(queue_t *queue, queue_item_t item) {
    pthread_mutex_lock(&queue->mutex);
    // Wait while queue is full
    while (((queue->head + 1) % QUEUE_SPACE) == queue->tail) {
        pthread_cond_wait(&queue->not_full, &queue->mutex);
    }
    // Add item
    queue->buffer[queue->head] = item;
    queue->head = (queue->head + 1) % QUEUE_SPACE;
    // Signal any waiting consumer
    pthread_cond_signal(&queue->not_empty);
    pthread_mutex_unlock(&queue->mutex);
}

queue_item_t dequeue(queue_t *queue) {
    pthread_mutex_lock(&queue->mutex);
    // Wait while queue is empty
    while (queue->head == queue->tail) {
        pthread_cond_wait(&queue->not_empty, &queue->mutex);
    }
    // Remove item
    queue_item_t item = queue->buffer[queue->tail];
    queue->tail = (queue->tail + 1) % QUEUE_SPACE;
    // Signal any waiting producer
    pthread_cond_signal(&queue->not_full);
    pthread_mutex_unlock(&queue->mutex);
    return item;
}

void queue_init(queue_t *queue) {
    queue->buffer = malloc(QUEUE_SPACE * sizeof(queue_item_t));
    queue->head = queue->tail = 0;
    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->not_full, NULL);
    pthread_cond_init(&queue->not_empty, NULL);
}
