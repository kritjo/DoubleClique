#include <pthread.h>
#include <malloc.h>
#include "phase_2_queue.h"

static queue_item_t *buffer;
static size_t head;
static size_t tail;
static pthread_mutex_t mutex;
static pthread_cond_t not_full;
static pthread_cond_t not_empty;

void enqueue(queue_item_t item) {
    pthread_mutex_lock(&mutex);
    // Wait while queue is full
    while (((head + 1) % QUEUE_CAPACITY) == tail) {
        pthread_cond_wait(&not_full, &mutex);
    }
    // Add item
    buffer[head] = item;
    head = (head + 1) % QUEUE_CAPACITY;
    // Signal any waiting consumer
    pthread_cond_signal(&not_empty);
    pthread_mutex_unlock(&mutex);
}

queue_item_t dequeue(void) {
    pthread_mutex_lock(&mutex);
    // Wait while queue is empty
    while (head == tail) {
        pthread_cond_wait(&not_empty, &mutex);
    }
    // Remove item
    queue_item_t item = buffer[tail];
    tail = (tail + 1) % QUEUE_CAPACITY;
    // Signal any waiting producer
    pthread_cond_signal(&not_full);
    pthread_mutex_unlock(&mutex);
    return item;
}

void queue_init(void) {
    buffer = malloc(QUEUE_CAPACITY * sizeof(queue_item_t));
    head = tail = 0;
    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&not_full, NULL);
    pthread_cond_init(&not_empty, NULL);
}
