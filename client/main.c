#include "main.h"

#include <stdlib.h>
#include <sisci_api.h>
#include <time.h>
#include <sched.h>

#include "sisci_glob_defs.h"

#include "super_fast_hash.h"
#include "2_phase_1_sided.h"
#include "put.h"
#include "request_region_connection.h"
#include "ack_region.h"
#include "2_phase_2_sided.h"

static sci_desc_t sd;

static request_promise_t *put(const char *key, uint8_t key_len, void *value, uint32_t value_len);

int main(int argc, char *argv[]) {
    if (argc < REPLICA_COUNT + 1) {
        fprintf(stderr, "Usage: %s replica_id[0] ... replica_id[n]\n", argv[0]);
    }
    SEOE(SCIInitialize, NO_FLAGS);
    SEOE(SCIOpen, &sd, NO_FLAGS);

    // TODO: reset state if client reconnects
    init_put(sd);

    pthread_t ack_thread_id;
    pthread_create(&ack_thread_id, NULL, ack_thread, NULL);

    uint8_t replica_node_ids[REPLICA_COUNT];

    for (uint8_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        char *endptr;
        long replica_node_id;
        replica_node_id = strtol(argv[1 + replica_index], &endptr, 10);
        if (replica_node_id > UINT8_MAX) {
            fprintf(stderr, "String not convertable to uint8!\n");
            exit(EXIT_FAILURE);
        }

        replica_node_ids[replica_index] = (uint8_t) replica_node_id;
    }

    init_ack_region(sd);
    connect_to_request_region(sd);
    init_2_phase_1_sided_get(sd, replica_node_ids, false);
    init_2_phase_2_sided_get();

    unsigned char sample_data[8];

    for (unsigned char i = 0; i < 8; i++) {
        sample_data[i] = i;
    }

    char key[] = "tall";
    char key2[] = "tall2";

    struct timespec start, end;

    request_promise_t *promise;
    clock_gettime(CLOCK_MONOTONIC, &start);

    for (uint32_t i = 0; i < 200000; i++) {
        put(key, 4, sample_data, sizeof(sample_data));
        put(key2, 5, &i, sizeof(i));
    }

    promise = put(key, 4, sample_data, sizeof(sample_data));

    while (promise->put_result == PUT_PENDING);

    clock_gettime(CLOCK_MONOTONIC, &end);
    printf("Took on avg: %ld\n", ((end.tv_sec - start.tv_sec) * 1000000000L + (end.tv_nsec - start.tv_nsec)) / 400001);
    printf("Put result: %u\n", promise->put_result);


    request_promise_t *promise2 = get_2_phase_1_sided(key, 4);

    if (promise2->get_result == GET_RESULT_SUCCESS) printf("At place 7 of get with data_2 length %u: %u\n", promise2->data_len, ((unsigned char *) promise2->data)[7]);
    else printf("get error: %u\n", promise2->get_result);


    request_promise_t *promise1 = get_2_phase_1_sided(key2, 5);
    if (promise1->get_result == GET_RESULT_SUCCESS) printf("At place 0 of get with data length %u: %u\n", promise1->data_len , *(uint32_t *) promise1->data);

    free(promise1->data);
    free(promise2->data);
    free(promise1);
    free(promise2);

    clock_gettime(CLOCK_MONOTONIC, &start);
    for (uint32_t i = 0; i < 20; i++) {
        promise1 = get_2_phase_1_sided(key2, 5);
        promise2 = get_2_phase_1_sided(key, 4);

        free(promise1->data);
        free(promise2->data);
        free(promise1);
        free(promise2);
    }
    clock_gettime(CLOCK_MONOTONIC, &end);
    printf("Took on avg: %ld\n", ((end.tv_sec - start.tv_sec) * 1000000000L + (end.tv_nsec - start.tv_nsec)) / 20);


    clock_gettime(CLOCK_MONOTONIC, &start);
    for (uint32_t i = 0; i < 200; i++) {
        promise = get_2_phase_2_sided(key2, 5); // Note that data should really be freed but is not
        promise = get_2_phase_2_sided(key, 4);
    }
    while (promise->get_result == GET_PENDING);
    clock_gettime(CLOCK_MONOTONIC, &end);
    printf("Get b3p on avg: %ld\n", ((end.tv_sec - start.tv_sec) * 1000000000L + (end.tv_nsec - start.tv_nsec)) / 400);

    promise = get_2_phase_2_sided(key, 4);
    printf("main promise %p\n", (void *) promise);
    while (promise->get_result == GET_PENDING);
    if (promise->get_result == GET_RESULT_SUCCESS) {
        printf("success data ptr: %p\n", promise->data);
        printf("completed! at place 7: %u\n", ((unsigned char *) promise->data)[7]);
    }
    else printf("get error: %u at %p\n", promise->get_result, (void *) &promise->get_result);
    free(promise->data);
    free(promise);

    while (1);

    // TODO: How to free the slots in buddy and in general

    SEOE(SCIClose, sd, NO_FLAGS);
    SCITerminate();

    return EXIT_SUCCESS;
}

// Caller must free returned promise
static request_promise_t *put(const char *key, uint8_t key_len, void *value, uint32_t value_len) {
    request_promise_t *promise = put_blocking_until_available_put_request_region_slot(key, key_len, value, value_len);
    return promise;
}
