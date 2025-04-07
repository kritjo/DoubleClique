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
static char* gen_uuid(void);

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

    request_promise_t *promise;
    struct timespec start, end;

    unsigned char sample_data[8];

    for (unsigned char i = 0; i < 8; i++) {
        sample_data[i] = i;
    }

    printf("Loading table that has %lu index buckets\n", INDEX_BUCKETS);

#define PRELOAD_LOOP_COUNT 52428
    char *keys[PRELOAD_LOOP_COUNT];
    uint32_t index = 0;

    while(1) {
        keys[index] = gen_uuid();

        promise = put(keys[index], (uint8_t) 36, sample_data, 8);

        while (promise->put_result == PUT_PENDING);
        if (promise->put_result == PUT_RESULT_SUCCESS) {
            index++;
        } else {
            fprintf(stderr, "Put error: %d\n", promise->put_result);
        }

        free(promise);

        if (index >= 30000) break;
    }

    printf("Loaded table with %d keys\n", PRELOAD_LOOP_COUNT);

    char key[] = "tall";
    char key2[] = "tall2";

    clock_gettime(CLOCK_MONOTONIC, &start);

#ifndef PUT_LOOP
#define PUT_LOOP_COUNT 200000
    request_promise_t *promises_key[PUT_LOOP_COUNT];
    request_promise_t *promises_key2[PUT_LOOP_COUNT];

    for (uint32_t i = 0; i < PUT_LOOP_COUNT; i++) {
        promises_key[i] = put(key, 4, sample_data, sizeof(sample_data));
        promises_key2[i] = put(key2, 5, &i, sizeof(i));
    }

    promise = put(key, 4, sample_data, sizeof(sample_data));

    while (promise->put_result == PUT_PENDING);

    clock_gettime(CLOCK_MONOTONIC, &end);
    printf("Took on avg: %ld\n", ((end.tv_sec - start.tv_sec) * 1000000000L + (end.tv_nsec - start.tv_nsec)) / ((PUT_LOOP_COUNT * 2) + 1));
    printf("Put result: %u\n", promise->put_result);
    free(promise);

    for (uint32_t i = 0; i < PUT_LOOP_COUNT; i++) {
        while (promises_key[i]->put_result == PUT_PENDING);
        if (promises_key[i]->put_result != PUT_RESULT_SUCCESS) {
            printf("Put result: %u\n", promises_key[i]->put_result);
        }
        free(promises_key[i]);
        while (promises_key2[i]->put_result == PUT_PENDING);
        if (promises_key2[i]->put_result != PUT_RESULT_SUCCESS) {
            printf("Put result: %u\n", promises_key2[i]->put_result);
        }
        free(promises_key2[i]);
    }
#else
    promise = put(key, 4, sample_data, sizeof(sample_data));
    while (promise->put_result == PUT_PENDING);
    if (promise->put_result != PUT_RESULT_SUCCESS) {
        fprintf(stderr, "Put error1: %d!\n", promise->put_result);
        exit(EXIT_FAILURE);
    }

    int val = 7;
    promise = put(key2, 5, &val, sizeof(val));
    while (promise->put_result == PUT_PENDING);
    if (promise->put_result != PUT_RESULT_SUCCESS) {
        fprintf(stderr, "Put error2: %d!\n", promise->put_result);
        exit(EXIT_FAILURE);
    }
#endif

    promise = get_2_phase_1_sided(key, 4);

    if (promise->get_result == GET_RESULT_SUCCESS) {
        printf("At place 7 of get with data_2 length %u: %u\n", promise->data_len,
               ((unsigned char *) promise->data)[7]);
        free(promise->data);
    }
    else printf("get error: %u\n", promise->get_result);
    free(promise);

    promise = get_2_phase_1_sided(key2, 5);
    if (promise->get_result == GET_RESULT_SUCCESS) {
        printf("At place 0 of get with data length %u: %u\n", promise->data_len, *(uint32_t *) promise->data);
        free(promise->data);
    }

    free(promise);

#ifndef GET_LOOP_1
#define GET_LOOP_COUNT_1_SIDED 20000
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (uint32_t i = 0; i < GET_LOOP_COUNT_1_SIDED; i++) {
        promise = get_2_phase_1_sided(key2, 5);
        if (promise->get_result != GET_RESULT_SUCCESS) {
            printf("Get result: %u\n", promise->get_result);
        } else {
            free(promise->data);
        }
        free(promise);

        promise = get_2_phase_1_sided(key, 4);
        if (promise->get_result != GET_RESULT_SUCCESS) {
            printf("Get result: %u\n", promise->get_result);
        } else {
            free(promise->data);
        }
        free(promise);
    }
    clock_gettime(CLOCK_MONOTONIC, &end);
    printf("Took on avg: %ld\n", ((end.tv_sec - start.tv_sec) * 1000000000L + (end.tv_nsec - start.tv_nsec)) / (GET_LOOP_COUNT_1_SIDED * 2));
#endif

#define GET_LOOP_COUNT_2_SIDED 2000000
    request_promise_t *promises_get_key[GET_LOOP_COUNT_2_SIDED];
    request_promise_t *promises_get_key2[GET_LOOP_COUNT_2_SIDED];

    clock_gettime(CLOCK_MONOTONIC, &start);

    for (uint32_t i = 0; i < GET_LOOP_COUNT_2_SIDED; i++) {
        promises_get_key[i] = get_2_phase_2_sided(key2, 5);
        promises_get_key2[i] = get_2_phase_2_sided(key, 4);
        if (i%100000 == 0) {
            printf("Another day another GET\n");
        }
    }

    promise = get_2_phase_2_sided(key, 4);
    while (promise->get_result == GET_PENDING);
    clock_gettime(CLOCK_MONOTONIC, &end);
    printf("Get b3p on avg: %ld\n", ((end.tv_sec - start.tv_sec) * 1000000000L + (end.tv_nsec - start.tv_nsec)) / ((GET_LOOP_COUNT_2_SIDED * 2) + 1));
    if (promise->get_result == GET_RESULT_SUCCESS) {
        printf("completed! at place 7: %u\n", ((unsigned char *) promise->data)[7]);
        free(promise->data);
    }
    free(promise);

    uint32_t errors = 0;
    for (uint32_t i = 0; i < GET_LOOP_COUNT_2_SIDED; i++) {
        while (promises_get_key[i]->get_result == GET_PENDING);
        if (promises_get_key[i]->get_result != GET_RESULT_SUCCESS) {
            printf("error at %d: %d\n", i*2, promises_get_key[i]->get_result);
            errors++;
        }
        free(promises_get_key[i]);
        while (promises_get_key2[i]->get_result == GET_PENDING);
        if (promises_get_key2[i]->get_result != GET_RESULT_SUCCESS) {
            printf("error at %d: %d\n", i*2 + 1, promises_get_key2[i]->get_result);
            errors++;
        }
        free(promises_get_key2[i]);
    }

    printf("Completed! errors: %d\n", errors); //TODO: This is way too high

    while (1);

    SEOE(SCIClose, sd, NO_FLAGS);
    SCITerminate();

    return EXIT_SUCCESS;
}

// Caller must free returned promise
static request_promise_t *put(const char *key, uint8_t key_len, void *value, uint32_t value_len) {
    request_promise_t *promise = put_blocking_until_available_put_request_region_slot(key, key_len, value, value_len);
    return promise;
}

static char* gen_uuid(void) {
    char v[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    char *buf = malloc(37);

    //gen random for all spaces because lazy
    for(int i = 0; i < 36; ++i) {
        buf[i] = v[rand()%16];
    }

    //put dashes in place
    buf[8] = '-';
    buf[13] = '-';
    buf[18] = '-';
    buf[23] = '-';

    //needs end byte
    buf[36] = '\0';

    return buf;
}
