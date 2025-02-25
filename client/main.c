#include "main.h"

#include <stdlib.h>
#include <sisci_api.h>
#include <time.h>
#include <sched.h>

#include "sisci_glob_defs.h"
#include "put_request_region.h"

#include "super_fast_hash.h"
#include "2_phase_read_get.h"
#include "put.h"

static sci_desc_t sd;

static put_promise_t *put(const char *key, uint8_t key_len, void *value, uint32_t value_len);

int main(int argc, char* argv[]) {
    if (argc < REPLICA_COUNT + 1) {
        fprintf(stderr, "Usage: %s replica_id[0] ... replica_id[n]\n", argv[0]);
    }
    SEOE(SCIInitialize, NO_FLAGS);
    SEOE(SCIOpen, &sd, NO_FLAGS);

    // TODO: reset state if client reconnects
    init_put(sd);

    pthread_t put_ack_thread_id;
    pthread_create(&put_ack_thread_id, NULL, put_ack_thread, NULL);

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

    init_2_phase_read_get(sd, replica_node_ids);

    unsigned char sample_data[8];

    for (unsigned char i = 0; i < 8; i++) {
        sample_data[i] = i;
    }

    char key[] = "tall";
    char key2[] = "tall2";

    struct timespec start, end;

    put_promise_t *promise;

    clock_gettime(CLOCK_MONOTONIC, &start);
   for (uint32_t i = 0; i < 200; i++) {
        do {
            promise = put(key, 4, sample_data, sizeof(sample_data));
        } while (promise->result == PUT_NOT_POSTED);

        do {
            promise = put(key2, 5, &i, sizeof(i));
        } while (promise->result == PUT_NOT_POSTED);

    }

    promise = put(key, 4, sample_data, sizeof(sample_data));

    while (promise->result == PUT_NOT_POSTED || promise->result == PUT_PENDING);

    clock_gettime(CLOCK_MONOTONIC, &end);
    printf("Took on avg: %ld\n", ((end.tv_sec - start.tv_sec) * 1000000000L + (end.tv_nsec - start.tv_nsec))/201);
    printf("Put result: %u\n", promise->result);

    get_return_t *return_struct1 = get_2_phase_read(key2, 5);
    get_return_t *return_struct2 = get_2_phase_read(key, 4);

    if (return_struct2->status == GET_RETURN_SUCCESS) printf("At place 7 of get with data_2 length %u: %u\n", return_struct2->data_length, ((unsigned char *) return_struct2->data)[7]);
    if (return_struct1->status == GET_RETURN_SUCCESS) printf("At place 0 of get with data length %u: %u\n", return_struct1->data_length , *(uint32_t *) return_struct1->data);

    free(return_struct1->data);
    free(return_struct2->data);
    free(return_struct1);
    free(return_struct2);

    // TODO: How to free the slots in buddy and in general
    while(1);

    SEOE(SCIClose, sd, NO_FLAGS);
    SCITerminate();

    return EXIT_SUCCESS;
}

// Caller must free returned promise
static put_promise_t *put(const char *key, uint8_t key_len, void *value, uint32_t value_len) {
    put_promise_t *promise = put_blocking(key, key_len, value, value_len);
    return promise;
}
