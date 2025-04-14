#include "main.h"

#include <stdlib.h>
#include <sisci_api.h>
#include <time.h>
#include <sched.h>
#include <math.h>
#include <string.h>

#include "sisci_glob_defs.h"

#include "super_fast_hash.h"
#include "2_phase_1_sided.h"
#include "put.h"
#include "request_region_connection.h"
#include "ack_region.h"
#include "2_phase_2_sided.h"

#define NUM_KEYS 13107
#define THETA 0.99
#define NUM_SAMPLES 100000

static sci_desc_t sd;

static char* gen_uuid(void);
static double* zipf_cdf(double *cdf);
static int zipf_sample(double* cdf);
static request_promise_t *do_random_action(double* cdf, unsigned char *data, uint32_t value_len, double chance_for_get, bool get_2_sided);

static char *keys[NUM_KEYS];

int main(int argc, char *argv[]) {
    if (argc < REPLICA_COUNT + 1) {
        fprintf(stderr, "Usage: %s replica_id[0] ... replica_id[n]\n", argv[0]);
    }
    SEOE(SCIInitialize, NO_FLAGS);
    SEOE(SCIOpen, &sd, NO_FLAGS);

    // TODO: reset state if client reconnects
    init_put();

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

    uint32_t index = 0;

    while(1) {
        keys[index] = gen_uuid();

        promise = put_blocking_until_available_put_request_region_slot(keys[index], 36, sample_data, 8);

        while (promise->result == PROMISE_PENDING);
        if (promise->result == PROMISE_SUCCESS) {
            index++;
        }

        free(promise);

        if (index >= NUM_KEYS) break;
    }

    double* cdf = malloc(NUM_KEYS * sizeof(double));
    zipf_cdf(cdf);

    printf("Loaded table with %d keys\n", NUM_KEYS);

    request_promise_t *promises[NUM_SAMPLES];
    uint32_t errors[REQUEST_PROMISE_STATUS_COUNT] = {0};

    // Do 90-10 get-put
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        promises[i] = do_random_action(cdf, sample_data, 8, 0.9, true);
    }
    while (promises[NUM_SAMPLES-1]->result == PROMISE_PENDING);
    clock_gettime(CLOCK_MONOTONIC, &end);
    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        while(promises[i]->result == PROMISE_PENDING);
        if (promises[i]->result == PROMISE_SUCCESS) {
            if (promises[i]->operation == OP_GET) {
                free(promises[i]->data);
            }
        } else {
            errors[promises[i]->result]++;
        }
        free(promises[i]);
    }
    printf("90-10 GET-PUT with %d samples took %ld ns\n", NUM_SAMPLES, ((end.tv_sec - start.tv_sec) * 1000000000L) + (end.tv_nsec - start.tv_nsec));
    for (uint32_t i = 0; i < REQUEST_PROMISE_STATUS_COUNT; i++) {
        printf("    Status %d: %d\n", i, errors[i]);
    }

    // Do 90-10 get-put
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        promises[i] = do_random_action(cdf, sample_data, 8, 0.5, true);
    }
    while (promises[NUM_SAMPLES-1]->result == PROMISE_PENDING);
    clock_gettime(CLOCK_MONOTONIC, &end);
    memset(errors, 0, REQUEST_PROMISE_STATUS_COUNT * sizeof(uint32_t));
    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        while(promises[i]->result == PROMISE_PENDING);
        if (promises[i]->result == PROMISE_SUCCESS) {
            if (promises[i]->operation == OP_GET) {
                free(promises[i]->data);
            }
        } else {
            errors[promises[i]->result]++;
        }
        free(promises[i]);
    }
    printf("50-50 GET-PUT with %d samples took %ld ns\n", NUM_SAMPLES, ((end.tv_sec - start.tv_sec) * 1000000000L) + (end.tv_nsec - start.tv_nsec));
    for (uint32_t i = 0; i < REQUEST_PROMISE_STATUS_COUNT; i++) {
        printf("    Status %d: %d\n", i, errors[i]);
    }

    // Do 90-10 get-put
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        promises[i] = do_random_action(cdf, sample_data, 8, 0.1, true);
    }
    while (promises[NUM_SAMPLES-1]->result == PROMISE_PENDING);
    clock_gettime(CLOCK_MONOTONIC, &end);
    memset(errors, 0, REQUEST_PROMISE_STATUS_COUNT * sizeof(uint32_t));
    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        while(promises[i]->result == PROMISE_PENDING);
        if (promises[i]->result == PROMISE_SUCCESS) {
            if (promises[i]->operation == OP_GET) free(promises[i]->data);
        } else {
            errors[promises[i]->result]++;
        }
        free(promises[i]);
    }
    printf("10-90 GET-PUT with %d samples took %ld ns\n", NUM_SAMPLES, ((end.tv_sec - start.tv_sec) * 1000000000L) + (end.tv_nsec - start.tv_nsec));
    for (uint32_t i = 0; i < REQUEST_PROMISE_STATUS_COUNT; i++) {
        printf("    Status %d: %d\n", i, errors[i]);
    }

    // put
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        promises[i] = do_random_action(cdf, sample_data, 8, 0, true);
    }
    while (promises[NUM_SAMPLES-1]->result == PROMISE_PENDING);
    clock_gettime(CLOCK_MONOTONIC, &end);
    memset(errors, 0, REQUEST_PROMISE_STATUS_COUNT * sizeof(uint32_t));
    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        while(promises[i]->result == PROMISE_PENDING);
        if (promises[i]->result == PROMISE_SUCCESS) {
            if (promises[i]->operation == OP_GET) {
                free(promises[i]->data);
            }
        } else {
            errors[promises[i]->result]++;
        }
        free(promises[i]);
    }
    printf("PUT took %ld ns pr\n", (((end.tv_sec - start.tv_sec) * 1000000000L) + (end.tv_nsec - start.tv_nsec))/NUM_SAMPLES);
    for (uint32_t i = 0; i < REQUEST_PROMISE_STATUS_COUNT; i++) {
        printf("    Status %d: %d\n", i, errors[i]);
    }

    // 2 sided get
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        promises[i] = do_random_action(cdf, sample_data, 8, 1, true);
    }
    while (promises[NUM_SAMPLES-1]->result == PROMISE_PENDING);
    clock_gettime(CLOCK_MONOTONIC, &end);
    memset(errors, 0, REQUEST_PROMISE_STATUS_COUNT * sizeof(uint32_t));
    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        while(promises[i]->result == PROMISE_PENDING);
        if (promises[i]->result == PROMISE_SUCCESS) {
            if (promises[i]->operation == OP_GET) {
                free(promises[i]->data);
            }
        } else {
            errors[promises[i]->result]++;
        }
        free(promises[i]);
    }
    printf("2 sided GET took %ld ns pr\n", (((end.tv_sec - start.tv_sec) * 1000000000L) + (end.tv_nsec - start.tv_nsec))/NUM_SAMPLES);
    for (uint32_t i = 0; i < REQUEST_PROMISE_STATUS_COUNT; i++) {
        printf("    Status %d: %d\n", i, errors[i]);
    }

    // 1 sided get
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        promises[i] = do_random_action(cdf, sample_data, 8, 1, false);
    }
    while (promises[NUM_SAMPLES-1]->result == PROMISE_PENDING);
    clock_gettime(CLOCK_MONOTONIC, &end);
    memset(errors, 0, REQUEST_PROMISE_STATUS_COUNT * sizeof(uint32_t));
    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        while(promises[i]->result == PROMISE_PENDING);
        if (promises[i]->result == PROMISE_SUCCESS) {
            if (promises[i]->operation == OP_GET) {
                free(promises[i]->data);
            }
        } else {
            errors[promises[i]->result]++;
        }
        free(promises[i]);
    }
    printf("1 sided GET took %ld ns pr\n", (((end.tv_sec - start.tv_sec) * 1000000000L) + (end.tv_nsec - start.tv_nsec))/NUM_SAMPLES);
    for (uint32_t i = 0; i < REQUEST_PROMISE_STATUS_COUNT; i++) {
        printf("    Status %d: %d\n", i, errors[i]);
    }

    free(cdf);
    for (uint32_t i = 0; i < NUM_KEYS; i++) {
        free(keys[i]);
    }

    SEOE(SCIClose, sd, NO_FLAGS);
    SCITerminate();

    return EXIT_SUCCESS;
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

static double* zipf_cdf(double *cdf) {
    double sum = 0.0;

    // Harmonic sum
    for (int i = 1; i <= NUM_KEYS; i++) {
        sum += 1.0 / pow((double)i, THETA);
    }

    // Cumulative distribution
    double cumulative = 0.0;
    for (int i = 0; i < NUM_KEYS; i++) {
        cumulative += 1.0 / pow((double)(i + 1), THETA);
        cdf[i] = cumulative / sum;
    }

    return cdf;
}

static int zipf_sample(double* cdf) {
    double u = (double) rand() / RAND_MAX;
    int low = 0, high = NUM_KEYS - 1;
    while (low < high) {
        int mid = (low + high) / 2;
        if (u < cdf[mid]) {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    return low;
}

static request_promise_t *do_random_action(double* cdf, unsigned char *data, uint32_t value_len, double chance_for_get, bool get_2_sided) {
    int key_index = zipf_sample(cdf);

    double rand_0_to_1 = (double)rand() / (double)RAND_MAX;
    if (rand_0_to_1 > chance_for_get)
        return put_blocking_until_available_put_request_region_slot(keys[key_index], 36, data, value_len);
    else if (get_2_sided) return get_2_phase_2_sided(keys[key_index], 36);
    else return get_2_phase_1_sided(keys[key_index], 36);
}
