#include "main.h"

#include <immintrin.h>
#include <stdlib.h>
#include <sisci_api.h>
#include <time.h>
#include <sched.h>
#include <math.h>
#include <string.h>
#include <inttypes.h>

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
#define VALUE_LEN 8
#define DO_NON_BATCH false

static sci_desc_t sd;

static char* gen_uuid(void);
static double* zipf_cdf(double *cdf);
static int zipf_sample(const double* cdf);
static request_promise_t *do_random_zipf_action(const char *key, unsigned char *data, uint32_t value_len, double chance_for_get, bool get_2_sided);
static request_promise_t *do_random_action(unsigned char *data, uint32_t value_len, double chance_for_get, bool get_2_sided);

static char *keys[NUM_KEYS];
static int key_for_sample[NUM_SAMPLES] = {0};
static unsigned char sample_data[VALUE_LEN];

#define NS_PER_SEC 1000000000ULL

static int cmp_u64(const void *a, const void *b) {
    uint64_t aa = *(const uint64_t *)a;
    uint64_t bb = *(const uint64_t *)b;
    return (aa > bb) - (aa < bb);
}

static void finish_experiment(
    request_promise_t *promises[NUM_SAMPLES],
    const char *experiment_name,
    const char *distribution_name,
    struct timespec start
) {
    uint32_t errors[REQUEST_PROMISE_STATUS_COUNT] = {0};
    uint64_t latencies[NUM_SAMPLES];
    uint64_t total_latency_ns = 0;

    while (promises[NUM_SAMPLES-1]->result == PROMISE_PENDING) _mm_pause();
    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        if (promises[i]->result == PROMISE_PENDING) {
            printf("WARNING: out of order promise pending. Should never happen.");
        }
    }

    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);

    uint64_t wall_ns =
        (uint64_t)(end.tv_sec - start.tv_sec) * NS_PER_SEC +
        (uint64_t)(end.tv_nsec - start.tv_nsec);

    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        request_promise_t *p = promises[i];

        errors[p->result]++;

        latencies[i] =
            (uint64_t)p->duration.tv_sec * NS_PER_SEC +
            (uint64_t)p->duration.tv_nsec;

        total_latency_ns += latencies[i];

        if (p->result == PROMISE_SUCCESS && p->operation == OP_GET) {
            free(p->data);
        }
        free(p);
    }

    qsort(latencies, NUM_SAMPLES, sizeof(latencies[0]), cmp_u64);

    uint64_t avg_latency_ns = total_latency_ns / NUM_SAMPLES;
    uint64_t p50_ns = latencies[((NUM_SAMPLES - 1) * 50) / 100];
    uint64_t p95_ns = latencies[((NUM_SAMPLES - 1) * 95) / 100];
    uint64_t p99_ns = latencies[((NUM_SAMPLES - 1) * 99) / 100];

    double throughput_ops_per_sec =
        wall_ns ? ((double)NUM_SAMPLES * 1e9) / (double)wall_ns : 0.0;

    double avg_in_flight =
        wall_ns ? (double)total_latency_ns / (double)wall_ns : 0.0;

    printf("%s with %u %s samples\n", experiment_name, NUM_SAMPLES, distribution_name);
    printf("    wall time    : %" PRIu64 " ns\n", wall_ns);
    printf("    avg latency  : %" PRIu64 " ns\n", avg_latency_ns);
    printf("    p50 latency  : %" PRIu64 " ns\n", p50_ns);
    printf("    p95 latency  : %" PRIu64 " ns\n", p95_ns);
    printf("    p99 latency  : %" PRIu64 " ns\n", p99_ns);
    printf("    throughput   : %.2f ops/sec\n", throughput_ops_per_sec);
    printf("    avg in-flight: %.2f\n", avg_in_flight);

    for (uint32_t i = 0; i < REQUEST_PROMISE_STATUS_COUNT; i++) {
        printf("    Status %u: %u\n", i, errors[i]);
    }
}

static void do_experiment_uniform(
    request_promise_t *(*promise_func)(unsigned char *data, uint32_t value_len, double chance_for_get, bool get_2_sided),
    double chance_for_get,
    bool get_2_sided,
    const char *experiment_name
) {
    request_promise_t *promises[NUM_SAMPLES];
    struct timespec start;

    sleep(1);

    clock_gettime(CLOCK_MONOTONIC, &start);
    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        promises[i] = promise_func(sample_data, VALUE_LEN, chance_for_get, get_2_sided);
    }

    finish_experiment(promises, experiment_name, "uniform", start);
}

static void do_experiment_zipf(
    request_promise_t *(*promise_func)(const char *key, unsigned char *data, uint32_t value_len, double chance_for_get, bool get_2_sided),
    double chance_for_get,
    bool get_2_sided,
    const char *experiment_name
) {
    request_promise_t *promises[NUM_SAMPLES];
    struct timespec start;

    sleep(1);

    clock_gettime(CLOCK_MONOTONIC, &start);
    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        promises[i] = promise_func(keys[key_for_sample[i]], sample_data, VALUE_LEN, chance_for_get, get_2_sided);
    }

    finish_experiment(promises, experiment_name, "zipf", start);
}

int main(int argc, char *argv[]) {
    if (argc != REPLICA_COUNT + 1) {
        fprintf(stderr, "Usage: %s replica_id[0] ... replica_id[n]\n", argv[0]);
        exit(EXIT_FAILURE);
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


    for (unsigned char i = 0; i < VALUE_LEN; i++) {
        sample_data[i] = i;
    }

    printf("Loading table that has %lu index buckets\n", INDEX_BUCKETS);

    uint32_t index = 0;

    while(1) {
        keys[index] = gen_uuid();

        promise = put_blocking_until_available_put_request_region_slot(keys[index], 36, sample_data, VALUE_LEN);

        while (promise->result == PROMISE_PENDING) _mm_pause();
        if (promise->result == PROMISE_SUCCESS) {
            index++;
        }

        free(promise);

        if (index >= NUM_KEYS) break;
    }

    double* cdf = malloc(NUM_KEYS * sizeof(double));
    if (cdf == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    zipf_cdf(cdf);

    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        key_for_sample[i] = zipf_sample(cdf);
    }

    printf("Loaded table with %d keys\n", NUM_KEYS);

    request_promise_t *promises[NUM_SAMPLES];

    printf("warming up\n");
    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        promises[i] = do_random_zipf_action(keys[key_for_sample[i]], sample_data, VALUE_LEN, 0.9, true);
    }
    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        while(promises[i]->result == PROMISE_PENDING) _mm_pause();
        if (promises[i]->result == PROMISE_SUCCESS) {
            if (promises[i]->operation == OP_GET) {
                free(promises[i]->data);
            }
        }
        free(promises[i]);
    }
    printf("warmed up\n");

    // Do 90-10 get-put
    do_experiment_zipf(do_random_zipf_action, 0.9, true, "90-10 GET-PUT");

    // Do 90-10 get-put
    do_experiment_zipf(do_random_zipf_action, 0.5, true, "50-50 GET-PUT");

    // Do 90-10 get-put
    do_experiment_zipf(do_random_zipf_action, 0.1, true, "10-90 GET-PUT");

    // put
    do_experiment_zipf(do_random_zipf_action, 0, true, "PUT");
    

    // 2 sided get
    do_experiment_zipf(do_random_zipf_action, 1, true, "2 Sided GET");

    // 1 sided get
    do_experiment_zipf(do_random_zipf_action, 1, false, "1 Sided GET");

#if DO_NON_BATCH
    // 2 sided get - non batch
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        promises[i] = do_random_zipf_action(keys[key_for_sample[i]], sample_data, VALUE_LEN, 1, true);
        while(promises[i]->result == PROMISE_PENDING);
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
        }
        errors[promises[i]->result]++;
        free(promises[i]);
    }
    printf("2 sided GET - non batched - took %ld ns pr with zipf\n", (((end.tv_sec - start.tv_sec) * 1000000000L) + (end.tv_nsec - start.tv_nsec))/NUM_SAMPLES);
    for (uint32_t i = 0; i < REQUEST_PROMISE_STATUS_COUNT; i++) {
        printf("    Status %d: %d\n", i, errors[i]);
    }
#endif

    // Uniform time
    // Do 90-10 get-put
    do_experiment_uniform(do_random_action, 0.9, true, "90-10 GET-PUT");

    // Do 90-10 get-put
    do_experiment_uniform(do_random_action, 0.5, true, "50-50 GET-PUT");

    // Do 90-10 get-put
    do_experiment_uniform(do_random_action, 0.1, true, "10-90 GET-PUT");

    // put
    do_experiment_uniform(do_random_action, 0, true, "PUT");

    // 2 sided get
    do_experiment_uniform(do_random_action, 1, true, "2 Sided GET");

    // 1 sided get
    do_experiment_uniform(do_random_action, 1, false, "1 Sided GET");

#if DO_NON_BATCH
    // 2 sided get - non batch
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (uint32_t i = 0; i < NUM_SAMPLES; i++) {
        promises[i] = do_random_action(sample_data, VALUE_LEN, 1, true);
        while(promises[i]->result == PROMISE_PENDING);
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
        }
        errors[promises[i]->result]++;
        free(promises[i]);
    }
    printf("2 sided GET - non batched - took %ld ns pr without zipf\n", (((end.tv_sec - start.tv_sec) * 1000000000L) + (end.tv_nsec - start.tv_nsec))/NUM_SAMPLES);
    for (uint32_t i = 0; i < REQUEST_PROMISE_STATUS_COUNT; i++) {
        printf("    Status %d: %d\n", i, errors[i]);
    }
#endif

    printf("Completed!\n");

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

static int zipf_sample(const double* cdf) {
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

static request_promise_t *do_random_zipf_action(const char *key, unsigned char *data, uint32_t value_len, double chance_for_get, bool get_2_sided) {
    double rand_0_to_1 = (double)rand() / (double)RAND_MAX;
    if (rand_0_to_1 > chance_for_get)
        return put_blocking_until_available_put_request_region_slot(key, 36, data, value_len);
    else if (get_2_sided) return get_2_phase_2_sided(key, 36);
    else return get_2_phase_1_sided(key, 36);
}

static request_promise_t *do_random_action(unsigned char *data, uint32_t value_len, double chance_for_get, bool get_2_sided) {
    int key_index = rand() % NUM_KEYS;

    double rand_0_to_1 = (double)rand() / (double)RAND_MAX;
    if (rand_0_to_1 > chance_for_get)
        return put_blocking_until_available_put_request_region_slot(keys[key_index], 36, data, value_len);
    else if (get_2_sided) return get_2_phase_2_sided(keys[key_index], 36);
    else return get_2_phase_1_sided(keys[key_index], 36);
}
