#include "main.h"

#include <immintrin.h>
#include <stdlib.h>
#include <stdio.h>
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
#include "profiler.h"

#define NUM_KEYS 13107
#define THETA 0.99
#define NUM_SAMPLES 100000
#define VALUE_LEN 8

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

typedef struct {
    char experiment_name[64];
    char distribution_name[16];
    uint32_t max_inflight;
    uint64_t wall_ns;
    uint64_t avg_latency_ns;
    uint64_t p95_ns;
    uint64_t p99_ns;
    double throughput_ops_per_sec;
    uint32_t status_counts[REQUEST_PROMISE_STATUS_COUNT];
} experiment_result_t;

#define MAX_EXPERIMENT_RESULTS 256
static experiment_result_t g_experiment_results[MAX_EXPERIMENT_RESULTS];
static size_t g_experiment_result_count = 0;

static void add_experiment_result(
    const char *experiment_name,
    const char *distribution_name,
    uint32_t max_inflight,
    uint64_t wall_ns,
    uint64_t avg_latency_ns,
    uint64_t p95_ns,
    uint64_t p99_ns,
    double throughput_ops_per_sec,
    const uint32_t status_counts[REQUEST_PROMISE_STATUS_COUNT]
) {
    if (g_experiment_result_count >= MAX_EXPERIMENT_RESULTS) {
        return;
    }

    experiment_result_t *dst = &g_experiment_results[g_experiment_result_count++];
    snprintf(dst->experiment_name, sizeof(dst->experiment_name), "%s", experiment_name);
    snprintf(dst->distribution_name, sizeof(dst->distribution_name), "%s", distribution_name);
    dst->max_inflight = max_inflight;
    dst->wall_ns = wall_ns;
    dst->avg_latency_ns = avg_latency_ns;
    dst->p95_ns = p95_ns;
    dst->p99_ns = p99_ns;
    dst->throughput_ops_per_sec = throughput_ops_per_sec;
    memcpy(dst->status_counts, status_counts, sizeof(dst->status_counts));
}

static void print_experiment_result_report(void) {
    printf("\n=== Client Experiment Summary ===\n");
    printf("%-8s %-16s %10s %14s %12s %12s %12s %10s\n",
           "dist",
           "workload",
           "inflight",
           "throughput",
           "avg us",
           "p95 us",
           "p99 us",
           "ok %");

    for (size_t i = 0; i < g_experiment_result_count; i++) {
        const experiment_result_t *r = &g_experiment_results[i];
        uint32_t ok = r->status_counts[PROMISE_SUCCESS] + r->status_counts[PROMISE_SUCCESS_PH1];
        double ok_percent = NUM_SAMPLES > 0 ? ((double) ok * 100.0) / (double) NUM_SAMPLES : 0.0;

        printf("%-8s %-16s %10u %14.2f %12.2f %12.2f %12.2f %10.2f\n",
               r->distribution_name,
               r->experiment_name,
               r->max_inflight,
               r->throughput_ops_per_sec,
               (double) r->avg_latency_ns / 1e3,
               (double) r->p95_ns / 1e3,
               (double) r->p99_ns / 1e3,
               ok_percent);
    }
}

typedef request_promise_t *(*submit_fn_t)(
    void *ctx,
    uint32_t sample_idx,
    double chance_for_get,
    bool get_2_sided
);

struct uniform_submit_ctx {
    request_promise_t *(*promise_func)(
        unsigned char *data,
        uint32_t value_len,
        double chance_for_get,
        bool get_2_sided
    );
};

struct zipf_submit_ctx {
    request_promise_t *(*promise_func)(
        const char *key,
        unsigned char *data,
        uint32_t value_len,
        double chance_for_get,
        bool get_2_sided
    );
};

static request_promise_t *submit_uniform(
    void *ctx,
    uint32_t sample_idx,
    double chance_for_get,
    bool get_2_sided
) {
    (void)sample_idx;
    struct uniform_submit_ctx *u = ctx;
    return u->promise_func(sample_data, VALUE_LEN, chance_for_get, get_2_sided);
}

static request_promise_t *submit_zipf(
    void *ctx,
    uint32_t sample_idx,
    double chance_for_get,
    bool get_2_sided
) {
    struct zipf_submit_ctx *z = ctx;
    return z->promise_func(
        keys[key_for_sample[sample_idx]],
        sample_data,
        VALUE_LEN,
        chance_for_get,
        get_2_sided
    );
}

static void do_experiment_with_max_inflight(
    submit_fn_t submit_fn,
    void *submit_ctx,
    double chance_for_get,
    bool get_2_sided,
    const char *experiment_name,
    const char *distribution_name,
    uint32_t max_inflight
) {
    if (max_inflight == 0) max_inflight = 1;
    if (max_inflight > NUM_SAMPLES) max_inflight = NUM_SAMPLES;

    request_promise_t **active = calloc(max_inflight, sizeof(*active));
    uint64_t *latencies = malloc(NUM_SAMPLES * sizeof(*latencies));
    uint32_t errors[REQUEST_PROMISE_STATUS_COUNT] = {0};

    if (!active || !latencies) {
        fprintf(stderr, "allocation failed\n");
        free(active);
        free(latencies);
        return;
    }

    sleep(1);

    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);

    uint32_t next_to_submit = 0;
    uint32_t completed = 0;
    uint64_t total_latency_ns = 0;

    for (uint32_t slot = 0; slot < max_inflight && next_to_submit < NUM_SAMPLES; slot++) {
        active[slot] = submit_fn(submit_ctx, next_to_submit, chance_for_get, get_2_sided);
        next_to_submit++;
    }

    while (completed < NUM_SAMPLES) {
        bool made_progress = false;

        for (uint32_t slot = 0; slot < max_inflight; slot++) {
            request_promise_t *p = active[slot];
            if (p == NULL) continue;
            if (p->result == PROMISE_PENDING) continue;

            made_progress = true;

            errors[p->result]++;

            latencies[completed] =
                (uint64_t)p->duration.tv_sec * NS_PER_SEC +
                (uint64_t)p->duration.tv_nsec;
            total_latency_ns += latencies[completed];
            completed++;

            if (p->result == PROMISE_SUCCESS && p->operation == OP_GET) {
                free(p->data);
            }
            free(p);

            if (next_to_submit < NUM_SAMPLES) {
                active[slot] = submit_fn(
                    submit_ctx,
                    next_to_submit,
                    chance_for_get,
                    get_2_sided
                );
                next_to_submit++;
            } else {
                active[slot] = NULL;
            }
        }

        if (!made_progress) {
            _mm_pause();
        }
    }

    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);

    time_t wall_sec = end.tv_sec - start.tv_sec;
    long wall_nsec = end.tv_nsec - start.tv_nsec;
    if (wall_nsec < 0) {
        wall_sec--;
        wall_nsec += 1000000000L;
    }

    uint64_t wall_ns = (uint64_t)wall_sec * NS_PER_SEC + (uint64_t)wall_nsec;

    qsort(latencies, NUM_SAMPLES, sizeof(latencies[0]), cmp_u64);

    uint64_t avg_latency_ns = total_latency_ns / NUM_SAMPLES;
    uint64_t p50_ns = latencies[((NUM_SAMPLES - 1) * 50) / 100];
    uint64_t p95_ns = latencies[((NUM_SAMPLES - 1) * 95) / 100];
    uint64_t p99_ns = latencies[((NUM_SAMPLES - 1) * 99) / 100];
    uint64_t time_per_op_ns = wall_ns / NUM_SAMPLES;

    double throughput_ops_per_sec =
        wall_ns ? ((double)NUM_SAMPLES * 1e9) / (double)wall_ns : 0.0;

    double avg_in_flight =
        wall_ns ? (double)total_latency_ns / (double)wall_ns : 0.0;

    printf("%s with %u %s samples (max_inflight=%u)\n",
           experiment_name, NUM_SAMPLES, distribution_name, max_inflight);
    printf("    wall time                 : %" PRIu64 " ns\n", wall_ns);
    printf("    time/op at throughput     : %" PRIu64 " ns\n", time_per_op_ns);
    printf("    avg latency               : %" PRIu64 " ns\n", avg_latency_ns);
    printf("    p50 latency               : %" PRIu64 " ns\n", p50_ns);
    printf("    p95 latency               : %" PRIu64 " ns\n", p95_ns);
    printf("    p99 latency               : %" PRIu64 " ns\n", p99_ns);
    printf("    throughput                : %.2f ops/sec\n", throughput_ops_per_sec);
    printf("    avg in-flight             : %.2f\n", avg_in_flight);

    for (uint32_t i = 0; i < REQUEST_PROMISE_STATUS_COUNT; i++) {
        printf("    Status %u: %u\n", i, errors[i]);
    }

    add_experiment_result(
        experiment_name,
        distribution_name,
        max_inflight,
        wall_ns,
        avg_latency_ns,
        p95_ns,
        p99_ns,
        throughput_ops_per_sec,
        errors
    );

    free(active);
    free(latencies);
}

static void do_experiment_uniform_with_max_inflight(
    request_promise_t *(*promise_func)(
        unsigned char *data,
        uint32_t value_len,
        double chance_for_get,
        bool get_2_sided
    ),
    double chance_for_get,
    bool get_2_sided,
    const char *experiment_name,
    uint32_t max_inflight
) {
    struct uniform_submit_ctx ctx = { .promise_func = promise_func };
    do_experiment_with_max_inflight(
        submit_uniform,
        &ctx,
        chance_for_get,
        get_2_sided,
        experiment_name,
        "uniform",
        max_inflight
    );
}

static void do_experiment_zipf_with_max_inflight(
    request_promise_t *(*promise_func)(
        const char *key,
        unsigned char *data,
        uint32_t value_len,
        double chance_for_get,
        bool get_2_sided
    ),
    double chance_for_get,
    bool get_2_sided,
    const char *experiment_name,
    uint32_t max_inflight
) {
    struct zipf_submit_ctx ctx = { .promise_func = promise_func };
    do_experiment_with_max_inflight(
        submit_zipf,
        &ctx,
        chance_for_get,
        get_2_sided,
        experiment_name,
        "zipf",
        max_inflight
    );
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

    perf_reset_all();

    const uint32_t inflights[] = {1, 2, 4, 8, 16, 32, 64, 128};

    struct {
        double chance_for_get;
        bool get_2_sided;
        const char *name;
    } cases[] = {
        {0.9, true,  "90-10 GET-PUT"},
        {0.5, true,  "50-50 GET-PUT"},
        {0.1, true,  "10-90 GET-PUT"},
        {0.0, true,  "PUT"},
        {1.0, true,  "2 Sided GET"},
        {1.0, false, "1 Sided GET"},
    };

    for (uint32_t j = 0; j < sizeof(inflights) / sizeof(inflights[0]); j++) {
        uint32_t max_inflight = inflights[j];

        printf("\n=== max_inflight = %u ===\n", max_inflight);

        for (uint32_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
            do_experiment_zipf_with_max_inflight(
                do_random_zipf_action,
                cases[i].chance_for_get,
                cases[i].get_2_sided,
                cases[i].name,
                max_inflight
            );
        }

        for (uint32_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
            do_experiment_uniform_with_max_inflight(
                do_random_action,
                cases[i].chance_for_get,
                cases[i].get_2_sided,
                cases[i].name,
                max_inflight
            );
        }
    }

    print_experiment_result_report();
    perf_print_client_report();
    printf("\nCompleted!\n");

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
