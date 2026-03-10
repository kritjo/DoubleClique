#include "profiler.h"

#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <time.h>
#include <x86intrin.h>

#if !defined(__x86_64__) && !defined(__i386__)
#error "DoubleClique profiler requires x86/x86_64 (RDTSCP backend)"
#endif

typedef struct {
    _Atomic uint64_t count;
    _Atomic uint64_t total_ns;
    _Atomic uint64_t min_ns;
    _Atomic uint64_t max_ns;
    _Atomic uint64_t bytes;
} profile_metric_t;

typedef struct {
    uint64_t count;
    uint64_t total_ns;
    uint64_t min_ns;
    uint64_t max_ns;
    uint64_t bytes;
} metric_snapshot_t;

enum {
    PERF_METRIC_CAPACITY = 1024,
    CALIBRATION_SAMPLES = 15,
    CALIBRATION_WINDOW_NS = 5 * 1000 * 1000,
    OVERHEAD_SAMPLES = 4096
};

static profile_metric_t g_metrics[PERF_METRIC_CAPACITY];
static pthread_once_t g_profile_once = PTHREAD_ONCE_INIT;

static double g_timer_ns_per_cycle = 0.0;
static uint64_t g_timer_base_cycles = 0;
static uint64_t g_timer_base_ns = 0;
static double g_timer_read_overhead_ns = 0.0;
static double g_timer_pair_overhead_ns = 0.0;

static uint64_t monotonic_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t) ts.tv_sec * 1000000000ULL + (uint64_t) ts.tv_nsec;
}

static inline uint64_t rdtscp_cycles(void) {
    unsigned int aux;
    return (uint64_t) __rdtscp(&aux);
}

static int compare_double_asc(const void *lhs, const void *rhs) {
    double a = *(const double *) lhs;
    double b = *(const double *) rhs;
    if (a < b) {
        return -1;
    }
    if (a > b) {
        return 1;
    }
    return 0;
}

static void calibrate_rdtscp_to_ns(void) {
    double ns_per_cycle_samples[CALIBRATION_SAMPLES];

    for (size_t i = 0; i < CALIBRATION_SAMPLES; i++) {
        uint64_t wall_start_ns = monotonic_now_ns();
        uint64_t cycle_start = rdtscp_cycles();
        uint64_t wall_now_ns = wall_start_ns;

        while (wall_now_ns - wall_start_ns < CALIBRATION_WINDOW_NS) {
            wall_now_ns = monotonic_now_ns();
        }

        uint64_t cycle_end = rdtscp_cycles();
        uint64_t wall_end_ns = monotonic_now_ns();
        uint64_t elapsed_cycles = cycle_end - cycle_start;
        uint64_t elapsed_ns = wall_end_ns - wall_start_ns;

        if (elapsed_cycles == 0 || elapsed_ns == 0) {
            fprintf(stderr, "RDTSCP calibration failed: zero elapsed interval\n");
            exit(EXIT_FAILURE);
        }

        ns_per_cycle_samples[i] = (double) elapsed_ns / (double) elapsed_cycles;
    }

    qsort(ns_per_cycle_samples,
          CALIBRATION_SAMPLES,
          sizeof(ns_per_cycle_samples[0]),
          compare_double_asc);

    g_timer_ns_per_cycle = ns_per_cycle_samples[CALIBRATION_SAMPLES / 2];
    if (!(g_timer_ns_per_cycle > 0.0)) {
        fprintf(stderr, "RDTSCP calibration failed: invalid ns/cycle factor\n");
        exit(EXIT_FAILURE);
    }

    g_timer_base_cycles = rdtscp_cycles();
    g_timer_base_ns = monotonic_now_ns();
}

static void measure_rdtscp_overhead(void) {
    uint64_t min_single_cycles = UINT64_MAX;
    uint64_t min_pair_cycles = UINT64_MAX;

    for (size_t i = 0; i < OVERHEAD_SAMPLES; i++) {
        uint64_t c0 = rdtscp_cycles();
        uint64_t c1 = rdtscp_cycles();
        uint64_t c2 = rdtscp_cycles();

        uint64_t single_cycles = c1 - c0;
        uint64_t pair_cycles = c2 - c0;

        if (single_cycles < min_single_cycles) {
            min_single_cycles = single_cycles;
        }
        if (pair_cycles < min_pair_cycles) {
            min_pair_cycles = pair_cycles;
        }
    }

    g_timer_read_overhead_ns = (double) min_single_cycles * g_timer_ns_per_cycle;
    g_timer_pair_overhead_ns = (double) min_pair_cycles * g_timer_ns_per_cycle;
}

static uint64_t rdtscp_now_ns(void) {
    uint64_t cycle_now = rdtscp_cycles();
    uint64_t elapsed_cycles = cycle_now - g_timer_base_cycles;
    double elapsed_ns = (double) elapsed_cycles * g_timer_ns_per_cycle;
    return g_timer_base_ns + (uint64_t) elapsed_ns;
}

static void profile_reset_metric(size_t id) {
    atomic_store_explicit(&g_metrics[id].count, 0, memory_order_relaxed);
    atomic_store_explicit(&g_metrics[id].total_ns, 0, memory_order_relaxed);
    atomic_store_explicit(&g_metrics[id].min_ns, UINT64_MAX, memory_order_relaxed);
    atomic_store_explicit(&g_metrics[id].max_ns, 0, memory_order_relaxed);
    atomic_store_explicit(&g_metrics[id].bytes, 0, memory_order_relaxed);
}

static void profile_init(void) {
    calibrate_rdtscp_to_ns();
    measure_rdtscp_overhead();

    for (size_t i = 0; i < PERF_METRIC_CAPACITY; i++) {
        profile_reset_metric(i);
    }
}

static void profile_ensure_initialized(void) {
    pthread_once(&g_profile_once, profile_init);
}

static void profile_record_min(_Atomic uint64_t *target, uint64_t value) {
    uint64_t current = atomic_load_explicit(target, memory_order_relaxed);
    while (value < current &&
           !atomic_compare_exchange_weak_explicit(target,
                                                  &current,
                                                  value,
                                                  memory_order_relaxed,
                                                  memory_order_relaxed)) {
    }
}

static void profile_record_max(_Atomic uint64_t *target, uint64_t value) {
    uint64_t current = atomic_load_explicit(target, memory_order_relaxed);
    while (value > current &&
           !atomic_compare_exchange_weak_explicit(target,
                                                  &current,
                                                  value,
                                                  memory_order_relaxed,
                                                  memory_order_relaxed)) {
    }
}

static void profile_record_impl(uint32_t id, uint64_t duration_ns, uint64_t bytes) {
    size_t idx = (size_t) id;
    if (idx >= PERF_METRIC_CAPACITY) {
        return;
    }

    profile_ensure_initialized();
    profile_metric_t *metric = &g_metrics[idx];

    atomic_fetch_add_explicit(&metric->count, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&metric->total_ns, duration_ns, memory_order_relaxed);
    atomic_fetch_add_explicit(&metric->bytes, bytes, memory_order_relaxed);

    profile_record_min(&metric->min_ns, duration_ns);
    profile_record_max(&metric->max_ns, duration_ns);
}

uint64_t perf_now_ns(void) {
    profile_ensure_initialized();
    return rdtscp_now_ns();
}

void perf_record_ns(uint32_t id, uint64_t duration_ns) {
    profile_record_impl(id, duration_ns, 0);
}

void perf_record_ns_bytes(uint32_t id, uint64_t duration_ns, uint64_t bytes) {
    profile_record_impl(id, duration_ns, bytes);
}

void perf_increment(uint32_t id, uint64_t amount) {
    size_t idx = (size_t) id;
    if (idx >= PERF_METRIC_CAPACITY) {
        return;
    }

    profile_ensure_initialized();
    atomic_fetch_add_explicit(&g_metrics[idx].count, amount, memory_order_relaxed);
}

void perf_reset_all(void) {
    profile_ensure_initialized();
    for (size_t i = 0; i < PERF_METRIC_CAPACITY; i++) {
        profile_reset_metric(i);
    }
}

static metric_snapshot_t snapshot_metric(uint32_t id) {
    metric_snapshot_t snapshot = {0};
    size_t idx = (size_t) id;

    if (idx >= PERF_METRIC_CAPACITY) {
        return snapshot;
    }

    profile_metric_t *metric = &g_metrics[idx];
    snapshot.count = atomic_load_explicit(&metric->count, memory_order_relaxed);
    snapshot.total_ns = atomic_load_explicit(&metric->total_ns, memory_order_relaxed);
    snapshot.min_ns = atomic_load_explicit(&metric->min_ns, memory_order_relaxed);
    snapshot.max_ns = atomic_load_explicit(&metric->max_ns, memory_order_relaxed);
    snapshot.bytes = atomic_load_explicit(&metric->bytes, memory_order_relaxed);

    if (snapshot.min_ns == UINT64_MAX) {
        snapshot.min_ns = 0;
    }

    return snapshot;
}

static void print_metric_row(const char *label,
                             int depth,
                             const metric_snapshot_t *metric,
                             const metric_snapshot_t *parent) {
    char full_label[96];
    snprintf(full_label, sizeof(full_label), "%*s%s", depth * 2, "", label);

    char parent_pct[16] = "-";
    if (parent != NULL && parent->total_ns > 0) {
        double pct = ((double) metric->total_ns * 100.0) / (double) parent->total_ns;
        snprintf(parent_pct, sizeof(parent_pct), "%.2f", pct);
    }

    char bandwidth[16] = "-";
    if (metric->bytes > 0 && metric->total_ns > 0) {
        double mb_per_sec = ((double) metric->bytes * 1e3) / (double) metric->total_ns;
        snprintf(bandwidth, sizeof(bandwidth), "%.2f", mb_per_sec);
    }

    double total_ms = (double) metric->total_ns / 1e6;
    double avg_us = metric->count > 0 ? (double) metric->total_ns / (double) metric->count / 1e3 : 0.0;
    double min_us = (double) metric->min_ns / 1e3;
    double max_us = (double) metric->max_ns / 1e3;

    printf("%-44s %10" PRIu64 " %12.3f %12.3f %12.3f %12.3f %10s %10s\n",
           full_label,
           metric->count,
           total_ms,
           avg_us,
           min_us,
           max_us,
           parent_pct,
           bandwidth);
}

static const char *resolve_metric_label(const perf_report_node_t *node,
                                        const char *const *metric_names,
                                        size_t metric_name_count,
                                        char *fallback,
                                        size_t fallback_size) {
    if (node->label != NULL) {
        return node->label;
    }

    size_t idx = (size_t) node->id;
    if (metric_names != NULL && idx < metric_name_count && metric_names[idx] != NULL) {
        return metric_names[idx];
    }

    snprintf(fallback, fallback_size, "metric_%u", (unsigned int) node->id);
    return fallback;
}

static metric_snapshot_t print_tree_node(const perf_report_node_t *node,
                                         const metric_snapshot_t *parent,
                                         int depth,
                                         const char *const *metric_names,
                                         size_t metric_name_count) {
    metric_snapshot_t metric = snapshot_metric(node->id);
    char fallback_label[32];
    const char *label = resolve_metric_label(node,
                                             metric_names,
                                             metric_name_count,
                                             fallback_label,
                                             sizeof(fallback_label));
    print_metric_row(label, depth, &metric, parent);

    uint64_t child_total_ns = 0;
    for (size_t i = 0; i < node->child_count; i++) {
        metric_snapshot_t child = print_tree_node(&node->children[i],
                                                  &metric,
                                                  depth + 1,
                                                  metric_names,
                                                  metric_name_count);
        child_total_ns += child.total_ns;
    }

    if (node->child_count > 0 && metric.count > 0) {
        if (child_total_ns < metric.total_ns) {
            metric_snapshot_t unattributed = {
                .count = metric.count,
                .total_ns = metric.total_ns - child_total_ns,
                .min_ns = 0,
                .max_ns = 0,
                .bytes = 0
            };
            print_metric_row("[unattributed]", depth + 1, &unattributed, &metric);
        } else if (child_total_ns > metric.total_ns) {
            metric_snapshot_t overlap = {
                .count = metric.count,
                .total_ns = child_total_ns - metric.total_ns,
                .min_ns = 0,
                .max_ns = 0,
                .bytes = 0
            };
            print_metric_row("[children_overlap]", depth + 1, &overlap, &metric);
        }
    }

    return metric;
}

static void print_layered_report(const char *title,
                                 const perf_report_node_t *roots,
                                 size_t root_count,
                                 const char *const *metric_names,
                                 size_t metric_name_count) {
    profile_ensure_initialized();

    printf("\n=== %s ===\n", title);
    printf("Timer backend: rdtscp_calibrated_ns (x86-only)\n");
    printf("Timer metrics: rdtscp_read_ns=%.2f rdtscp_pair_ns=%.2f. Tiny-span averages near this floor are timer-dominated.\n",
           g_timer_read_overhead_ns,
           g_timer_pair_overhead_ns);
    printf("Rows are hierarchical; %%parent is this row's share of its parent's total time.\n");
    printf("%-44s %10s %12s %12s %12s %12s %10s %10s\n",
           "metric",
           "count",
           "total ms",
           "avg us",
           "min us",
           "max us",
           "%parent",
           "MB/s");

    for (size_t i = 0; i < root_count; i++) {
        print_tree_node(&roots[i], NULL, 0, metric_names, metric_name_count);
        if (i + 1 < root_count) {
            printf("\n");
        }
    }
}

void perf_print_report(const char *title,
                       const perf_report_node_t *roots,
                       size_t root_count,
                       const char *const *metric_names,
                       size_t metric_name_count,
                       bool clear_after_print) {
    if (roots == NULL || root_count == 0) {
        fprintf(stderr, "perf_print_report requires non-empty roots\n");
        return;
    }
    if (title == NULL) {
        fprintf(stderr, "perf_print_report requires a non-null title\n");
        return;
    }

    print_layered_report(title,
                         roots,
                         root_count,
                         metric_names,
                         metric_name_count);

    if (clear_after_print) {
        perf_reset_all();
    }
}
