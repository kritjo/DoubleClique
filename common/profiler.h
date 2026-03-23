#ifndef DOUBLECLIQUE_PROFILER_H
#define DOUBLECLIQUE_PROFILER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifndef PERF_ENABLED
#define PERF_ENABLED 0
#endif

typedef struct perf_report_node {
    uint32_t id;
    const char *label;
    const struct perf_report_node *children;
    size_t child_count;
} perf_report_node_t;

#if PERF_ENABLED || defined(PERF_INTERNAL_IMPL)
uint64_t perf_now_ns(void);
void perf_record_ns(uint32_t id, uint64_t duration_ns);
void perf_record_ns_bytes(uint32_t id, uint64_t duration_ns, uint64_t bytes);
void perf_increment(uint32_t id, uint64_t amount);
void perf_reset_all(void);
void perf_print_report(const char *title,
                       const perf_report_node_t *roots,
                       size_t root_count,
                       const char *const *metric_names,
                       size_t metric_name_count,
                       bool clear_after_print);
#else
#define perf_now_ns() (0ULL)
#define perf_record_ns(id, duration_ns) \
    do { \
        (void) sizeof(id); \
        (void) sizeof(duration_ns); \
    } while (0)
#define perf_record_ns_bytes(id, duration_ns, bytes) \
    do { \
        (void) sizeof(id); \
        (void) sizeof(duration_ns); \
        (void) sizeof(bytes); \
    } while (0)
#define perf_increment(id, amount) \
    do { \
        (void) sizeof(id); \
        (void) sizeof(amount); \
    } while (0)
#define perf_reset_all() ((void) 0)
#define perf_print_report(title, roots, root_count, metric_names, metric_name_count, clear_after_print) \
    do { \
        (void) sizeof(title); \
        (void) sizeof(roots); \
        (void) sizeof(root_count); \
        (void) sizeof(metric_names); \
        (void) sizeof(metric_name_count); \
        (void) sizeof(clear_after_print); \
    } while (0)
#endif

#endif //DOUBLECLIQUE_PROFILER_H
