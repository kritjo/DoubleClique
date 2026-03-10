#ifndef DOUBLECLIQUE_PROFILER_H
#define DOUBLECLIQUE_PROFILER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct perf_report_node {
    uint32_t id;
    const char *label;
    const struct perf_report_node *children;
    size_t child_count;
} perf_report_node_t;

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

#endif //DOUBLECLIQUE_PROFILER_H
