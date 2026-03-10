#ifndef DOUBLECLIQUE_CLIENT_PROFILER_REPORT_LAYOUT_H
#define DOUBLECLIQUE_CLIENT_PROFILER_REPORT_LAYOUT_H

#include <stddef.h>

#include "profiler.h"

const perf_report_node_t *client_profiler_report_roots(size_t *count);
const char *const *client_profiler_metric_names(size_t *count);
const char *client_profiler_report_title(void);

#endif //DOUBLECLIQUE_CLIENT_PROFILER_REPORT_LAYOUT_H
