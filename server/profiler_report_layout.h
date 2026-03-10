#ifndef DOUBLECLIQUE_SERVER_PROFILER_REPORT_LAYOUT_H
#define DOUBLECLIQUE_SERVER_PROFILER_REPORT_LAYOUT_H

#include <stddef.h>

#include "profiler.h"

const perf_report_node_t *server_profiler_report_roots(size_t *count);
const char *const *server_profiler_metric_names(size_t *count);
const char *server_profiler_report_title(void);

#endif //DOUBLECLIQUE_SERVER_PROFILER_REPORT_LAYOUT_H
