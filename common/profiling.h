#ifndef DOUBLECLIQUE_PROFILING_H
#define DOUBLECLIQUE_PROFILING_H

#include <time.h>
#include <stdio.h>
#include <string.h>

#define ENABLE_PROFILING
#ifdef ENABLE_PROFILING

typedef struct {
    char name[64];
    struct timespec start;
    struct timespec end;
    long accumulated_ns;
    int call_count;
} profile_timer_t;

#define MAX_TIMERS 100
static profile_timer_t timers[MAX_TIMERS];
static int timer_count = 0;

// Start timing a section
#define PROFILE_START(timer_name) \
    do { \
        int idx = -1; \
        for (int i = 0; i < timer_count; i++) { \
            if (strcmp(timers[i].name, timer_name) == 0) { \
                idx = i; \
                break; \
            } \
        } \
        if (idx == -1 && timer_count < MAX_TIMERS) { \
            idx = timer_count++; \
            strncpy(timers[idx].name, timer_name, 63); \
            timers[idx].accumulated_ns = 0; \
            timers[idx].call_count = 0; \
        } \
        if (idx != -1) { \
            clock_gettime(CLOCK_MONOTONIC, &timers[idx].start); \
        } \
    } while(0)

// End timing a section
#define PROFILE_END(timer_name) \
    do { \
        for (int i = 0; i < timer_count; i++) { \
            if (strcmp(timers[i].name, timer_name) == 0) { \
                clock_gettime(CLOCK_MONOTONIC, &timers[i].end); \
                long elapsed = (timers[i].end.tv_sec - timers[i].start.tv_sec) * 1000000000L + \
                               (timers[i].end.tv_nsec - timers[i].start.tv_nsec); \
                timers[i].accumulated_ns += elapsed; \
                timers[i].call_count++; \
                break; \
            } \
        } \
    } while(0)

#define CLEAR_TIMER(timer_name) \
    do { \
        for (int i = 0; i < timer_count; i++) { \
            if (strcmp(timers[i].name, timer_name) == 0) { \
                timers[i].accumulated_ns = 0; \
                timers[i].call_count = 0; \
                break; \
            } \
        } \
    } while(0)

// Print profiling report
static void print_profile_report(FILE* fp) {
    fprintf(fp, "\n=== PROFILING REPORT ===\n");
    fprintf(fp, "%-30s %15s %10s %15s\n", "Section", "Total (ms)", "Calls", "Avg (ms)");
    fprintf(fp, "%-30s %15s %10s %15s\n", "-------", "---------", "-----", "-------");

    for (int i = 0; i < timer_count; i++) {
        double total_ms = (double)timers[i].accumulated_ns / 1e6;
        double avg_ms = total_ms / timers[i].call_count;
        fprintf(fp, "%-30s %15.3f %10d %15.3f\n",
                timers[i].name, total_ms, timers[i].call_count, avg_ms);
    }
    fprintf(fp, "\n");
}

#else
// No-op macros when profiling is disabled
#define PROFILE_START(timer_name)
#define PROFILE_END(timer_name)
#define print_profile_report(fp)
#define CLEAR_TIMER(timer_name)
#endif

#endif //DOUBLECLIQUE_PROFILING_H
