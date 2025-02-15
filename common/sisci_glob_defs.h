#ifndef SISCI_PERF_SISCI_GLOB_DEFS_H
#define SISCI_PERF_SISCI_GLOB_DEFS_H

#include "power_of_two.h"

#define PUT_REQUEST_SEGMENT_ID 1
#define REPLICA_COUNT 1

#define MIN_SIZE_ELEMENT_EXP 3 // User needs to tune
#define MIN_SIZE_ELEMENT (POWER_OF_TWO(MIN_SIZE_ELEMENT_EXP))
#define MAX_SIZE_ELEMENT_EXP 11 // User needs to tune
#define MAX_SIZE_ELEMENT (POWER_OF_TWO(MAX_SIZE_ELEMENT_EXP))
#define PUT_REQUEST_BUCKETS (MAX_SIZE_ELEMENT_EXP - MIN_SIZE_ELEMENT_EXP + 1) // plus 1 because we want it to be inclusive

static int replica_index_segment_id[REPLICA_COUNT] = {101, 102, 103};
static int replica_data_segment_id[REPLICA_COUNT] = {201, 202, 203};

#define ADAPTER_NO 0

#define NO_FLAGS 0
#define NO_CALLBACK 0
#define NO_ARG 0
#define NO_OFFSET 0
#define NO_SUGGESTED_ADDRESS 0

#include <unistd.h>
#include <limits.h>
#include <stdio.h>

// SISCI Exit On Error -- SEOE
#define SEOE(func, ...) \
do {                    \
    sci_error_t seoe_error; \
    func(__VA_ARGS__, &seoe_error); \
    if (seoe_error != SCI_ERR_OK) { \
        fprintf(stderr, "%s failed: %s\n", #func, SCIGetErrorString(seoe_error)); \
        exit(EXIT_FAILURE); \
    } \
} while (0)

#endif //SISCI_PERF_SISCI_GLOB_DEFS_H
