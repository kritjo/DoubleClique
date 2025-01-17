#ifndef SISCI_PERF_SISCI_GLOB_DEFS_H
#define SISCI_PERF_SISCI_GLOB_DEFS_H

#define PUT_REQUEST_SEGMENT_ID 1

#define MIN_SIZE_ELEMENT 8

static int replica_index_segment_id[3] = {101, 102, 103};
static int replica_data_segment_id[3] = {201, 202, 203};

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
