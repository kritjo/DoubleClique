#include "profiler_report_layout.h"

#include "profiler_metrics.h"

#define ARRAY_LEN(a) (sizeof(a) / sizeof((a)[0]))

static const char *const g_server_metric_names[SERVER_PROF_METRIC_COUNT] = {
    [PROF_SERVER_REQUESTS_PROCESSED] = "server.requests_processed",
    [PROF_SERVER_POLL_SLOT_TOTAL] = "server.poll.slot_total",
    [PROF_SERVER_POLL_KEY_COPY] = "server.poll.key_copy",
    [PROF_SERVER_POLL_KEY_HASH] = "server.poll.key_hash",
    [PROF_SERVER_PUT_TOTAL] = "server.put.total",
    [PROF_SERVER_PUT_READ_COPY] = "server.put.read_copy",
    [PROF_SERVER_PUT_HASH_VERIFY] = "server.put.hash_verify",
    [PROF_SERVER_PUT_INDEX_LOOKUP] = "server.put.index_lookup",
    [PROF_SERVER_PUT_RETRY_INDEX_LOOKUP] = "server.put.retry_index_lookup",
    [PROF_SERVER_PUT_FIND_FREE_INDEX_SLOT] = "server.put.find_free_index_slot",
    [PROF_SERVER_PUT_ALLOC] = "server.put.alloc",
    [PROF_SERVER_PUT_GC_ENQUEUE] = "server.put.gc_enqueue",
    [PROF_SERVER_PUT_GC_GET_CLOCK] = "server.put.gc_get_clock",
    [PROF_SERVER_PUT_GC_ENQUEUE_CALL] = "server.put.gc_enqueue_call",
    [PROF_SERVER_PUT_INSERT] = "server.put.insert",
    [PROF_SERVER_PUT_MARK_HEADER_UNUSED] = "server.put.mark_header_unused",
    [PROF_SERVER_PUT_ACK] = "server.put.ack",
    [PROF_SERVER_GET1_ACK_TOTAL] = "server.get1.ack_total",
    [PROF_SERVER_GET1_COPY_BUCKET] = "server.get1.copy_bucket",
    [PROF_SERVER_GET1_COPY_BUCKET_PLAIN] = "server.get1.copy_bucket_plain",
    [PROF_SERVER_GET1_COPY_BUCKET_WRITEBACK_SCAN] = "server.get1.copy_bucket_writeback_scan",
    [PROF_SERVER_GET1_COPY_BUCKET_WRITEBACK_COPY_TO_ACK] = "server.get1.copy_bucket_writeback_copy_to_ack",
    [PROF_SERVER_GET1_COPY_BUCKET_WRITEBACK_MATCH_CHECK] = "server.get1.copy_bucket_writeback_match_check",
    [PROF_SERVER_GET1_WRITEBACK_COPY] = "server.get1.writeback_copy",
    [PROF_SERVER_GET1_ACK_FINALIZE] = "server.get1.ack_finalize",
    [PROF_SERVER_GET2_ACK_TOTAL] = "server.get2.ack_total",
    [PROF_SERVER_GET2_COPY] = "server.get2.copy",
};

static const perf_report_node_t g_server_poll_slot_children[] = {
    {PROF_SERVER_POLL_KEY_COPY, "key_copy", NULL, 0},
    {PROF_SERVER_POLL_KEY_HASH, "key_hash", NULL, 0},
};

static const perf_report_node_t g_server_put_gc_enqueue_children[] = {
    {PROF_SERVER_PUT_GC_GET_CLOCK, "get_clock", NULL, 0},
    {PROF_SERVER_PUT_GC_ENQUEUE_CALL, "enqueue_call", NULL, 0},
};

static const perf_report_node_t g_server_put_children[] = {
    {PROF_SERVER_PUT_READ_COPY, "read_copy", NULL, 0},
    {PROF_SERVER_PUT_HASH_VERIFY, "hash_verify", NULL, 0},
    {PROF_SERVER_PUT_INDEX_LOOKUP, "index_lookup", NULL, 0},
    {PROF_SERVER_PUT_RETRY_INDEX_LOOKUP, "retry_index_lookup", NULL, 0},
    {PROF_SERVER_PUT_FIND_FREE_INDEX_SLOT, "find_free_index_slot", NULL, 0},
    {PROF_SERVER_PUT_ALLOC, "alloc", NULL, 0},
    {PROF_SERVER_PUT_GC_ENQUEUE, "gc_enqueue", g_server_put_gc_enqueue_children, ARRAY_LEN(g_server_put_gc_enqueue_children)},
    {PROF_SERVER_PUT_INSERT, "insert", NULL, 0},
    {PROF_SERVER_PUT_MARK_HEADER_UNUSED, "mark_header_unused", NULL, 0},
    {PROF_SERVER_PUT_ACK, "ack", NULL, 0},
};

static const perf_report_node_t g_server_get1_writeback_scan_children[] = {
    {PROF_SERVER_GET1_COPY_BUCKET_WRITEBACK_COPY_TO_ACK, "copy_to_ack", NULL, 0},
    {PROF_SERVER_GET1_COPY_BUCKET_WRITEBACK_MATCH_CHECK, "match_check", NULL, 0},
    {PROF_SERVER_GET1_WRITEBACK_COPY, "writeback_copy", NULL, 0},
};

static const perf_report_node_t g_server_get1_copy_bucket_branch_children[] = {
    {PROF_SERVER_GET1_COPY_BUCKET_PLAIN, "plain", NULL, 0},
    {PROF_SERVER_GET1_COPY_BUCKET_WRITEBACK_SCAN, "writeback_scan", g_server_get1_writeback_scan_children, ARRAY_LEN(g_server_get1_writeback_scan_children)},
};

static const perf_report_node_t g_server_get1_ack_children[] = {
    {PROF_SERVER_GET1_COPY_BUCKET, "copy_bucket", g_server_get1_copy_bucket_branch_children, ARRAY_LEN(g_server_get1_copy_bucket_branch_children)},
    {PROF_SERVER_GET1_ACK_FINALIZE, "ack_finalize", NULL, 0},
};

static const perf_report_node_t g_server_get2_ack_children[] = {
    {PROF_SERVER_GET2_COPY, "copy", NULL, 0},
};

static const perf_report_node_t g_server_report_roots[] = {
    {PROF_SERVER_REQUESTS_PROCESSED, "server.requests_processed", NULL, 0},
    {PROF_SERVER_POLL_SLOT_TOTAL, "server.poll.slot_total", g_server_poll_slot_children, ARRAY_LEN(g_server_poll_slot_children)},
    {PROF_SERVER_PUT_TOTAL, "server.put.total", g_server_put_children, ARRAY_LEN(g_server_put_children)},
    {PROF_SERVER_GET1_ACK_TOTAL, "server.get1.ack_total", g_server_get1_ack_children, ARRAY_LEN(g_server_get1_ack_children)},
    {PROF_SERVER_GET2_ACK_TOTAL, "server.get2.ack_total", g_server_get2_ack_children, ARRAY_LEN(g_server_get2_ack_children)},
};

const perf_report_node_t *server_profiler_report_roots(size_t *count) {
    if (count != NULL) {
        *count = ARRAY_LEN(g_server_report_roots);
    }
    return g_server_report_roots;
}

const char *const *server_profiler_metric_names(size_t *count) {
    if (count != NULL) {
        *count = ARRAY_LEN(g_server_metric_names);
    }
    return g_server_metric_names;
}

const char *server_profiler_report_title(void) {
    return "Server Micro Profile (Layered)";
}
