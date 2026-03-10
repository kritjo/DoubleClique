#include "profiler.h"

#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <time.h>

typedef struct {
    _Atomic uint64_t count;
    _Atomic uint64_t total_ns;
    _Atomic uint64_t min_ns;
    _Atomic uint64_t max_ns;
    _Atomic uint64_t bytes;
} profile_metric_t;

static profile_metric_t g_metrics[PROF_METRIC_COUNT];
static pthread_once_t g_profile_once = PTHREAD_ONCE_INIT;

static const char *const g_metric_names[PROF_METRIC_COUNT] = {
    [PROF_CLIENT_ACK_GET_QUEUE_WAIT] = "client.ack.get_queue_wait",
    [PROF_CLIENT_ACK_HEADER_SLOT_WAIT] = "client.ack.header_slot_wait",
    [PROF_CLIENT_ACK_RETRY_PAUSE] = "client.ack.retry_pause",
    [PROF_CLIENT_ACK_MUTEX_LOCK_WAIT] = "client.ack.mutex_lock_wait",
    [PROF_CLIENT_ACK_MUTEX_UNLOCK_WAIT] = "client.ack.mutex_unlock_wait",
    [PROF_CLIENT_ACK_CRITICAL_SECTION] = "client.ack.critical_section",
    [PROF_CLIENT_ACK_CRITICAL_SLOT_CHECK] = "client.ack.critical_slot_check",
    [PROF_CLIENT_ACK_CRITICAL_NO_SLOT] = "client.ack.critical_no_slot",
    [PROF_CLIENT_ACK_DATA_SPACE_WAIT] = "client.ack.data_space_wait",
    [PROF_CLIENT_ACK_DATA_SPACE_TOTAL] = "client.ack.data_space_total",
    [PROF_CLIENT_ACK_ACK_SPACE_WAIT] = "client.ack.ack_space_wait",
    [PROF_CLIENT_ACK_ACK_SPACE_TOTAL] = "client.ack.ack_space_total",
    [PROF_CLIENT_ACK_ALLOC_TOTAL] = "client.ack.alloc_total",
    [PROF_CLIENT_ACK_ALLOC_RESIDUAL] = "client.ack.alloc_residual",
    [PROF_CLIENT_ACK_REPLICA_RESET] = "client.ack.replica_reset",
    [PROF_CLIENT_ACK_PROMISE_ALLOC] = "client.ack.promise_alloc",
    [PROF_CLIENT_ACK_SLOT_PREP] = "client.ack.slot_prep",
    [PROF_CLIENT_ACK_COMMIT] = "client.ack.commit",
    [PROF_CLIENT_SEND_HEADER] = "client.send.header",
    [PROF_CLIENT_PUT_COPY] = "client.put.copy",
    [PROF_CLIENT_PUT_HASH] = "client.put.hash",
    [PROF_CLIENT_PUT_ACK_SLOT_ACQUIRE] = "client.put.ack_slot_acquire",
    [PROF_CLIENT_PUT_HASH_BUF_ALLOC] = "client.put.hash_buf_alloc",
    [PROF_CLIENT_PUT_SEND_HEADER] = "client.put.send_header",
    [PROF_CLIENT_PUT_TOTAL] = "client.put.total",
    [PROF_CLIENT_PUT_ACK_POLL] = "client.put.ack_poll",
    [PROF_CLIENT_PUT_ACK_POLL_SCAN] = "client.put.ack_poll_scan",
    [PROF_CLIENT_PUT_ACK_POLL_QUORUM_ERROR_EVAL] = "client.put.ack_poll_quorum_error_eval",
    [PROF_CLIENT_PUT_ACK_POLL_TIMEOUT_CHECK] = "client.put.ack_poll_timeout_check",
    [PROF_CLIENT_PUT_ACK_POLL_CONTROL_FLOW] = "client.put.ack_poll_control_flow",
    [PROF_CLIENT_PUT_ACK_POLL_RESULT] = "client.put.ack_poll_result",
    [PROF_CLIENT_PUT_ACK_POLL_RESIDUAL] = "client.put.ack_poll_residual",
    [PROF_CLIENT_GET2_COPY] = "client.get2.copy",
    [PROF_CLIENT_GET2_HASH] = "client.get2.hash",
    [PROF_CLIENT_GET2_ACK_SLOT_ACQUIRE] = "client.get2.ack_slot_acquire",
    [PROF_CLIENT_GET2_HASH_BUF_ALLOC] = "client.get2.hash_buf_alloc",
    [PROF_CLIENT_GET2_SEND_PHASE1] = "client.get2.send_phase1",
    [PROF_CLIENT_GET2_PHASE1_TOTAL] = "client.get2.phase1_total",
    [PROF_CLIENT_GET2_ACK_PHASE1_POLL] = "client.get2.ack_phase1_poll",
    [PROF_CLIENT_GET2_ACK_PHASE1_SCAN_ACKS] = "client.get2.ack_phase1.scan_acks",
    [PROF_CLIENT_GET2_ACK_PHASE1_BUILD_CANDIDATES] = "client.get2.ack_phase1.build_candidates",
    [PROF_CLIENT_GET2_ACK_PHASE1_FILTER_QUORUM] = "client.get2.ack_phase1.filter_quorum",
    [PROF_CLIENT_GET2_ACK_PHASE1_FASTPATH_VERIFY] = "client.get2.ack_phase1.fastpath_verify",
    [PROF_CLIENT_GET2_ACK_PHASE1_SHIP_PHASE2] = "client.get2.ack_phase1.ship_phase2",
    [PROF_CLIENT_GET2_ACK_PHASE1_TIMEOUT_CHECK] = "client.get2.ack_phase1.timeout_check",
    [PROF_CLIENT_GET2_ACK_PHASE1_DECISION] = "client.get2.ack_phase1.decision",
    [PROF_CLIENT_GET2_ACK_PHASE1_SELECT_CANDIDATE] = "client.get2.ack_phase1.select_candidate",
    [PROF_CLIENT_GET2_ACK_PHASE1_RESULT] = "client.get2.ack_phase1.result",
    [PROF_CLIENT_GET2_ACK_PHASE1_RESIDUAL] = "client.get2.ack_phase1.residual",
    [PROF_CLIENT_GET2_ACK_PHASE2_POLL] = "client.get2.ack_phase2_poll",
    [PROF_CLIENT_GET2_ACK_PHASE2_TIMEOUT_CHECK] = "client.get2.ack_phase2.timeout_check",
    [PROF_CLIENT_GET2_ACK_PHASE2_VERIFY_AND_COPY] = "client.get2.ack_phase2.verify_copy",
    [PROF_CLIENT_GET2_ACK_PHASE2_RESULT] = "client.get2.ack_phase2.result",
    [PROF_CLIENT_GET1_KEY_HASH] = "client.get1.key_hash",
    [PROF_CLIENT_GET1_INDEX_FETCH_DISPATCH] = "client.get1.index_dispatch",
    [PROF_CLIENT_GET1_INDEX_CALLBACK] = "client.get1.index_callback",
    [PROF_CLIENT_GET1_PREFERRED_FETCH_CB] = "client.get1.preferred_cb",
    [PROF_CLIENT_GET1_CONTINGENCY_PREP] = "client.get1.contingency_prep",
    [PROF_CLIENT_GET1_CONTINGENCY_CB] = "client.get1.contingency_cb",
    [PROF_CLIENT_GET1_TOTAL] = "client.get1.total",

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

static const perf_metric_id_t g_client_metrics[] = {
    PROF_CLIENT_ACK_GET_QUEUE_WAIT,
    PROF_CLIENT_ACK_HEADER_SLOT_WAIT,
    PROF_CLIENT_ACK_RETRY_PAUSE,
    PROF_CLIENT_ACK_MUTEX_LOCK_WAIT,
    PROF_CLIENT_ACK_MUTEX_UNLOCK_WAIT,
    PROF_CLIENT_ACK_CRITICAL_SECTION,
    PROF_CLIENT_ACK_CRITICAL_SLOT_CHECK,
    PROF_CLIENT_ACK_CRITICAL_NO_SLOT,
    PROF_CLIENT_ACK_DATA_SPACE_WAIT,
    PROF_CLIENT_ACK_DATA_SPACE_TOTAL,
    PROF_CLIENT_ACK_ACK_SPACE_WAIT,
    PROF_CLIENT_ACK_ACK_SPACE_TOTAL,
    PROF_CLIENT_ACK_ALLOC_TOTAL,
    PROF_CLIENT_ACK_ALLOC_RESIDUAL,
    PROF_CLIENT_ACK_REPLICA_RESET,
    PROF_CLIENT_ACK_PROMISE_ALLOC,
    PROF_CLIENT_ACK_SLOT_PREP,
    PROF_CLIENT_ACK_COMMIT,
    PROF_CLIENT_SEND_HEADER,
    PROF_CLIENT_PUT_COPY,
    PROF_CLIENT_PUT_HASH,
    PROF_CLIENT_PUT_ACK_SLOT_ACQUIRE,
    PROF_CLIENT_PUT_HASH_BUF_ALLOC,
    PROF_CLIENT_PUT_SEND_HEADER,
    PROF_CLIENT_PUT_TOTAL,
    PROF_CLIENT_PUT_ACK_POLL,
    PROF_CLIENT_PUT_ACK_POLL_SCAN,
    PROF_CLIENT_PUT_ACK_POLL_QUORUM_ERROR_EVAL,
    PROF_CLIENT_PUT_ACK_POLL_TIMEOUT_CHECK,
    PROF_CLIENT_PUT_ACK_POLL_CONTROL_FLOW,
    PROF_CLIENT_PUT_ACK_POLL_RESULT,
    PROF_CLIENT_PUT_ACK_POLL_RESIDUAL,
    PROF_CLIENT_GET2_COPY,
    PROF_CLIENT_GET2_HASH,
    PROF_CLIENT_GET2_ACK_SLOT_ACQUIRE,
    PROF_CLIENT_GET2_HASH_BUF_ALLOC,
    PROF_CLIENT_GET2_SEND_PHASE1,
    PROF_CLIENT_GET2_PHASE1_TOTAL,
    PROF_CLIENT_GET2_ACK_PHASE1_POLL,
    PROF_CLIENT_GET2_ACK_PHASE1_SCAN_ACKS,
    PROF_CLIENT_GET2_ACK_PHASE1_BUILD_CANDIDATES,
    PROF_CLIENT_GET2_ACK_PHASE1_FILTER_QUORUM,
    PROF_CLIENT_GET2_ACK_PHASE1_FASTPATH_VERIFY,
    PROF_CLIENT_GET2_ACK_PHASE1_SHIP_PHASE2,
    PROF_CLIENT_GET2_ACK_PHASE1_TIMEOUT_CHECK,
    PROF_CLIENT_GET2_ACK_PHASE1_DECISION,
    PROF_CLIENT_GET2_ACK_PHASE1_SELECT_CANDIDATE,
    PROF_CLIENT_GET2_ACK_PHASE1_RESULT,
    PROF_CLIENT_GET2_ACK_PHASE1_RESIDUAL,
    PROF_CLIENT_GET2_ACK_PHASE2_POLL,
    PROF_CLIENT_GET2_ACK_PHASE2_TIMEOUT_CHECK,
    PROF_CLIENT_GET2_ACK_PHASE2_VERIFY_AND_COPY,
    PROF_CLIENT_GET2_ACK_PHASE2_RESULT,
    PROF_CLIENT_GET1_KEY_HASH,
    PROF_CLIENT_GET1_INDEX_FETCH_DISPATCH,
    PROF_CLIENT_GET1_INDEX_CALLBACK,
    PROF_CLIENT_GET1_PREFERRED_FETCH_CB,
    PROF_CLIENT_GET1_CONTINGENCY_PREP,
    PROF_CLIENT_GET1_CONTINGENCY_CB,
    PROF_CLIENT_GET1_TOTAL,
};

static const perf_metric_id_t g_server_metrics[] = {
    PROF_SERVER_REQUESTS_PROCESSED,
    PROF_SERVER_POLL_SLOT_TOTAL,
    PROF_SERVER_POLL_KEY_COPY,
    PROF_SERVER_POLL_KEY_HASH,
    PROF_SERVER_PUT_TOTAL,
    PROF_SERVER_PUT_READ_COPY,
    PROF_SERVER_PUT_HASH_VERIFY,
    PROF_SERVER_PUT_INDEX_LOOKUP,
    PROF_SERVER_PUT_RETRY_INDEX_LOOKUP,
    PROF_SERVER_PUT_FIND_FREE_INDEX_SLOT,
    PROF_SERVER_PUT_ALLOC,
    PROF_SERVER_PUT_GC_ENQUEUE,
    PROF_SERVER_PUT_GC_GET_CLOCK,
    PROF_SERVER_PUT_GC_ENQUEUE_CALL,
    PROF_SERVER_PUT_INSERT,
    PROF_SERVER_PUT_MARK_HEADER_UNUSED,
    PROF_SERVER_PUT_ACK,
    PROF_SERVER_GET1_ACK_TOTAL,
    PROF_SERVER_GET1_COPY_BUCKET,
    PROF_SERVER_GET1_COPY_BUCKET_PLAIN,
    PROF_SERVER_GET1_COPY_BUCKET_WRITEBACK_SCAN,
    PROF_SERVER_GET1_COPY_BUCKET_WRITEBACK_COPY_TO_ACK,
    PROF_SERVER_GET1_COPY_BUCKET_WRITEBACK_MATCH_CHECK,
    PROF_SERVER_GET1_WRITEBACK_COPY,
    PROF_SERVER_GET1_ACK_FINALIZE,
    PROF_SERVER_GET2_ACK_TOTAL,
    PROF_SERVER_GET2_COPY,
};

#define ARRAY_LEN(a) (sizeof(a) / sizeof((a)[0]))

typedef struct profile_tree_node {
    perf_metric_id_t id;
    const char *label;
    const struct profile_tree_node *children;
    size_t child_count;
} profile_tree_node_t;

typedef struct {
    uint64_t count;
    uint64_t total_ns;
    uint64_t min_ns;
    uint64_t max_ns;
    uint64_t bytes;
} metric_snapshot_t;

static const profile_tree_node_t g_client_ack_data_space_children[] = {
    {PROF_CLIENT_ACK_DATA_SPACE_WAIT, "wait", NULL, 0},
};

static const profile_tree_node_t g_client_ack_ack_space_children[] = {
    {PROF_CLIENT_ACK_ACK_SPACE_WAIT, "wait", NULL, 0},
};

static const profile_tree_node_t g_client_ack_critical_children[] = {
    {PROF_CLIENT_ACK_CRITICAL_SLOT_CHECK, "slot_check", NULL, 0},
    {PROF_CLIENT_ACK_REPLICA_RESET, "replica_reset", NULL, 0},
    {PROF_CLIENT_ACK_PROMISE_ALLOC, "promise_alloc", NULL, 0},
    {PROF_CLIENT_ACK_SLOT_PREP, "slot_prep", NULL, 0},
    {PROF_CLIENT_ACK_DATA_SPACE_TOTAL, "data_space_total", g_client_ack_data_space_children, ARRAY_LEN(g_client_ack_data_space_children)},
    {PROF_CLIENT_ACK_ACK_SPACE_TOTAL, "ack_space_total", g_client_ack_ack_space_children, ARRAY_LEN(g_client_ack_ack_space_children)},
    {PROF_CLIENT_ACK_COMMIT, "commit", NULL, 0},
    {PROF_CLIENT_ACK_CRITICAL_NO_SLOT, "no_slot", NULL, 0},
};

static const profile_tree_node_t g_client_ack_alloc_children[] = {
    {PROF_CLIENT_ACK_GET_QUEUE_WAIT, "get_queue_wait", NULL, 0},
    {PROF_CLIENT_ACK_HEADER_SLOT_WAIT, "header_slot_wait", NULL, 0},
    {PROF_CLIENT_ACK_RETRY_PAUSE, "retry_pause", NULL, 0},
    {PROF_CLIENT_ACK_MUTEX_LOCK_WAIT, "mutex_lock_wait", NULL, 0},
    {PROF_CLIENT_ACK_MUTEX_UNLOCK_WAIT, "mutex_unlock_wait", NULL, 0},
    {PROF_CLIENT_ACK_CRITICAL_SECTION, "critical_section", g_client_ack_critical_children, ARRAY_LEN(g_client_ack_critical_children)},
    {PROF_CLIENT_ACK_ALLOC_RESIDUAL, "residual", NULL, 0},
};

static const profile_tree_node_t g_client_put_total_children[] = {
    {PROF_CLIENT_PUT_ACK_SLOT_ACQUIRE, "ack_slot_acquire", NULL, 0},
    {PROF_CLIENT_PUT_HASH_BUF_ALLOC, "hash_buf_alloc", NULL, 0},
    {PROF_CLIENT_PUT_COPY, "copy", NULL, 0},
    {PROF_CLIENT_PUT_HASH, "hash", NULL, 0},
    {PROF_CLIENT_PUT_SEND_HEADER, "send_header", NULL, 0},
};

static const profile_tree_node_t g_client_put_ack_poll_children[] = {
    {PROF_CLIENT_PUT_ACK_POLL_SCAN, "scan", NULL, 0},
    {PROF_CLIENT_PUT_ACK_POLL_QUORUM_ERROR_EVAL, "quorum_error_eval", NULL, 0},
    {PROF_CLIENT_PUT_ACK_POLL_TIMEOUT_CHECK, "timeout_check", NULL, 0},
    {PROF_CLIENT_PUT_ACK_POLL_CONTROL_FLOW, "control_flow", NULL, 0},
    {PROF_CLIENT_PUT_ACK_POLL_RESULT, "result", NULL, 0},
    {PROF_CLIENT_PUT_ACK_POLL_RESIDUAL, "residual", NULL, 0},
};

static const profile_tree_node_t g_client_get2_phase1_total_children[] = {
    {PROF_CLIENT_GET2_ACK_SLOT_ACQUIRE, "ack_slot_acquire", NULL, 0},
    {PROF_CLIENT_GET2_HASH_BUF_ALLOC, "hash_buf_alloc", NULL, 0},
    {PROF_CLIENT_GET2_COPY, "copy", NULL, 0},
    {PROF_CLIENT_GET2_HASH, "hash", NULL, 0},
    {PROF_CLIENT_GET2_SEND_PHASE1, "send_phase1", NULL, 0},
};

static const profile_tree_node_t g_client_get2_ack_phase1_children[] = {
    {PROF_CLIENT_GET2_ACK_PHASE1_TIMEOUT_CHECK, "timeout_check", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE1_DECISION, "decision", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE1_SCAN_ACKS, "scan_acks", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE1_BUILD_CANDIDATES, "build_candidates", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE1_FILTER_QUORUM, "filter_quorum", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE1_SELECT_CANDIDATE, "select_candidate", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE1_FASTPATH_VERIFY, "fastpath_verify", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE1_SHIP_PHASE2, "ship_phase2", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE1_RESULT, "result", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE1_RESIDUAL, "residual", NULL, 0},
};

static const profile_tree_node_t g_client_get2_ack_phase2_children[] = {
    {PROF_CLIENT_GET2_ACK_PHASE2_TIMEOUT_CHECK, "timeout_check", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE2_VERIFY_AND_COPY, "verify_and_copy", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE2_RESULT, "result", NULL, 0},
};

static const profile_tree_node_t g_client_get1_total_children[] = {
    {PROF_CLIENT_GET1_KEY_HASH, "key_hash", NULL, 0},
    {PROF_CLIENT_GET1_INDEX_FETCH_DISPATCH, "index_fetch_dispatch", NULL, 0},
    {PROF_CLIENT_GET1_INDEX_CALLBACK, "index_callback", NULL, 0},
    {PROF_CLIENT_GET1_PREFERRED_FETCH_CB, "preferred_fetch_cb", NULL, 0},
    {PROF_CLIENT_GET1_CONTINGENCY_PREP, "contingency_prep", NULL, 0},
    {PROF_CLIENT_GET1_CONTINGENCY_CB, "contingency_cb", NULL, 0},
};

static const profile_tree_node_t g_client_report_roots[] = {
    {PROF_CLIENT_ACK_ALLOC_TOTAL, "client.ack.alloc_total", g_client_ack_alloc_children, ARRAY_LEN(g_client_ack_alloc_children)},
    {PROF_CLIENT_PUT_TOTAL, "client.put.total", g_client_put_total_children, ARRAY_LEN(g_client_put_total_children)},
    {PROF_CLIENT_PUT_ACK_POLL, "client.put.ack_poll", g_client_put_ack_poll_children, ARRAY_LEN(g_client_put_ack_poll_children)},
    {PROF_CLIENT_GET2_PHASE1_TOTAL, "client.get2.phase1_total", g_client_get2_phase1_total_children, ARRAY_LEN(g_client_get2_phase1_total_children)},
    {PROF_CLIENT_GET2_ACK_PHASE1_POLL, "client.get2.ack_phase1_poll", g_client_get2_ack_phase1_children, ARRAY_LEN(g_client_get2_ack_phase1_children)},
    {PROF_CLIENT_GET2_ACK_PHASE2_POLL, "client.get2.ack_phase2_poll", g_client_get2_ack_phase2_children, ARRAY_LEN(g_client_get2_ack_phase2_children)},
    {PROF_CLIENT_GET1_TOTAL, "client.get1.total", g_client_get1_total_children, ARRAY_LEN(g_client_get1_total_children)},
    {PROF_CLIENT_SEND_HEADER, "client.send.header", NULL, 0},
};

static const profile_tree_node_t g_server_poll_slot_children[] = {
    {PROF_SERVER_POLL_KEY_COPY, "key_copy", NULL, 0},
    {PROF_SERVER_POLL_KEY_HASH, "key_hash", NULL, 0},
};

static const profile_tree_node_t g_server_put_gc_enqueue_children[] = {
    {PROF_SERVER_PUT_GC_GET_CLOCK, "get_clock", NULL, 0},
    {PROF_SERVER_PUT_GC_ENQUEUE_CALL, "enqueue_call", NULL, 0},
};

static const profile_tree_node_t g_server_put_children[] = {
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

static const profile_tree_node_t g_server_get1_writeback_scan_children[] = {
    {PROF_SERVER_GET1_COPY_BUCKET_WRITEBACK_COPY_TO_ACK, "copy_to_ack", NULL, 0},
    {PROF_SERVER_GET1_COPY_BUCKET_WRITEBACK_MATCH_CHECK, "match_check", NULL, 0},
    {PROF_SERVER_GET1_WRITEBACK_COPY, "writeback_copy", NULL, 0},
};

static const profile_tree_node_t g_server_get1_copy_bucket_branch_children[] = {
    {PROF_SERVER_GET1_COPY_BUCKET_PLAIN, "plain", NULL, 0},
    {PROF_SERVER_GET1_COPY_BUCKET_WRITEBACK_SCAN, "writeback_scan", g_server_get1_writeback_scan_children, ARRAY_LEN(g_server_get1_writeback_scan_children)},
};

static const profile_tree_node_t g_server_get1_ack_children[] = {
    {PROF_SERVER_GET1_COPY_BUCKET, "copy_bucket", g_server_get1_copy_bucket_branch_children, ARRAY_LEN(g_server_get1_copy_bucket_branch_children)},
    {PROF_SERVER_GET1_ACK_FINALIZE, "ack_finalize", NULL, 0},
};

static const profile_tree_node_t g_server_get2_ack_children[] = {
    {PROF_SERVER_GET2_COPY, "copy", NULL, 0},
};

static const profile_tree_node_t g_server_report_roots[] = {
    {PROF_SERVER_REQUESTS_PROCESSED, "server.requests_processed", NULL, 0},
    {PROF_SERVER_POLL_SLOT_TOTAL, "server.poll.slot_total", g_server_poll_slot_children, ARRAY_LEN(g_server_poll_slot_children)},
    {PROF_SERVER_PUT_TOTAL, "server.put.total", g_server_put_children, ARRAY_LEN(g_server_put_children)},
    {PROF_SERVER_GET1_ACK_TOTAL, "server.get1.ack_total", g_server_get1_ack_children, ARRAY_LEN(g_server_get1_ack_children)},
    {PROF_SERVER_GET2_ACK_TOTAL, "server.get2.ack_total", g_server_get2_ack_children, ARRAY_LEN(g_server_get2_ack_children)},
};

static void profile_reset_metric(perf_metric_id_t id) {
    atomic_store_explicit(&g_metrics[id].count, 0, memory_order_relaxed);
    atomic_store_explicit(&g_metrics[id].total_ns, 0, memory_order_relaxed);
    atomic_store_explicit(&g_metrics[id].min_ns, UINT64_MAX, memory_order_relaxed);
    atomic_store_explicit(&g_metrics[id].max_ns, 0, memory_order_relaxed);
    atomic_store_explicit(&g_metrics[id].bytes, 0, memory_order_relaxed);
}

static void profile_init(void) {
    for (size_t i = 0; i < PROF_METRIC_COUNT; i++) {
        profile_reset_metric((perf_metric_id_t) i);
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

static void profile_record_impl(perf_metric_id_t id, uint64_t duration_ns, uint64_t bytes) {
    if ((size_t) id >= PROF_METRIC_COUNT) {
        return;
    }

    profile_ensure_initialized();
    profile_metric_t *metric = &g_metrics[id];

    atomic_fetch_add_explicit(&metric->count, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&metric->total_ns, duration_ns, memory_order_relaxed);
    atomic_fetch_add_explicit(&metric->bytes, bytes, memory_order_relaxed);

    profile_record_min(&metric->min_ns, duration_ns);
    profile_record_max(&metric->max_ns, duration_ns);
}

uint64_t perf_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t) ts.tv_sec * 1000000000ULL + (uint64_t) ts.tv_nsec;
}

void perf_record_ns(perf_metric_id_t id, uint64_t duration_ns) {
    profile_record_impl(id, duration_ns, 0);
}

void perf_record_ns_bytes(perf_metric_id_t id, uint64_t duration_ns, uint64_t bytes) {
    profile_record_impl(id, duration_ns, bytes);
}

void perf_increment(perf_metric_id_t id, uint64_t amount) {
    if ((size_t) id >= PROF_METRIC_COUNT) {
        return;
    }

    profile_ensure_initialized();
    profile_metric_t *metric = &g_metrics[id];

    atomic_fetch_add_explicit(&metric->count, amount, memory_order_relaxed);
}

void perf_reset_all(void) {
    profile_ensure_initialized();
    for (size_t i = 0; i < PROF_METRIC_COUNT; i++) {
        profile_reset_metric((perf_metric_id_t) i);
    }
}

static metric_snapshot_t snapshot_metric(perf_metric_id_t id) {
    profile_metric_t *metric = &g_metrics[id];
    metric_snapshot_t snapshot;

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

static void print_metric_row(const char *label, int depth, const metric_snapshot_t *metric, const metric_snapshot_t *parent) {
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

static metric_snapshot_t print_tree_node(const profile_tree_node_t *node, const metric_snapshot_t *parent, int depth) {
    metric_snapshot_t metric = snapshot_metric(node->id);
    const char *label = node->label != NULL ? node->label : g_metric_names[node->id];
    print_metric_row(label, depth, &metric, parent);

    uint64_t child_total_ns = 0;
    for (size_t i = 0; i < node->child_count; i++) {
        metric_snapshot_t child = print_tree_node(&node->children[i], &metric, depth + 1);
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

static void print_layered_report(const char *title, const profile_tree_node_t *roots, size_t root_count) {
    profile_ensure_initialized();

    printf("\n=== %s ===\n", title);
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
        print_tree_node(&roots[i], NULL, 0);
        if (i + 1 < root_count) {
            printf("\n");
        }
    }
}

static void reset_metric_group(const perf_metric_id_t *ids, size_t count) {
    for (size_t i = 0; i < count; i++) {
        profile_reset_metric(ids[i]);
    }
}

void perf_print_client_report(void) {
    print_layered_report("Client Micro Profile (Layered)", g_client_report_roots, ARRAY_LEN(g_client_report_roots));
}

void perf_print_server_report(bool reset_after_print) {
    print_layered_report("Server Micro Profile (Layered)", g_server_report_roots, ARRAY_LEN(g_server_report_roots));
    if (reset_after_print) {
        reset_metric_group(g_server_metrics, sizeof(g_server_metrics) / sizeof(g_server_metrics[0]));
    }
}
