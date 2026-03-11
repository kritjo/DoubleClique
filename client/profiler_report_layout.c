#include "profiler_report_layout.h"

#include "profiler_metrics.h"

#define ARRAY_LEN(a) (sizeof(a) / sizeof((a)[0]))

static const char *const g_client_metric_names[CLIENT_PROF_METRIC_COUNT] = {
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
    [PROF_CLIENT_GET2_ACK_PHASE1_DECISION_TIMEOUT] = "client.get2.ack_phase1.decision_timeout",
    [PROF_CLIENT_GET2_ACK_PHASE1_MAIN_PATH] = "client.get2.ack_phase1.main_path",
    [PROF_CLIENT_GET2_ACK_PHASE1_PRE_SCAN] = "client.get2.ack_phase1.pre_scan",
    [PROF_CLIENT_GET2_ACK_PHASE1_MAIN_PATH_OVERHEAD] = "client.get2.ack_phase1.main_path_overhead",
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
    [PROF_CLIENT_GET1_MUTEX_LOCK_WAIT] = "client.get1.mutex_lock_wait",
    [PROF_CLIENT_GET1_PROMISE_SETUP] = "client.get1.promise_setup",
    [PROF_CLIENT_GET1_WAIT_FOR_RESULT] = "client.get1.wait_for_result",
    [PROF_CLIENT_GET1_TIMEOUT_CLEANUP] = "client.get1.timeout_cleanup",
    [PROF_CLIENT_GET1_COMPLETION_CLEANUP] = "client.get1.completion_cleanup",
    [PROF_CLIENT_GET1_MUTEX_UNLOCK] = "client.get1.mutex_unlock",
    [PROF_CLIENT_GET1_INDEX_CALLBACK] = "client.get1.index_callback",
    [PROF_CLIENT_GET1_PREFERRED_FETCH_CB] = "client.get1.preferred_cb",
    [PROF_CLIENT_GET1_CONTINGENCY_PREP] = "client.get1.contingency_prep",
    [PROF_CLIENT_GET1_CONTINGENCY_CB] = "client.get1.contingency_cb",
    [PROF_CLIENT_GET1_CALLBACKS_TOTAL] = "client.get1.callbacks_total",
    [PROF_CLIENT_GET1_RESIDUAL] = "client.get1.residual",
    [PROF_CLIENT_GET1_TOTAL] = "client.get1.total",
};

static const perf_report_node_t g_client_ack_data_space_children[] = {
    {PROF_CLIENT_ACK_DATA_SPACE_WAIT, "wait", NULL, 0},
};

static const perf_report_node_t g_client_ack_ack_space_children[] = {
    {PROF_CLIENT_ACK_ACK_SPACE_WAIT, "wait", NULL, 0},
};

static const perf_report_node_t g_client_ack_critical_children[] = {
    {PROF_CLIENT_ACK_CRITICAL_SLOT_CHECK, "slot_check", NULL, 0},
    {PROF_CLIENT_ACK_REPLICA_RESET, "replica_reset", NULL, 0},
    {PROF_CLIENT_ACK_PROMISE_ALLOC, "promise_alloc", NULL, 0},
    {PROF_CLIENT_ACK_SLOT_PREP, "slot_prep", NULL, 0},
    {PROF_CLIENT_ACK_DATA_SPACE_TOTAL, "data_space_total", g_client_ack_data_space_children, ARRAY_LEN(g_client_ack_data_space_children)},
    {PROF_CLIENT_ACK_ACK_SPACE_TOTAL, "ack_space_total", g_client_ack_ack_space_children, ARRAY_LEN(g_client_ack_ack_space_children)},
    {PROF_CLIENT_ACK_COMMIT, "commit", NULL, 0},
    {PROF_CLIENT_ACK_CRITICAL_NO_SLOT, "no_slot", NULL, 0},
};

static const perf_report_node_t g_client_ack_alloc_children[] = {
    {PROF_CLIENT_ACK_GET_QUEUE_WAIT, "get_queue_wait", NULL, 0},
    {PROF_CLIENT_ACK_HEADER_SLOT_WAIT, "header_slot_wait", NULL, 0},
    {PROF_CLIENT_ACK_RETRY_PAUSE, "retry_pause", NULL, 0},
    {PROF_CLIENT_ACK_MUTEX_LOCK_WAIT, "mutex_lock_wait", NULL, 0},
    {PROF_CLIENT_ACK_MUTEX_UNLOCK_WAIT, "mutex_unlock_wait", NULL, 0},
    {PROF_CLIENT_ACK_CRITICAL_SECTION, "critical_section", g_client_ack_critical_children, ARRAY_LEN(g_client_ack_critical_children)},
    {PROF_CLIENT_ACK_ALLOC_RESIDUAL, "residual", NULL, 0},
};

static const perf_report_node_t g_client_put_total_children[] = {
    {PROF_CLIENT_PUT_ACK_SLOT_ACQUIRE, "ack_slot_acquire", NULL, 0},
    {PROF_CLIENT_PUT_HASH_BUF_ALLOC, "hash_buf_alloc", NULL, 0},
    {PROF_CLIENT_PUT_COPY, "copy", NULL, 0},
    {PROF_CLIENT_PUT_HASH, "hash", NULL, 0},
    {PROF_CLIENT_PUT_SEND_HEADER, "send_header", NULL, 0},
};

static const perf_report_node_t g_client_put_ack_poll_children[] = {
    {PROF_CLIENT_PUT_ACK_POLL_SCAN, "scan", NULL, 0},
    {PROF_CLIENT_PUT_ACK_POLL_QUORUM_ERROR_EVAL, "quorum_error_eval", NULL, 0},
    {PROF_CLIENT_PUT_ACK_POLL_TIMEOUT_CHECK, "timeout_check", NULL, 0},
    {PROF_CLIENT_PUT_ACK_POLL_CONTROL_FLOW, "control_flow", NULL, 0},
    {PROF_CLIENT_PUT_ACK_POLL_RESULT, "result", NULL, 0},
    {PROF_CLIENT_PUT_ACK_POLL_RESIDUAL, "residual", NULL, 0},
};

static const perf_report_node_t g_client_get2_phase1_total_children[] = {
    {PROF_CLIENT_GET2_ACK_SLOT_ACQUIRE, "ack_slot_acquire", NULL, 0},
    {PROF_CLIENT_GET2_HASH_BUF_ALLOC, "hash_buf_alloc", NULL, 0},
    {PROF_CLIENT_GET2_COPY, "copy", NULL, 0},
    {PROF_CLIENT_GET2_HASH, "hash", NULL, 0},
    {PROF_CLIENT_GET2_SEND_PHASE1, "send_phase1", NULL, 0},
};

static const perf_report_node_t g_client_get2_ack_phase1_main_path_children[] = {
    {PROF_CLIENT_GET2_ACK_PHASE1_PRE_SCAN, "pre_scan", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE1_SCAN_ACKS, "scan_acks", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE1_DECISION, "decision", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE1_BUILD_CANDIDATES, "build_candidates", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE1_FILTER_QUORUM, "filter_quorum", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE1_SELECT_CANDIDATE, "select_candidate", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE1_FASTPATH_VERIFY, "fastpath_verify", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE1_SHIP_PHASE2, "ship_phase2", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE1_MAIN_PATH_OVERHEAD, "overhead", NULL, 0},
};

static const perf_report_node_t g_client_get2_ack_phase1_children[] = {
    {PROF_CLIENT_GET2_ACK_PHASE1_TIMEOUT_CHECK, "timeout_check", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE1_DECISION_TIMEOUT, "decision_timeout", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE1_MAIN_PATH, "main_path", g_client_get2_ack_phase1_main_path_children, ARRAY_LEN(g_client_get2_ack_phase1_main_path_children)},
    {PROF_CLIENT_GET2_ACK_PHASE1_RESULT, "result", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE1_RESIDUAL, "residual", NULL, 0},
};

static const perf_report_node_t g_client_get2_ack_phase2_children[] = {
    {PROF_CLIENT_GET2_ACK_PHASE2_TIMEOUT_CHECK, "timeout_check", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE2_VERIFY_AND_COPY, "verify_and_copy", NULL, 0},
    {PROF_CLIENT_GET2_ACK_PHASE2_RESULT, "result", NULL, 0},
};

static const perf_report_node_t g_client_get1_sync_children[] = {
    {PROF_CLIENT_GET1_KEY_HASH, "key_hash", NULL, 0},
    {PROF_CLIENT_GET1_INDEX_FETCH_DISPATCH, "index_fetch_dispatch", NULL, 0},
    {PROF_CLIENT_GET1_MUTEX_LOCK_WAIT, "mutex_lock_wait", NULL, 0},
    {PROF_CLIENT_GET1_PROMISE_SETUP, "promise_setup", NULL, 0},
    {PROF_CLIENT_GET1_WAIT_FOR_RESULT, "wait_for_result", NULL, 0},
    {PROF_CLIENT_GET1_TIMEOUT_CLEANUP, "timeout_cleanup", NULL, 0},
    {PROF_CLIENT_GET1_COMPLETION_CLEANUP, "completion_cleanup", NULL, 0},
    {PROF_CLIENT_GET1_MUTEX_UNLOCK, "mutex_unlock", NULL, 0},
    {PROF_CLIENT_GET1_RESIDUAL, "residual", NULL, 0},
};

static const perf_report_node_t g_client_get1_callbacks_children[] = {
    {PROF_CLIENT_GET1_INDEX_CALLBACK, "index_callback", NULL, 0},
    {PROF_CLIENT_GET1_PREFERRED_FETCH_CB, "preferred_fetch_cb", NULL, 0},
    {PROF_CLIENT_GET1_CONTINGENCY_PREP, "contingency_prep", NULL, 0},
    {PROF_CLIENT_GET1_CONTINGENCY_CB, "contingency_cb", NULL, 0},
};

static const perf_report_node_t g_client_report_roots[] = {
    {PROF_CLIENT_ACK_ALLOC_TOTAL, "client.ack.alloc_total", g_client_ack_alloc_children, ARRAY_LEN(g_client_ack_alloc_children)},
    {PROF_CLIENT_PUT_TOTAL, "client.put.total", g_client_put_total_children, ARRAY_LEN(g_client_put_total_children)},
    {PROF_CLIENT_PUT_ACK_POLL, "client.put.ack_poll", g_client_put_ack_poll_children, ARRAY_LEN(g_client_put_ack_poll_children)},
    {PROF_CLIENT_GET2_PHASE1_TOTAL, "client.get2.phase1_total", g_client_get2_phase1_total_children, ARRAY_LEN(g_client_get2_phase1_total_children)},
    {PROF_CLIENT_GET2_ACK_PHASE1_POLL, "client.get2.ack_phase1_poll", g_client_get2_ack_phase1_children, ARRAY_LEN(g_client_get2_ack_phase1_children)},
    {PROF_CLIENT_GET2_ACK_PHASE2_POLL, "client.get2.ack_phase2_poll", g_client_get2_ack_phase2_children, ARRAY_LEN(g_client_get2_ack_phase2_children)},
    {PROF_CLIENT_GET1_TOTAL, "client.get1.total", g_client_get1_sync_children, ARRAY_LEN(g_client_get1_sync_children)},
    {PROF_CLIENT_GET1_CALLBACKS_TOTAL, "client.get1.callbacks_total", g_client_get1_callbacks_children, ARRAY_LEN(g_client_get1_callbacks_children)},
    {PROF_CLIENT_SEND_HEADER, "client.send.header", NULL, 0},
};

const perf_report_node_t *client_profiler_report_roots(size_t *count) {
    if (count != NULL) {
        *count = ARRAY_LEN(g_client_report_roots);
    }
    return g_client_report_roots;
}

const char *const *client_profiler_metric_names(size_t *count) {
    if (count != NULL) {
        *count = ARRAY_LEN(g_client_metric_names);
    }
    return g_client_metric_names;
}

const char *client_profiler_report_title(void) {
    return "Client Micro Profile (Layered)";
}
