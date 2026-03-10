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
    [PROF_CLIENT_ACK_DATA_SPACE_WAIT] = "client.ack.data_space_wait",
    [PROF_CLIENT_ACK_ACK_SPACE_WAIT] = "client.ack.ack_space_wait",
    [PROF_CLIENT_ACK_ALLOC_TOTAL] = "client.ack.alloc_total",
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
    [PROF_CLIENT_PUT_ACK_POLL_TIMEOUT_CHECK] = "client.put.ack_poll_timeout_check",
    [PROF_CLIENT_PUT_ACK_POLL_RESULT] = "client.put.ack_poll_result",
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
    [PROF_CLIENT_GET2_ACK_PHASE1_RESULT] = "client.get2.ack_phase1.result",
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
    [PROF_SERVER_PUT_INSERT] = "server.put.insert",
    [PROF_SERVER_PUT_MARK_HEADER_UNUSED] = "server.put.mark_header_unused",
    [PROF_SERVER_PUT_ACK] = "server.put.ack",
    [PROF_SERVER_GET1_ACK_TOTAL] = "server.get1.ack_total",
    [PROF_SERVER_GET1_COPY_BUCKET] = "server.get1.copy_bucket",
    [PROF_SERVER_GET1_COPY_BUCKET_PLAIN] = "server.get1.copy_bucket_plain",
    [PROF_SERVER_GET1_COPY_BUCKET_WRITEBACK_SCAN] = "server.get1.copy_bucket_writeback_scan",
    [PROF_SERVER_GET1_WRITEBACK_COPY] = "server.get1.writeback_copy",
    [PROF_SERVER_GET1_ACK_FINALIZE] = "server.get1.ack_finalize",
    [PROF_SERVER_GET2_ACK_TOTAL] = "server.get2.ack_total",
    [PROF_SERVER_GET2_COPY] = "server.get2.copy",
};

static const perf_metric_id_t g_client_metrics[] = {
    PROF_CLIENT_ACK_GET_QUEUE_WAIT,
    PROF_CLIENT_ACK_HEADER_SLOT_WAIT,
    PROF_CLIENT_ACK_DATA_SPACE_WAIT,
    PROF_CLIENT_ACK_ACK_SPACE_WAIT,
    PROF_CLIENT_ACK_ALLOC_TOTAL,
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
    PROF_CLIENT_PUT_ACK_POLL_TIMEOUT_CHECK,
    PROF_CLIENT_PUT_ACK_POLL_RESULT,
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
    PROF_CLIENT_GET2_ACK_PHASE1_RESULT,
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
    PROF_SERVER_PUT_INSERT,
    PROF_SERVER_PUT_MARK_HEADER_UNUSED,
    PROF_SERVER_PUT_ACK,
    PROF_SERVER_GET1_ACK_TOTAL,
    PROF_SERVER_GET1_COPY_BUCKET,
    PROF_SERVER_GET1_COPY_BUCKET_PLAIN,
    PROF_SERVER_GET1_COPY_BUCKET_WRITEBACK_SCAN,
    PROF_SERVER_GET1_WRITEBACK_COPY,
    PROF_SERVER_GET1_ACK_FINALIZE,
    PROF_SERVER_GET2_ACK_TOTAL,
    PROF_SERVER_GET2_COPY,
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

static void print_metric_table(const char *title, const perf_metric_id_t *ids, size_t count) {
    profile_ensure_initialized();

    printf("\n=== %s ===\n", title);
    printf("%-32s %10s %12s %12s %12s %12s %12s\n",
           "metric",
           "count",
           "total ms",
           "avg us",
           "min us",
           "max us",
           "MB/s");

    size_t rows_printed = 0;
    for (size_t i = 0; i < count; i++) {
        perf_metric_id_t id = ids[i];
        profile_metric_t *metric = &g_metrics[id];
        uint64_t sample_count = atomic_load_explicit(&metric->count, memory_order_relaxed);
        if (sample_count == 0) {
            continue;
        }

        rows_printed++;
        uint64_t total_ns = atomic_load_explicit(&metric->total_ns, memory_order_relaxed);
        uint64_t min_ns = atomic_load_explicit(&metric->min_ns, memory_order_relaxed);
        uint64_t max_ns = atomic_load_explicit(&metric->max_ns, memory_order_relaxed);
        uint64_t bytes = atomic_load_explicit(&metric->bytes, memory_order_relaxed);

        if (min_ns == UINT64_MAX) {
            min_ns = 0;
        }

        double total_ms = (double) total_ns / 1e6;
        double avg_us = sample_count > 0 ? (double) total_ns / (double) sample_count / 1e3 : 0.0;
        double min_us = (double) min_ns / 1e3;
        double max_us = (double) max_ns / 1e3;
        double mb_per_sec = total_ns > 0 ? ((double) bytes * 1e3) / (double) total_ns : 0.0;

        printf("%-32s %10" PRIu64 " %12.3f %12.3f %12.3f %12.3f %12.3f\n",
               g_metric_names[id],
               sample_count,
               total_ms,
               avg_us,
               min_us,
               max_us,
               mb_per_sec);
    }

    if (rows_printed == 0) {
        printf("(no samples)\n");
    }
}

static void reset_metric_group(const perf_metric_id_t *ids, size_t count) {
    for (size_t i = 0; i < count; i++) {
        profile_reset_metric(ids[i]);
    }
}

void perf_print_client_report(void) {
    print_metric_table("Client Micro Profile", g_client_metrics, sizeof(g_client_metrics) / sizeof(g_client_metrics[0]));
}

void perf_print_server_report(bool reset_after_print) {
    print_metric_table("Server Micro Profile", g_server_metrics, sizeof(g_server_metrics) / sizeof(g_server_metrics[0]));
    if (reset_after_print) {
        reset_metric_group(g_server_metrics, sizeof(g_server_metrics) / sizeof(g_server_metrics[0]));
    }
}
