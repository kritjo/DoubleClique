#include <sisci_api.h>
#include <stdlib.h>
#include <sched.h>
#include <string.h>
#include "put.h"
#include "sisci_glob_defs.h"
#include "request_region.h"
#include "get_node_id.h"
#include "super_fast_hash.h"
#include "index_data_protocol.h"
#include "request_region_connection.h"
#include "ack_region.h"
#include "sequence.h"
#include "profiler.h"
#include "profiler_metrics.h"
#include "avx_cpy.h"

// wraparound version_number, large enough to avoid replay attacks
static volatile _Atomic uint32_t version_number = 0;

static uint8_t client_id;

static inline void copy_to_request_region(volatile char *dst, const char *src, size_t bytes) {
    if (bytes >= 64) {
        memcpy_nt_avx2(dst, src, bytes, CHUNK_SIZE);
        return;
    }

    for (size_t i = 0; i < bytes; i++) {
        dst[i] = src[i];
    }
}

void init_put(void) {
    unsigned int node_id = get_node_id();
    if (node_id > UINT8_MAX) {
        fprintf(stderr, "node_id too large!\n");
        exit(EXIT_FAILURE);
    }
    client_id = (uint8_t) node_id;
}

request_promise_t *put_blocking_until_available_put_request_region_slot(const char *key, uint8_t key_len, void *value, uint32_t value_len) {
    uint64_t put_total_start_ns = perf_now_ns();
    if (key_len + value_len > REQUEST_REGION_DATA_SIZE) {
        fprintf(stderr, "illegally large data\n");
        exit(EXIT_FAILURE);
    }

    uint32_t my_version_number = ((uint32_t) client_id) << 24 | version_number;
    version_number = (version_number + 1) % MAX_VERSION_NUMBER;

    uint64_t ack_slot_start_ns = perf_now_ns();
    ack_slot_t *ack_slot = get_ack_slot_blocking(PUT, key_len, value_len, key_len+value_len, 0, my_version_number, NULL);
    perf_record_ns(PROF_CLIENT_PUT_ACK_SLOT_ACQUIRE, perf_now_ns() - ack_slot_start_ns);

    uint32_t starting_offset = ack_slot->starting_data_offset;
    volatile char *data_region_start = ((volatile char *) request_region) + sizeof(request_region_t);
    volatile char *request_slot_start = data_region_start + starting_offset;

    uint64_t hash_buf_alloc_start_ns = perf_now_ns();
    char *hash_data = malloc(key_len + value_len + sizeof(((header_slot_t *) 0)->version_number));
    if (hash_data == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    perf_record_ns(PROF_CLIENT_PUT_HASH_BUF_ALLOC, perf_now_ns() - hash_buf_alloc_start_ns);

    uint64_t copy_start_ns = perf_now_ns();

    memcpy(hash_data, key, key_len);
    memcpy(hash_data + key_len, value, value_len);
    copy_to_request_region(request_slot_start, hash_data, (size_t) key_len + value_len);

    // Copy version number into the first 4 bytes of hash_data
    memcpy(hash_data + key_len + value_len, &ack_slot->version_number, sizeof(((header_slot_t *) 0)->version_number));
    perf_record_ns_bytes(
        PROF_CLIENT_PUT_COPY,
        perf_now_ns() - copy_start_ns,
        (uint64_t) key_len + value_len
    );

    uint64_t hash_start_ns = perf_now_ns();
    uint32_t payload_hash = super_fast_hash(hash_data,
                                            (uint32_t) (key_len + value_len + sizeof(((header_slot_t *) 0)->version_number)));
    perf_record_ns_bytes(
        PROF_CLIENT_PUT_HASH,
        perf_now_ns() - hash_start_ns,
        (uint64_t) key_len + value_len + sizeof(((header_slot_t *) 0)->version_number)
    );
    free(hash_data);

    uint64_t send_start_ns = perf_now_ns();
    send_request_region_slot(
        ack_slot->header_slot_WRITE_ONLY,
        key_len,
        value_len,
        ack_slot->version_number,
        (size_t) starting_offset,
        0,
        0,
        payload_hash,
        HEADER_SLOT_USED_PUT
    );
    perf_record_ns(PROF_CLIENT_PUT_SEND_HEADER, perf_now_ns() - send_start_ns);

    perf_record_ns(PROF_CLIENT_PUT_TOTAL, perf_now_ns() - put_total_start_ns);
    return ack_slot->promise;
}

bool consume_put_ack_slot(ack_slot_t *ack_slot) {
    uint64_t poll_start_ns = perf_now_ns();
    uint64_t scan_ns = 0;
    uint64_t quorum_error_eval_ns = 0;
    uint64_t timeout_check_ns = 0;
    uint64_t control_flow_ns = 0;
    uint64_t result_ns = 0;

#define PUT_POLL_RETURN(result_value) \
    do { \
        uint64_t total_ns = perf_now_ns() - poll_start_ns; \
        uint64_t accounted_ns = scan_ns + quorum_error_eval_ns + timeout_check_ns + control_flow_ns + result_ns; \
        uint64_t residual_ns = total_ns > accounted_ns ? total_ns - accounted_ns : 0; \
        perf_record_ns(PROF_CLIENT_PUT_ACK_POLL_SCAN, scan_ns); \
        perf_record_ns(PROF_CLIENT_PUT_ACK_POLL_QUORUM_ERROR_EVAL, quorum_error_eval_ns); \
        perf_record_ns(PROF_CLIENT_PUT_ACK_POLL_TIMEOUT_CHECK, timeout_check_ns); \
        perf_record_ns(PROF_CLIENT_PUT_ACK_POLL_CONTROL_FLOW, control_flow_ns); \
        perf_record_ns(PROF_CLIENT_PUT_ACK_POLL_RESULT, result_ns); \
        perf_record_ns(PROF_CLIENT_PUT_ACK_POLL_RESIDUAL, residual_ns); \
        perf_record_ns(PROF_CLIENT_PUT_ACK_POLL, total_ns); \
        return (result_value); \
    } while (0)

    uint32_t ack_success_count = 0;
    uint32_t ack_count = 0;

    uint64_t scan_start_ns = perf_now_ns();
    for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        replica_ack_t *replica_ack_instance = ack_slot->replica_ack_instances[replica_index];
        enum replica_ack_type ack_type = replica_ack_instance->replica_ack_type;
        uint32_t actual = replica_ack_instance->version_number;
        uint32_t expected = ack_slot->version_number;

        // If wrong version number, we do not count as ack
        if (expected != actual) continue;

        if (ack_type != REPLICA_NOT_ACKED)
            ack_count++;

        if (ack_type == REPLICA_ACK_SUCCESS)
            ack_success_count++;
    }
    scan_ns += perf_now_ns() - scan_start_ns;

    // If we got a quorum of success acks, count as success
    uint64_t control_start_ns = perf_now_ns();
    bool quorum_reached = ack_success_count >= (REPLICA_COUNT + 1) / 2;
    control_flow_ns += perf_now_ns() - control_start_ns;
    if (quorum_reached) {
        uint64_t result_start_ns = perf_now_ns();
        // Success!
        ack_slot->promise->result = PROMISE_SUCCESS;
        insert_duration_end_now(ack_slot->promise, ack_slot->start_time);
        result_ns += perf_now_ns() - result_start_ns;
        PUT_POLL_RETURN(true);
    }

    control_start_ns = perf_now_ns();
    bool all_replied = ack_count == REPLICA_COUNT;
    control_flow_ns += perf_now_ns() - control_start_ns;
    if (all_replied) {
        uint64_t eval_start_ns = perf_now_ns();
        // Check what errors we have gotten
        enum replica_ack_type replica_ack_type;
        bool mix = false;
        for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
            replica_ack_t *replica_ack_instance = ack_slot->replica_ack_instances[replica_index];

            if (replica_index == 0) {
                replica_ack_type = replica_ack_instance->replica_ack_type;
                continue;
            }
            if (replica_ack_instance->replica_ack_type != replica_ack_type) {
                mix = true;
            }
        }
        quorum_error_eval_ns += perf_now_ns() - eval_start_ns;

        control_start_ns = perf_now_ns();
        if (mix) {
            control_flow_ns += perf_now_ns() - control_start_ns;
            uint64_t result_start_ns = perf_now_ns();
            ack_slot->promise->result = PROMISE_ERROR_MIX;
            insert_duration_end_now(ack_slot->promise, ack_slot->start_time);
            result_ns += perf_now_ns() - result_start_ns;
            PUT_POLL_RETURN(true);
        }
        control_flow_ns += perf_now_ns() - control_start_ns;

        control_start_ns = perf_now_ns();
        switch (replica_ack_type) {
            case REPLICA_ACK_ERROR_OUT_OF_SPACE:
                {
                control_flow_ns += perf_now_ns() - control_start_ns;
                uint64_t result_start_ns = perf_now_ns();
                ack_slot->promise->result = PROMISE_ERROR_OUT_OF_SPACE;
                result_ns += perf_now_ns() - result_start_ns;
                PUT_POLL_RETURN(true);
                }
            case REPLICA_ACK_SUCCESS:
            case REPLICA_NOT_ACKED:
            default:
                fprintf(stderr, "Illegal REPLICA_ACK type!\n");
                exit(EXIT_FAILURE);
        }
    }

    // If we do not have error replies and not a quorum, check for timeout
    uint64_t timeout_check_start_ns = perf_now_ns();
    struct timespec end_p;
    clock_gettime(CLOCK_MONOTONIC, &end_p);
    bool timed_out =
        ((end_p.tv_sec - ack_slot->start_time.tv_sec) * 1000000000L + (end_p.tv_nsec - ack_slot->start_time.tv_nsec)) >= PUT_TIMEOUT_NS;
    timeout_check_ns += perf_now_ns() - timeout_check_start_ns;

    control_start_ns = perf_now_ns();
    if (timed_out) {
        control_flow_ns += perf_now_ns() - control_start_ns;
        uint64_t result_start_ns = perf_now_ns();
        ack_slot->promise->result = PROMISE_TIMEOUT;
        insert_duration_end_now(ack_slot->promise, ack_slot->start_time);
        result_ns += perf_now_ns() - result_start_ns;
        PUT_POLL_RETURN(true);
    }
    control_flow_ns += perf_now_ns() - control_start_ns;

    PUT_POLL_RETURN(false);

#undef PUT_POLL_RETURN
}
