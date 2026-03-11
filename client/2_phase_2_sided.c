#include <malloc.h>
#include <stdlib.h>
#include <string.h>
#include <sisci_api.h>
#include "2_phase_2_sided.h"
#include "ack_region.h"
#include "request_region_connection.h"
#include "super_fast_hash.h"
#include "2_phase_1_sided.h"
#include "phase_2_queue.h"
#include "sequence.h"
#include "profiler.h"
#include "profiler_metrics.h"

static void *phase2_thread(__attribute__((unused)) void *_args);

static inline void copy_to_request_region(volatile char *dst, const char *src, size_t bytes) {
    for (size_t i = 0; i < bytes; i++) {
        dst[i] = src[i];
    }
}

void init_2_phase_2_sided_get(void) {
    queue_init();
    pthread_t id;
    pthread_create(&id, NULL, phase2_thread, NULL);
}

request_promise_t *get_2_phase_2_sided(const char *key, uint8_t key_len) {
    uint64_t total_start_ns = perf_now_ns();
    // First we need to get a header slot
    // Then we need to broadcast the request
    // Then wait until we have a quorum
    // Then fetch data from preferred backend

    uint64_t ack_slot_start_ns = perf_now_ns();
    ack_slot_t *ack_slot = get_ack_slot_blocking(GET_PHASE1, key_len, 0, key_len, SPECULATIVE_SIZE, 0, NULL);
    perf_record_ns(PROF_CLIENT_GET2_ACK_SLOT_ACQUIRE, perf_now_ns() - ack_slot_start_ns);

    uint32_t starting_offset = ack_slot->starting_data_offset;
    volatile char *data_region_start = ((volatile char *) request_region) + sizeof(request_region_t);
    volatile char *request_slot_start = data_region_start + starting_offset;

    uint64_t copy_start_ns = perf_now_ns();
    copy_to_request_region(request_slot_start, key, key_len);
    perf_record_ns_bytes(PROF_CLIENT_GET2_COPY, perf_now_ns() - copy_start_ns, key_len);

    uint64_t hash_start_ns = perf_now_ns();
    uint32_t key_hash = super_fast_hash(key, (int) (key_len));
    perf_record_ns_bytes(PROF_CLIENT_GET2_HASH, perf_now_ns() - hash_start_ns, key_len);

    ack_slot->key_hash = key_hash;

    uint64_t send_start_ns = perf_now_ns();
    send_request_region_slot(ack_slot->header_slot_WRITE_ONLY,
        key_len,
        0,
        ack_slot->version_number,
        (size_t) starting_offset,
        ack_slot->starting_ack_data_offset,
        WRITE_BACK_REPLICA, // TODO: use 'best' replica
        key_hash,
        HEADER_SLOT_USED_GET_PHASE1
    );
    perf_record_ns(PROF_CLIENT_GET2_SEND_PHASE1, perf_now_ns() - send_start_ns);

    perf_record_ns(PROF_CLIENT_GET2_PHASE1_TOTAL, perf_now_ns() - total_start_ns);

    return ack_slot->promise;
}

bool consume_get_ack_slot_phase1(ack_slot_t *ack_slot) {
    uint64_t poll_start_ns = perf_now_ns();
    uint64_t timeout_check_ns = 0;
    uint64_t decision_timeout_ns = 0;
    uint64_t decision_ns = 0;
    uint64_t pre_scan_ns = 0;
    uint64_t scan_acks_ns = 0;
    uint64_t build_candidates_ns = 0;
    uint64_t filter_quorum_ns = 0;
    uint64_t select_candidate_ns = 0;
    uint64_t fastpath_verify_ns = 0;
    uint64_t ship_phase2_ns = 0;
    uint64_t main_path_start_ns = 0;
    bool main_path_started = false;

#define GET2_PHASE1_RETURN(result_value, result_start_ns) \
    do { \
        uint64_t _result_ns = perf_now_ns() - (result_start_ns); \
        uint64_t _total_ns = perf_now_ns() - poll_start_ns; \
        uint64_t _main_path_ns = (main_path_started && (result_start_ns) > main_path_start_ns) ? (result_start_ns) - main_path_start_ns : 0; \
        uint64_t _main_path_accounted_ns = pre_scan_ns + decision_ns + scan_acks_ns + build_candidates_ns + filter_quorum_ns + select_candidate_ns + fastpath_verify_ns + ship_phase2_ns; \
        uint64_t _main_path_overhead_ns = _main_path_ns > _main_path_accounted_ns ? _main_path_ns - _main_path_accounted_ns : 0; \
        uint64_t _accounted_ns = timeout_check_ns + decision_timeout_ns + _main_path_ns + _result_ns; \
        perf_record_ns(PROF_CLIENT_GET2_ACK_PHASE1_TIMEOUT_CHECK, timeout_check_ns); \
        perf_record_ns(PROF_CLIENT_GET2_ACK_PHASE1_DECISION_TIMEOUT, decision_timeout_ns); \
        perf_record_ns(PROF_CLIENT_GET2_ACK_PHASE1_MAIN_PATH, _main_path_ns); \
        perf_record_ns(PROF_CLIENT_GET2_ACK_PHASE1_PRE_SCAN, pre_scan_ns); \
        perf_record_ns(PROF_CLIENT_GET2_ACK_PHASE1_DECISION, decision_ns); \
        perf_record_ns(PROF_CLIENT_GET2_ACK_PHASE1_SCAN_ACKS, scan_acks_ns); \
        perf_record_ns(PROF_CLIENT_GET2_ACK_PHASE1_BUILD_CANDIDATES, build_candidates_ns); \
        perf_record_ns(PROF_CLIENT_GET2_ACK_PHASE1_FILTER_QUORUM, filter_quorum_ns); \
        perf_record_ns(PROF_CLIENT_GET2_ACK_PHASE1_SELECT_CANDIDATE, select_candidate_ns); \
        perf_record_ns(PROF_CLIENT_GET2_ACK_PHASE1_FASTPATH_VERIFY, fastpath_verify_ns); \
        perf_record_ns(PROF_CLIENT_GET2_ACK_PHASE1_SHIP_PHASE2, ship_phase2_ns); \
        perf_record_ns(PROF_CLIENT_GET2_ACK_PHASE1_MAIN_PATH_OVERHEAD, _main_path_overhead_ns); \
        perf_record_ns(PROF_CLIENT_GET2_ACK_PHASE1_RESULT, _result_ns); \
        perf_record_ns(PROF_CLIENT_GET2_ACK_PHASE1_RESIDUAL, _total_ns > _accounted_ns ? _total_ns - _accounted_ns : 0); \
        perf_record_ns(PROF_CLIENT_GET2_ACK_PHASE1_POLL, _total_ns); \
        return (result_value); \
    } while (0)

    // First thing we do is to check for a timeout
    uint64_t timeout_check_start_ns = perf_now_ns();
    struct timespec end_p;
    clock_gettime(CLOCK_MONOTONIC, &end_p);
    bool timed_out =
        ((end_p.tv_sec - ack_slot->start_time.tv_sec) * 1000000000L + (end_p.tv_nsec - ack_slot->start_time.tv_nsec)) >= GET_TIMEOUT_2_SIDED_NS;
    timeout_check_ns += perf_now_ns() - timeout_check_start_ns;

    uint64_t decision_start_ns = perf_now_ns();
    if (timed_out) {
        decision_timeout_ns += perf_now_ns() - decision_start_ns;
        uint64_t result_start_ns = perf_now_ns();
        ack_slot->promise->result = PROMISE_TIMEOUT;
        insert_duration_end_now(ack_slot->promise, ack_slot->start_time);
        get_2_sided_decrement();
        GET2_PHASE1_RETURN(true, result_start_ns);
    }
    decision_timeout_ns += perf_now_ns() - decision_start_ns;
    main_path_start_ns = perf_now_ns();
    main_path_started = true;

    uint64_t pre_scan_start_ns = perf_now_ns();
    uint32_t ack_success_count = 0;
    uint32_t ack_count = 0;
    pre_scan_ns += perf_now_ns() - pre_scan_start_ns;

    uint64_t scan_acks_start_ns = perf_now_ns();
    for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        replica_ack_t *replica_ack_instance = ack_slot->replica_ack_instances[replica_index];
        enum replica_ack_type ack_type = replica_ack_instance->replica_ack_type;

        if (ack_type != REPLICA_NOT_ACKED)
            ack_count++;

        if (ack_type == REPLICA_ACK_SUCCESS)
            ack_success_count++;
    }
    scan_acks_ns += perf_now_ns() - scan_acks_start_ns;

    decision_start_ns = perf_now_ns();
    bool quorum_reached = ack_success_count >= (REPLICA_COUNT + 1) / 2;
    decision_ns += perf_now_ns() - decision_start_ns;
    if (quorum_reached) {
        // Now we must check if we have a quorum, if we do dispatch final request and return true
        uint64_t build_candidates_start_ns = perf_now_ns();
        version_count_t candidates[REPLICA_COUNT * INDEX_SLOTS_PR_BUCKET];
        uint32_t candidate_count = 0;

        for (uint32_t i = 0; i < REPLICA_COUNT * INDEX_SLOTS_PR_BUCKET; i++) {
            candidates[i].count = 0;
            candidates[i].version_number = 0;
            for (uint32_t j = 0; j < REPLICA_COUNT; j++) {
                candidates[i].index_entry[j].status = 0;
            }
        }

        for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
            replica_ack_t *replica_ack_instance = ack_slot->replica_ack_instances[replica_index];

            // For every replica, we need to check all of their returned index entries
            for (int slot_index = 0; slot_index < INDEX_SLOTS_PR_BUCKET; slot_index++) {
                index_entry_t slot = replica_ack_instance->bucket[slot_index];

                if (slot.status != 1) continue;
                if (slot.key_length != ack_slot->key_len) continue;
                if (slot.hash != ack_slot->key_hash) continue;

                bool found = false;

                // For every index entry's version number we need to count it in the candidates structure
                // This could be done more efficiently with a hash map or something but the structures are very short
                // so that is not done yet.
                for (uint32_t candidate_index = 0; candidate_index < candidate_count; candidate_index++) {
                    if (candidates[candidate_index].version_number == slot.version_number) {
                        found = true;
                        candidates[candidate_index].count++;
                        candidates[candidate_index].index_entry[replica_index] = slot;

                        if (replica_index == WRITE_BACK_REPLICA &&
                            replica_ack_instance->index_entry_written == slot_index
                        ) {
                            candidates[candidate_count].write_back = true;
                            candidates[candidate_index++].write_back_offset = ack_slot->starting_ack_data_offset;
                        }
                    }
                }

                if (!found) {
                    candidates[candidate_count].version_number = slot.version_number;
                    candidates[candidate_count].index_entry[replica_index] = slot;
                    candidates[candidate_count].count = 1;

                    if (replica_index == WRITE_BACK_REPLICA &&
                        replica_ack_instance->index_entry_written == slot_index
                    ) {
                        candidates[candidate_count].write_back = true;
                        candidates[candidate_count++].write_back_offset = ack_slot->starting_ack_data_offset;
                    } else {
                        candidates[candidate_count++].write_back = false;
                    }
                }
            }
        }
        build_candidates_ns += perf_now_ns() - build_candidates_start_ns;

        found_candidates_t found_candidates[REPLICA_COUNT * INDEX_SLOTS_PR_BUCKET];
        uint32_t found_candidates_count = 0;
        uint64_t filter_quorum_start_ns = perf_now_ns();
        for (uint32_t version_count_index = 0; version_count_index < candidate_count; version_count_index++) {
            if (candidates[version_count_index].count >= (REPLICA_COUNT + 1) / 2) {
                for (uint32_t i = 0; i < REPLICA_COUNT; i++) {
                    found_candidates[found_candidates_count].index_entry[i] = candidates[version_count_index].index_entry[i];
                }
                found_candidates[found_candidates_count].write_back = candidates[version_count_index].write_back;
                found_candidates[found_candidates_count].write_back_offset = candidates[version_count_index].write_back_offset;
                found_candidates[found_candidates_count++].version_number = candidates[version_count_index].version_number;
            }
        }
        filter_quorum_ns += perf_now_ns() - filter_quorum_start_ns;

        decision_start_ns = perf_now_ns();
        if (found_candidates_count == 0) {
            decision_ns += perf_now_ns() - decision_start_ns;
            decision_start_ns = perf_now_ns();
            if (ack_count == REPLICA_COUNT) {
                decision_ns += perf_now_ns() - decision_start_ns;
                uint64_t result_start_ns = perf_now_ns();
                // If we have gotten replies from all replicas and can not find a quorum, it is an error
                ack_slot->promise->result = PROMISE_ERROR_NO_MATCH;
                insert_duration_end_now(ack_slot->promise, ack_slot->start_time);
                get_2_sided_decrement();
                GET2_PHASE1_RETURN(true, result_start_ns);
            } else {
                decision_ns += perf_now_ns() - decision_start_ns;
                // We did not find any candidates, so we do not want to consume yet wait for more acks to arrive
                uint64_t result_start_ns = perf_now_ns();
                GET2_PHASE1_RETURN(false, result_start_ns);
            }
        }
        decision_ns += perf_now_ns() - decision_start_ns;

        uint32_t shipped = 0;
        uint64_t select_start_ns = perf_now_ns();
        uint64_t select_fastpath_start_ns = fastpath_verify_ns;
        uint64_t select_ship_start_ns = ship_phase2_ns;

        // Did find one or more quorums, lets try to get them.
        for (uint32_t candidate_index = 0; candidate_index < found_candidates_count; candidate_index++) {
            for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {

                // Check if this replica has that data
                if (found_candidates[candidate_index].index_entry[replica_index].status == 0) continue;

                // Now we have found a replica that has this candidate, ship it and continue with next c
                // We could optimize it so that if multiple of these candidates were shipped to a single server
                // we would switch to a vector dma transfer, but I think that the overhead of computing that outweighs the
                // benefit, at least since we probably usually only have a single candidate
                // TODO: This will make the load more heavy on the first replicas, should probably introduce some randomness
                uint8_t key_len = (uint8_t) found_candidates[candidate_index].index_entry[replica_index].key_length;
                uint32_t value_len = found_candidates[candidate_index].index_entry[replica_index].data_length;

                if (replica_index == WRITE_BACK_REPLICA && found_candidates[candidate_index].write_back) {
                    uint64_t verify_start_ns = perf_now_ns();
                    char *ack_data = ((char *) replica_ack) + ACK_REGION_SLOT_SIZE + found_candidates[candidate_index].write_back_offset;
                    uint32_t expected_hash = *((uint32_t *) (ack_data + key_len + value_len));
                    *((uint32_t *) (ack_data + key_len + value_len)) = found_candidates[candidate_index].version_number;

                    uint32_t hash = super_fast_hash(ack_data, (uint32_t) (key_len + value_len + sizeof(uint32_t)));
                    if (hash == expected_hash) {
                        ack_slot->promise->data = malloc(value_len);
                        if (ack_slot->promise->data == NULL) {
                            perror("malloc");
                            exit(EXIT_FAILURE);
                        }

                        //TODO: Full key verification?

                        for (uint32_t i = 0; i < value_len; i++) {
                            ((char *) ack_slot->promise->data)[i] = ack_data[key_len + i];
                        }
                        ack_slot->promise->result = PROMISE_SUCCESS_PH1;
                        insert_duration_end_now(ack_slot->promise, ack_slot->start_time);
                        get_2_sided_decrement();
                        fastpath_verify_ns += perf_now_ns() - verify_start_ns;
                        uint64_t select_total_ns = perf_now_ns() - select_start_ns;
                        uint64_t select_accounted_ns = (fastpath_verify_ns - select_fastpath_start_ns) + (ship_phase2_ns - select_ship_start_ns);
                        if (select_total_ns > select_accounted_ns) {
                            select_candidate_ns += select_total_ns - select_accounted_ns;
                        }
                        uint64_t result_start_ns = perf_now_ns();
                        GET2_PHASE1_RETURN(true, result_start_ns);
                    }
                    fastpath_verify_ns += perf_now_ns() - verify_start_ns;
                }

                ptrdiff_t server_data_offset = found_candidates[candidate_index].index_entry[replica_index].offset;
                uint32_t version_number = found_candidates[candidate_index].index_entry[replica_index].version_number;

                uint64_t ship_start_ns = perf_now_ns();
                queue_item_t queue_item;
                queue_item.version_number = version_number;
                queue_item.replica_index = replica_index;
                queue_item.key_len = key_len;
                queue_item.value_len = value_len;
                queue_item.server_data_offset = server_data_offset;
                queue_item.promise = ack_slot->promise;

                enqueue(queue_item);
                shipped++;
                ship_phase2_ns += perf_now_ns() - ship_start_ns;

                break;
            }
        }

        uint64_t select_total_ns = perf_now_ns() - select_start_ns;
        uint64_t select_accounted_ns = (fastpath_verify_ns - select_fastpath_start_ns) + (ship_phase2_ns - select_ship_start_ns);
        if (select_total_ns > select_accounted_ns) {
            select_candidate_ns += select_total_ns - select_accounted_ns;
        }

        /* Note that at this point it is in theory possible that we find a quorum for a spurious candidate,
         * and we would later receive the correct candidate. This will currently lead to a timeout, by design.
         * An alternative would be to not consume the ack after this phase, and only consume it after
         * A: a timeout
         * B: Wait for some very short timeout to hopefully get the last candidate.
         * C: We have hit a good candidate in phase 2.
         * We would however be careful as to not get a deadlock with the request queue being filled up by only phase1
         * requests. */
        decision_start_ns = perf_now_ns();
        if (shipped == 0) {
            decision_ns += perf_now_ns() - decision_start_ns;
            uint64_t result_start_ns = perf_now_ns();
            ack_slot->promise->result = PROMISE_ERROR_NO_MATCH;
            insert_duration_end_now(ack_slot->promise, ack_slot->start_time);
            get_2_sided_decrement();
            GET2_PHASE1_RETURN(true, result_start_ns);
        }
        decision_ns += perf_now_ns() - decision_start_ns;

        uint64_t result_start_ns = perf_now_ns();
        GET2_PHASE1_RETURN(true, result_start_ns);
    }

    uint64_t result_start_ns = perf_now_ns();
    GET2_PHASE1_RETURN(false, result_start_ns);

#undef GET2_PHASE1_RETURN
}

bool consume_get_ack_slot_phase2(ack_slot_t *ack_slot) {
    uint64_t poll_start_ns = perf_now_ns();

#define GET2_PHASE2_RETURN(result_value, result_start_ns) \
    do { \
        perf_record_ns(PROF_CLIENT_GET2_ACK_PHASE2_RESULT, perf_now_ns() - (result_start_ns)); \
        perf_record_ns(PROF_CLIENT_GET2_ACK_PHASE2_POLL, perf_now_ns() - poll_start_ns); \
        return (result_value); \
    } while (0)

    // Again: first thing to do is to check for a timeout
    uint64_t timeout_check_start_ns = perf_now_ns();
    struct timespec end_p;
    clock_gettime(CLOCK_MONOTONIC, &end_p);

    bool timed_out =
        ((end_p.tv_sec - ack_slot->start_time.tv_sec) * 1000000000L + (end_p.tv_nsec - ack_slot->start_time.tv_nsec)) >= GET_TIMEOUT_2_SIDED_NS;
    perf_record_ns(PROF_CLIENT_GET2_ACK_PHASE2_TIMEOUT_CHECK, perf_now_ns() - timeout_check_start_ns);

    if (timed_out) {
        uint64_t result_start_ns = perf_now_ns();
        ack_slot->promise->result = PROMISE_TIMEOUT;
        insert_duration_end_now(ack_slot->promise, ack_slot->start_time);
        GET2_PHASE2_RETURN(true, result_start_ns);
    }

    // We only use the index 0 of the replica slots no matter the index, as this put was only sent to a single replica
    if (ack_slot->replica_ack_instances[0]->replica_ack_type == REPLICA_NOT_ACKED) {
        uint64_t result_start_ns = perf_now_ns();
        GET2_PHASE2_RETURN(false, result_start_ns);
    }

    if (ack_slot->replica_ack_instances[0]->replica_ack_type != REPLICA_ACK_SUCCESS) {
        uint64_t result_start_ns = perf_now_ns();
        fprintf(stderr, "Unsupported ack state for phase2\n");
        ack_slot->promise->result = PROMISE_ERROR_TRANSFER;
        insert_duration_end_now(ack_slot->promise, ack_slot->start_time);
        GET2_PHASE2_RETURN(true, result_start_ns);
    }

    uint64_t verify_and_copy_start_ns = perf_now_ns();
    // First do verification
    char *ack_data = ((char *) replica_ack) + ACK_REGION_SLOT_SIZE + ack_slot->starting_ack_data_offset;

    uint32_t expected_hash = *((uint32_t *) (ack_data + ack_slot->key_len + ack_slot->value_len));
    *((uint32_t *) (ack_data + ack_slot->key_len + ack_slot->value_len)) = ack_slot->version_number;

    uint32_t hash = super_fast_hash(ack_data, (uint32_t) (ack_slot->key_len + ack_slot->value_len + sizeof(uint32_t)));
    if (hash != expected_hash) {
        //TODO: This seems like a bug, as we could have shipped multiple phase2 requests.
        uint64_t result_start_ns = perf_now_ns();
        ack_slot->promise->result = PROMISE_ERROR_NO_MATCH;
        insert_duration_end_now(ack_slot->promise, ack_slot->start_time);
        perf_record_ns(PROF_CLIENT_GET2_ACK_PHASE2_VERIFY_AND_COPY, perf_now_ns() - verify_and_copy_start_ns);
        GET2_PHASE2_RETURN(true, result_start_ns);
    }

    ack_slot->promise->data = malloc(ack_slot->value_len);
    if (ack_slot->promise->data == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    //TODO: No full key verification?
    for (uint32_t i = 0; i < ack_slot->value_len; i++) {
        ((char *) ack_slot->promise->data)[i] = ack_data[ack_slot->key_len + i];
    }
    ack_slot->promise->result = PROMISE_SUCCESS_PH2;
    insert_duration_end_now(ack_slot->promise, ack_slot->start_time);
    perf_record_ns(PROF_CLIENT_GET2_ACK_PHASE2_VERIFY_AND_COPY, perf_now_ns() - verify_and_copy_start_ns);
    uint64_t result_start_ns = perf_now_ns();
    GET2_PHASE2_RETURN(true, result_start_ns);

#undef GET2_PHASE2_RETURN
}

void send_phase_2_get(uint32_t version_number, uint32_t replica_index, uint8_t key_len, uint32_t value_len, ptrdiff_t server_data_offset, request_promise_t *promise) {
    ack_slot_t *ack_slot = get_ack_slot_blocking(GET_PHASE2, key_len, value_len, 0, key_len + value_len + sizeof(uint32_t), version_number, promise);

    send_request_region_slot(
        ack_slot->header_slot_WRITE_ONLY,
        key_len,
        value_len,
        version_number,
        (size_t) server_data_offset,
        ack_slot->starting_ack_data_offset,
        (uint8_t) replica_index,
        0,
        HEADER_SLOT_USED_GET_PHASE2
    );
}

static void *phase2_thread(__attribute__((unused)) void *_args) {
    queue_item_t queue_item;
    while (1) {
        queue_item = dequeue();
        send_phase_2_get(
                queue_item.version_number,
                queue_item.replica_index,
                queue_item.key_len,
                queue_item.value_len,
                queue_item.server_data_offset,
                queue_item.promise);
    }

    return NULL;
}
