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

static void *phase2_thread(__attribute__((unused)) void *_args);

void init_2_phase_2_sided_get(void) {
    queue_init();
    pthread_t id;
    pthread_create(&id, NULL, phase2_thread, NULL);
}

request_promise_t *get_2_phase_2_sided(const char *key, uint8_t key_len) {
    // First we need to get a header slot
    // Then we need to broadcast the request
    // Then wait until we have a quorum
    // Then fetch data from preferred backend

    ack_slot_t *ack_slot = get_ack_slot_blocking(GET_PHASE1, key_len, 0, key_len, SPECULATIVE_SIZE, 0, NULL);

    uint32_t starting_offset = ack_slot->starting_data_offset;
    uint32_t current_offset = starting_offset;
    volatile char *data_region_start = ((volatile char *) request_region) + sizeof(request_region_t);

    char *hash_data = malloc(key_len);
    if (hash_data == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    // First copy the key
    for (uint32_t i = 0; i < key_len; i++) {
        hash_data[i] = key[i];
        data_region_start[current_offset] = key[i];
        current_offset = (current_offset + 1) % REQUEST_REGION_DATA_SIZE;
    }

    uint32_t key_hash = super_fast_hash(hash_data, (int) (key_len));

    ack_slot->key_hash = key_hash;

    ack_slot->header_slot_WRITE_ONLY->payload_hash = key_hash;
    ack_slot->header_slot_WRITE_ONLY->offset = (size_t) starting_offset;
    ack_slot->header_slot_WRITE_ONLY->key_length = key_len;
    ack_slot->header_slot_WRITE_ONLY->value_length = 0;
    ack_slot->header_slot_WRITE_ONLY->replica_write_back_hint = WRITE_BACK_REPLICA; // TODO: Use 'best' replica
    ack_slot->header_slot_WRITE_ONLY->return_offset = ack_slot->starting_ack_data_offset;
    ack_slot->header_slot_WRITE_ONLY->version_number = ack_slot->version_number;
    check_for_errors(request_sequence);
    ack_slot->header_slot_WRITE_ONLY->status = HEADER_SLOT_USED_GET_PHASE1;

    return ack_slot->promise;
}

bool consume_get_ack_slot_phase1(ack_slot_t *ack_slot) {
    // First thing we do is to check for a timeout
    struct timespec end_p;
    clock_gettime(CLOCK_MONOTONIC, &end_p);

    if (((end_p.tv_sec - ack_slot->start_time.tv_sec) * 1000000000L + (end_p.tv_nsec - ack_slot->start_time.tv_nsec)) >= GET_TIMEOUT_2_SIDED_NS) {
        ack_slot->promise->result = PROMISE_TIMEOUT;
        get_2_sided_decrement();
        return true;
    }

    uint32_t ack_success_count = 0;
    uint32_t ack_count = 0;

    for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        replica_ack_t *replica_ack_instance = ack_slot->replica_ack_instances[replica_index];
        enum replica_ack_type ack_type = replica_ack_instance->replica_ack_type;

        if (ack_type != REPLICA_NOT_ACKED)
            ack_count++;

        if (ack_type == REPLICA_ACK_SUCCESS)
            ack_success_count++;
    }

    if (ack_success_count >= (REPLICA_COUNT + 1) / 2) {
        // Now we must check if we have a quorum, if we do dispatch final request and return true
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
            for (uint32_t slot_index = 0; slot_index < INDEX_SLOTS_PR_BUCKET; slot_index++) {
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

        found_candidates_t found_candidates[REPLICA_COUNT * INDEX_SLOTS_PR_BUCKET];
        uint32_t found_candidates_count = 0;
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

        if (found_candidates_count == 0) {
            if (ack_count == REPLICA_COUNT) {
                // If we have gotten replies from all replicas and can not find a quorum, it is an error
                ack_slot->promise->result = PROMISE_ERROR_NO_MATCH;
                get_2_sided_decrement();
                return true;
            } else {
                // We did not find any candidates, so we do not want to consume yet wait for more acks to arrive
                return false;
            }
        }

        uint32_t shipped = 0;

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
                    char *ack_data = ((char *) replica_ack) + ACK_REGION_SLOT_SIZE + found_candidates[candidate_index].write_back_offset;
                    uint32_t expected_hash = *((uint32_t *) (ack_data + key_len + value_len));
                    *((uint32_t *) (ack_data + key_len + value_len)) = found_candidates[candidate_index].version_number;

                    uint32_t hash = super_fast_hash(ack_data, (int) (key_len + value_len + sizeof(uint32_t)));
                    if (hash == expected_hash) {
                        ack_slot->promise->data = malloc(value_len);
                        if (ack_slot->promise->data == NULL) {
                            perror("malloc");
                            exit(EXIT_FAILURE);
                        }

                        memcpy(ack_slot->promise->data, ack_data + key_len, value_len);
                        ack_slot->promise->result = PROMISE_SUCCESS;
                        get_2_sided_decrement();
                        return true;
                    }
                }

                ptrdiff_t server_data_offset = found_candidates[candidate_index].index_entry[replica_index].offset;
                uint32_t version_number = found_candidates[candidate_index].index_entry[replica_index].version_number;

                queue_item_t queue_item;
                queue_item.version_number = version_number;
                queue_item.replica_index = replica_index;
                queue_item.key_len = key_len;
                queue_item.value_len = value_len;
                queue_item.server_data_offset = server_data_offset;
                queue_item.promise = ack_slot->promise;

                enqueue(queue_item);
                shipped++;

                break;
            }
        }

        /* Note that at this point it is in theory possible that we find a quorum for a spurious candidate,
         * and we would later receive the correct candidate. This will currently lead to a timeout, by design.
         * An alternative would be to not consume the ack after this phase, and only consume it after
         * A: a timeout
         * B: Wait for some very short timeout to hopefully get the last candidate.
         * C: We have hit a good candidate in phase 2.
         * We would however be careful as to not get a deadlock with the request queue being filled up by only phase1
         * requests. */
        if (shipped == 0) {
            ack_slot->promise->result = PROMISE_ERROR_NO_MATCH;
            get_2_sided_decrement();
            return true;
        }

        return true;
    }

    return false;
}

bool consume_get_ack_slot_phase2(ack_slot_t *ack_slot) {
    // Again: first thing to do is to check for a timeout
    struct timespec end_p;
    clock_gettime(CLOCK_MONOTONIC, &end_p);

    if (((end_p.tv_sec - ack_slot->start_time.tv_sec) * 1000000000L + (end_p.tv_nsec - ack_slot->start_time.tv_nsec)) >= GET_TIMEOUT_2_SIDED_NS) {
        ack_slot->promise->result = PROMISE_TIMEOUT;
        return true;
    }

    // We only use the index 0 of the replica slots no matter the index, as this put was only sent to a single replica
    if (ack_slot->replica_ack_instances[0]->replica_ack_type == REPLICA_NOT_ACKED) {
        return false;
    }

    if (ack_slot->replica_ack_instances[0]->replica_ack_type != REPLICA_ACK_SUCCESS) {
        fprintf(stderr, "Unsupported ack state for phase2\n");
        ack_slot->promise->result = PROMISE_ERROR_TRANSFER;
        return true;
    }

    // First do verification
    char *ack_data = ((char *) replica_ack) + ACK_REGION_SLOT_SIZE + ack_slot->starting_ack_data_offset;

    uint32_t expected_hash = *((uint32_t *) (ack_data + ack_slot->key_len + ack_slot->value_len));
    *((uint32_t *) (ack_data + ack_slot->key_len + ack_slot->value_len)) = ack_slot->version_number;

    uint32_t hash = super_fast_hash(ack_data, (int) (ack_slot->key_len + ack_slot->value_len + sizeof(uint32_t)));
    if (hash != expected_hash) {
        ack_slot->promise->result = PROMISE_ERROR_NO_MATCH;
        return true;
    }

    ack_slot->promise->data = malloc(ack_slot->value_len);
    if (ack_slot->promise->data == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    memcpy(ack_slot->promise->data, ack_data + ack_slot->key_len, ack_slot->value_len);
    ack_slot->promise->result = PROMISE_SUCCESS;
    return true;
}

void send_phase_2_get(uint32_t version_number, uint32_t replica_index, uint8_t key_len, uint32_t value_len, ptrdiff_t server_data_offset, request_promise_t *promise) {
    ack_slot_t *ack_slot = get_ack_slot_blocking(GET_PHASE2, key_len, value_len, 0, key_len + value_len + sizeof(uint32_t), version_number, promise);

    ack_slot->header_slot_WRITE_ONLY->offset = (size_t) server_data_offset;
    ack_slot->header_slot_WRITE_ONLY->return_offset = ack_slot->starting_ack_data_offset;
    ack_slot->header_slot_WRITE_ONLY->key_length = key_len;
    ack_slot->header_slot_WRITE_ONLY->value_length = value_len;
    ack_slot->header_slot_WRITE_ONLY->version_number = version_number;
    ack_slot->header_slot_WRITE_ONLY->replica_write_back_hint = replica_index;
    check_for_errors(request_sequence);
    ack_slot->header_slot_WRITE_ONLY->status = HEADER_SLOT_USED_GET_PHASE2;
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
}
