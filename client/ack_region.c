#include <sisci_api.h>
#include <immintrin.h>
#include <stdlib.h>
#include <time.h>
#include <stdatomic.h>
#include "ack_region.h"
#include "index_data_protocol.h"
#include "request_region_connection.h"
#include "put.h"
#include "2_phase_2_sided.h"
#include "phase_2_queue.h"
#include "profiler.h"
#include "profiler_metrics.h"

#define ACK_ALLOC_SHARDS 2
#define ACK_DRAIN_BATCH 64

typedef struct {
    pthread_mutex_t mutex;
    uint32_t header_base;
    uint32_t header_count;
    uint32_t free_header_slot;
    uint32_t oldest_header_slot;
    uint32_t data_base;
    uint32_t data_region_space;
    uint32_t free_data_offset;
    uint32_t oldest_data_offset;
    uint32_t ack_base;
    uint32_t ack_region_space;
    uint32_t free_ack_offset;
    uint32_t oldest_ack_offset;
} ack_allocator_shard_t;

static ack_allocator_shard_t ack_shards[ACK_ALLOC_SHARDS];
static _Atomic uint32_t shard_assignment_counter = 0;
static _Atomic uint32_t current_get_2_sided_requests = 0;
static _Thread_local int tls_shard_id = -1;

replica_ack_t *replica_ack;
ack_slot_t ack_slots[REQUEST_SLOTS];

sci_local_segment_t ack_segment;
sci_map_t ack_map;

static uint32_t shard_partition_size(uint32_t total, uint32_t shard_index);
static uint32_t get_thread_shard_id(void);

static bool try_allocate_space(
    uint32_t required_space,
    uint32_t *free_offset,
    uint32_t oldest_offset,
    uint32_t *out_starting_offset,
    uint32_t *out_reserved_space,
    uint32_t region_space
);

void init_ack_region(sci_desc_t sd) {
    sci_error_t sci_error;
    SEOE(SCICreateSegment,
         sd,
         &ack_segment,
         ACK_SEGMENT_ID,
         ACK_REGION_SIZE,
         NO_CALLBACK,
         NO_ARG,
         NO_FLAGS
    );

    SEOE(SCIPrepareSegment,
         ack_segment,
         ADAPTER_NO,
         NO_FLAGS);

    SEOE(SCISetSegmentAvailable,
         ack_segment,
         ADAPTER_NO,
         NO_FLAGS);

    replica_ack = (replica_ack_t *) SCIMapLocalSegment(
            ack_segment,
            &ack_map,
            NO_OFFSET,
            ACK_REGION_SIZE,
            NO_SUGGESTED_ADDRESS,
            NO_FLAGS,
            &sci_error);

    if (sci_error != SCI_ERR_OK) {
        fprintf(stderr, "Could not map local segment: %s\n", SCIGetErrorString(sci_error));
        exit(EXIT_FAILURE);
    }

    uint32_t header_base = 0;
    uint32_t data_base = 0;
    uint32_t ack_base = 0;
    for (uint32_t shard_index = 0; shard_index < ACK_ALLOC_SHARDS; shard_index++) {
        ack_allocator_shard_t *shard = &ack_shards[shard_index];
        shard->header_base = header_base;
        shard->header_count = shard_partition_size(REQUEST_SLOTS, shard_index);
        shard->free_header_slot = 0;
        shard->oldest_header_slot = 0;

        shard->data_base = data_base;
        shard->data_region_space = shard_partition_size(REQUEST_REGION_DATA_SIZE, shard_index);
        shard->free_data_offset = 0;
        shard->oldest_data_offset = 0;

        shard->ack_base = ack_base;
        shard->ack_region_space = shard_partition_size(ACK_REGION_DATA_SIZE, shard_index);
        shard->free_ack_offset = 0;
        shard->oldest_ack_offset = 0;

        if (shard->header_count < 2 || shard->data_region_space == 0 || shard->ack_region_space == 0) {
            fprintf(stderr, "Illegal shard sizing for shard %u\n", shard_index);
            exit(EXIT_FAILURE);
        }

        pthread_mutex_init(&shard->mutex, NULL);
        header_base += shard->header_count;
        data_base += shard->data_region_space;
        ack_base += shard->ack_region_space;
    }
    if (header_base != REQUEST_SLOTS || data_base != REQUEST_REGION_DATA_SIZE || ack_base != ACK_REGION_DATA_SIZE) {
        fprintf(stderr, "Shard partitioning failed\n");
        exit(EXIT_FAILURE);
    }
}

// Critical region function
ack_slot_t *get_ack_slot_blocking(enum request_type request_type, uint8_t key_len, uint32_t value_len, uint32_t header_data_length, uint32_t ack_data_length, uint32_t version_number, request_promise_t *promise) {
    uint64_t alloc_start_ns = perf_now_ns();
    uint32_t shard_id = get_thread_shard_id();
    ack_allocator_shard_t *shard = &ack_shards[shard_id];
    uint64_t queue_wait_ns = 0;
    uint64_t header_wait_ns = 0;
    uint64_t retry_pause_ns = 0;
    uint64_t lock_wait_ns = 0;
    uint64_t unlock_wait_ns = 0;
    uint64_t critical_ns = 0;
    uint64_t critical_success_ns = 0;
    uint64_t critical_slot_check_ns = 0;
    uint64_t data_space_total_ns = 0;
    uint64_t ack_space_total_ns = 0;
    uint64_t data_space_wait_ns = 0;
    uint64_t ack_space_wait_ns = 0;
    if (request_type == GET_PHASE1) {
        uint64_t queue_wait_start_ns = perf_now_ns();
        while (1) {
            uint32_t queued_requests = atomic_load_explicit(&current_get_2_sided_requests, memory_order_relaxed);
            if (queued_requests < QUEUE_SPACE &&
                atomic_compare_exchange_weak_explicit(
                    &current_get_2_sided_requests,
                    &queued_requests,
                    queued_requests + 1,
                    memory_order_acq_rel,
                    memory_order_relaxed)) {
                break;
            }
            _mm_pause();
        }
        queue_wait_ns += perf_now_ns() - queue_wait_start_ns;
    }

    bool available_slot = false;
    ack_slot_t *ack_slot;
    uint64_t replica_reset_ns = 0;
    uint64_t promise_alloc_ns = 0;
    uint64_t slot_prep_ns = 0;
    uint64_t commit_ns = 0;
    while (!available_slot) {
        bool blocked_by_data_space = false;
        bool blocked_by_ack_space = false;
        bool blocked_by_header_slot = false;

        uint64_t lock_wait_start_ns = perf_now_ns();
        pthread_mutex_lock(&shard->mutex);
        lock_wait_ns += perf_now_ns() - lock_wait_start_ns;
        uint64_t critical_start_ns = perf_now_ns();
        uint64_t slot_check_start_ns = perf_now_ns();
        uint32_t next_free_header_slot = (shard->free_header_slot + 1) % shard->header_count;
        available_slot = next_free_header_slot != shard->oldest_header_slot;
        critical_slot_check_ns += perf_now_ns() - slot_check_start_ns;
        if (available_slot) {
            uint32_t new_free_data_offset = shard->free_data_offset;
            uint32_t new_free_ack_offset = shard->free_ack_offset;
            uint32_t starting_data_offset = 0;
            uint32_t starting_ack_offset = 0;
            uint32_t reserved_data_size = 0;
            uint32_t reserved_ack_size = 0;

            uint64_t data_space_start_ns = perf_now_ns();
            bool data_space_available = try_allocate_space(
                header_data_length,
                &new_free_data_offset,
                shard->oldest_data_offset,
                &starting_data_offset,
                &reserved_data_size,
                shard->data_region_space
            );
            data_space_total_ns += perf_now_ns() - data_space_start_ns;

            bool ack_space_available = false;
            if (data_space_available) {
                uint64_t ack_space_start_ns = perf_now_ns();
                ack_space_available = try_allocate_space(
                    ack_data_length,
                    &new_free_ack_offset,
                    shard->oldest_ack_offset,
                    &starting_ack_offset,
                    &reserved_ack_size,
                    shard->ack_region_space
                );
                ack_space_total_ns += perf_now_ns() - ack_space_start_ns;
            }

            if (!data_space_available) {
                blocked_by_data_space = true;
                available_slot = false;
            } else if (!ack_space_available) {
                blocked_by_ack_space = true;
                available_slot = false;
            } else {
                uint64_t slot_prep_start_ns = perf_now_ns();
                uint32_t global_header_slot = shard->header_base + shard->free_header_slot;
                ack_slot = &ack_slots[global_header_slot];
                ack_slot->header_slot_WRITE_ONLY = &request_region->header_slots[global_header_slot];

                uint64_t replica_reset_start_ns = perf_now_ns();
                for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
                    replica_ack_t *replica_ack_instance = replica_ack + (global_header_slot * REPLICA_COUNT) + replica_index;
                    ack_slot->replica_ack_instances[replica_index] = replica_ack_instance;
                    replica_ack_instance->replica_ack_type = REPLICA_NOT_ACKED;
                    replica_ack_instance->version_number = 0;
                }
                replica_reset_ns += perf_now_ns() - replica_reset_start_ns;

                if (promise == NULL) {
                    uint64_t promise_alloc_start_ns = perf_now_ns();
                    promise = malloc(sizeof(request_promise_t));
                    if (promise == NULL) {
                        perror("malloc");
                        exit(EXIT_FAILURE);
                    }
                    promise_alloc_ns += perf_now_ns() - promise_alloc_start_ns;
                }

                ack_slot->request_type = request_type;

                switch (ack_slot->request_type) {
                    case PUT:
                        promise->result = PROMISE_PENDING;
                        promise->operation = OP_PUT;
                        break;
                    case GET_PHASE1:
                        promise->result = PROMISE_PENDING;
                        promise->operation = OP_GET;
                        break;
                    case GET_PHASE2:
                        break;
                    default:
                        fprintf(stderr, "Got illegal request type\n");
                        exit(EXIT_FAILURE);
                }

                ack_slot->promise = promise;
                ack_slot->key_len = key_len;
                ack_slot->value_len = value_len;
                ack_slot->version_number = version_number;
                ack_slot->shard_id = (uint8_t) shard_id;
                ack_slot->starting_data_offset = shard->data_base + starting_data_offset;
                ack_slot->data_size = reserved_data_size;
                ack_slot->starting_ack_data_offset = shard->ack_base + starting_ack_offset;
                ack_slot->ack_data_size = reserved_ack_size;
                slot_prep_ns += perf_now_ns() - slot_prep_start_ns;

                uint64_t commit_start_ns = perf_now_ns();
                clock_gettime(CLOCK_MONOTONIC, &ack_slot->start_time);

                shard->free_data_offset = new_free_data_offset;
                shard->free_ack_offset = new_free_ack_offset;
                shard->free_header_slot = next_free_header_slot;
                commit_ns += perf_now_ns() - commit_start_ns;
            }
        } else {
            blocked_by_header_slot = true;
        }
        uint64_t critical_elapsed_ns = perf_now_ns() - critical_start_ns;
        critical_ns += critical_elapsed_ns;
        if (available_slot) {
            critical_success_ns += critical_elapsed_ns;
        }
        uint64_t unlock_start_ns = perf_now_ns();
        pthread_mutex_unlock(&shard->mutex);
        unlock_wait_ns += perf_now_ns() - unlock_start_ns;

        if (!available_slot) {
            uint64_t pause_start_ns = perf_now_ns();
            _mm_pause();
            uint64_t pause_ns = perf_now_ns() - pause_start_ns;
            if (blocked_by_header_slot) {
                header_wait_ns += pause_ns;
            } else {
                retry_pause_ns += pause_ns;
                if (blocked_by_data_space) {
                    data_space_wait_ns += pause_ns;
                }
                if (blocked_by_ack_space) {
                    ack_space_wait_ns += pause_ns;
                }
            }
        }
    }

    uint64_t alloc_total_ns = perf_now_ns() - alloc_start_ns;
    uint64_t alloc_accounted_ns =
        queue_wait_ns +
        header_wait_ns +
        retry_pause_ns +
        lock_wait_ns +
        unlock_wait_ns +
        critical_ns;
    uint64_t alloc_residual_ns = alloc_total_ns > alloc_accounted_ns ? alloc_total_ns - alloc_accounted_ns : 0;
    uint64_t critical_no_slot_ns = critical_ns > critical_success_ns ? critical_ns - critical_success_ns : 0;

    perf_record_ns(PROF_CLIENT_ACK_GET_QUEUE_WAIT, queue_wait_ns);
    perf_record_ns(PROF_CLIENT_ACK_HEADER_SLOT_WAIT, header_wait_ns);
    perf_record_ns(PROF_CLIENT_ACK_RETRY_PAUSE, retry_pause_ns);
    perf_record_ns(PROF_CLIENT_ACK_MUTEX_LOCK_WAIT, lock_wait_ns);
    perf_record_ns(PROF_CLIENT_ACK_MUTEX_UNLOCK_WAIT, unlock_wait_ns);
    perf_record_ns(PROF_CLIENT_ACK_CRITICAL_SECTION, critical_ns);
    perf_record_ns(PROF_CLIENT_ACK_CRITICAL_SLOT_CHECK, critical_slot_check_ns);
    perf_record_ns(PROF_CLIENT_ACK_CRITICAL_NO_SLOT, critical_no_slot_ns);
    perf_record_ns(PROF_CLIENT_ACK_DATA_SPACE_WAIT, data_space_wait_ns);
    perf_record_ns(PROF_CLIENT_ACK_DATA_SPACE_TOTAL, data_space_total_ns);
    perf_record_ns(PROF_CLIENT_ACK_ACK_SPACE_WAIT, ack_space_wait_ns);
    perf_record_ns(PROF_CLIENT_ACK_ACK_SPACE_TOTAL, ack_space_total_ns);
    perf_record_ns(PROF_CLIENT_ACK_ALLOC_TOTAL, alloc_total_ns);
    perf_record_ns(PROF_CLIENT_ACK_ALLOC_RESIDUAL, alloc_residual_ns);
    perf_record_ns(PROF_CLIENT_ACK_REPLICA_RESET, replica_reset_ns);
    perf_record_ns(PROF_CLIENT_ACK_PROMISE_ALLOC, promise_alloc_ns);
    perf_record_ns(PROF_CLIENT_ACK_SLOT_PREP, slot_prep_ns);
    perf_record_ns(PROF_CLIENT_ACK_COMMIT, commit_ns);

    return ack_slot;
}

void get_2_sided_decrement(void) {
    atomic_fetch_sub_explicit(&current_get_2_sided_requests, 1, memory_order_acq_rel);
}

void *ack_thread(__attribute__((unused)) void *_args) {
    uint32_t shard_cursor = 0;
    while (1) {
        bool did_work = false;
        for (uint32_t scanned = 0; scanned < ACK_ALLOC_SHARDS; scanned++) {
            uint32_t shard_id = (shard_cursor + scanned) % ACK_ALLOC_SHARDS;
            ack_allocator_shard_t *shard = &ack_shards[shard_id];

            for (uint32_t drained = 0; drained < ACK_DRAIN_BATCH; drained++) {
                bool has_pending = false;
                uint32_t local_oldest_header_slot = 0;
                uint32_t global_header_slot = 0;

                pthread_mutex_lock(&shard->mutex);
                if (shard->oldest_header_slot != shard->free_header_slot) {
                    has_pending = true;
                    local_oldest_header_slot = shard->oldest_header_slot;
                    global_header_slot = shard->header_base + local_oldest_header_slot;
                }
                pthread_mutex_unlock(&shard->mutex);

                if (!has_pending) {
                    break;
                }

                ack_slot_t *ack_slot = &ack_slots[global_header_slot];

                bool consumed;
                switch (ack_slot->request_type) {
                    case PUT:
                        consumed = consume_put_ack_slot(ack_slot);
                        break;
                    case GET_PHASE1:
                        consumed = consume_get_ack_slot_phase1(ack_slot);
                        break;
                    case GET_PHASE2:
                        consumed = consume_get_ack_slot_phase2(ack_slot);
                        if (consumed) get_2_sided_decrement();
                        break;
                    default:
                        fprintf(stderr, "Got illegal request type\n");
                        exit(EXIT_FAILURE);
                }

                if (!consumed) {
                    break;
                }

                pthread_mutex_lock(&shard->mutex);
                if (shard->oldest_header_slot == local_oldest_header_slot) {
                    ack_slot->header_slot_WRITE_ONLY->status = HEADER_SLOT_UNUSED;
                    shard->oldest_header_slot = (shard->oldest_header_slot + 1) % shard->header_count;
                    shard->oldest_data_offset = (shard->oldest_data_offset + ack_slot->data_size) % shard->data_region_space;
                    shard->oldest_ack_offset = (shard->oldest_ack_offset + ack_slot->ack_data_size) % shard->ack_region_space;
                }
                pthread_mutex_unlock(&shard->mutex);
                did_work = true;
            }
        }
        shard_cursor = (shard_cursor + 1u) % ACK_ALLOC_SHARDS;
        if (!did_work) {
            _mm_pause();
        }
    }

    return NULL;
}

static uint32_t shard_partition_size(uint32_t total, uint32_t shard_index) {
    uint32_t base = total / ACK_ALLOC_SHARDS;
    uint32_t remainder = total % ACK_ALLOC_SHARDS;
    return base + (shard_index < remainder ? 1u : 0u);
}

static uint32_t get_thread_shard_id(void) {
    if (tls_shard_id < 0) {
        uint32_t assigned = atomic_fetch_add_explicit(&shard_assignment_counter, 1, memory_order_relaxed);
        tls_shard_id = (int) (assigned % ACK_ALLOC_SHARDS);
    }
    return (uint32_t) tls_shard_id;
}

static bool try_allocate_space(
    uint32_t required_space,
    uint32_t *free_offset,
    uint32_t oldest_offset,
    uint32_t *out_starting_offset,
    uint32_t *out_reserved_space,
    uint32_t region_space
) {
    if (required_space > region_space) {
        return false;
    }

    if (*free_offset < oldest_offset) {
        uint32_t contiguous_space = oldest_offset - *free_offset;
        if (contiguous_space < required_space) {
            return false;
        }

        *out_starting_offset = *free_offset;
        *out_reserved_space = required_space;
        *free_offset += required_space;
        if (*free_offset == region_space) {
            *free_offset = 0;
        }
        return true;
    }

    uint32_t tail_space = region_space - *free_offset;
    if (tail_space >= required_space) {
        *out_starting_offset = *free_offset;
        *out_reserved_space = required_space;
        *free_offset = (*free_offset + required_space) % region_space;
        return true;
    }

    if (oldest_offset < required_space) {
        return false;
    }

    // Skip the tail and reserve it as padding so reclaiming stays monotonic.
    *out_starting_offset = 0;
    *out_reserved_space = tail_space + required_space;
    *free_offset = required_space;
    return true;
}

void insert_duration(request_promise_t *promise, struct timespec ts_pre, struct timespec ts_post) {
    promise->duration.tv_sec = ts_post.tv_sec - ts_pre.tv_sec;
    promise->duration.tv_nsec = ts_post.tv_nsec - ts_pre.tv_nsec;
    if (promise->duration.tv_nsec < 0) {
        promise->duration.tv_sec -= 1;
        promise->duration.tv_nsec += 1000000000L;
    }
}

void insert_duration_end_now(request_promise_t *promise, struct timespec ts_pre) {
    struct timespec ts_post;
    clock_gettime(CLOCK_MONOTONIC, &ts_post);
    insert_duration(promise, ts_pre, ts_post);
}
