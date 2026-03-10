#include <sisci_api.h>
#include <immintrin.h>
#include <stdlib.h>
#include <time.h>
#include "ack_region.h"
#include "index_data_protocol.h"
#include "request_region_connection.h"
#include "put.h"
#include "2_phase_2_sided.h"
#include "phase_2_queue.h"
#include "profiler.h"

static pthread_mutex_t ack_mutex;

static uint32_t free_header_slot = 0;
static uint32_t oldest_header_slot = 0;

static uint32_t free_ack_offset = 0;
static uint32_t oldest_ack_offset = 0;

static uint32_t current_get_2_sided_requests = 0;

static uint32_t free_data_offset = 0;
static uint32_t oldest_data_offset = 0;

replica_ack_t *replica_ack;
ack_slot_t ack_slots[REQUEST_SLOTS];

sci_local_segment_t ack_segment;
sci_map_t ack_map;

static void block_for_available_space(uint32_t required_space, uint32_t *free_offset, uint32_t oldest_offset, uint32_t *out_starting_offset, uint32_t region_space, perf_metric_id_t wait_metric);

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

    pthread_mutex_init(&ack_mutex, NULL);
}

// Critical region function
ack_slot_t *get_ack_slot_blocking(enum request_type request_type, uint8_t key_len, uint32_t value_len, uint32_t header_data_length, uint32_t ack_data_length, uint32_t version_number, request_promise_t *promise) {
    uint64_t alloc_start_ns = perf_now_ns();
    if (request_type == GET_PHASE1) {
        uint64_t queue_wait_start_ns = perf_now_ns();
        bool available_get_queue_space = false;
        while (1) {
            pthread_mutex_lock(&ack_mutex);
            available_get_queue_space = current_get_2_sided_requests < QUEUE_SPACE;
            if (available_get_queue_space) {
                current_get_2_sided_requests++;
            }
            pthread_mutex_unlock(&ack_mutex);
            if (available_get_queue_space) {
                break;
            }
            _mm_pause();
        }
        perf_record_ns(PROF_CLIENT_ACK_GET_QUEUE_WAIT, perf_now_ns() - queue_wait_start_ns);
    }

    bool available_slot = false;
    ack_slot_t *ack_slot;
    uint64_t header_wait_ns = 0;
    uint64_t replica_reset_ns = 0;
    uint64_t promise_alloc_ns = 0;
    uint64_t slot_prep_ns = 0;
    uint64_t commit_ns = 0;
    while (!available_slot) {
        if ((free_header_slot + 1) % REQUEST_SLOTS == oldest_header_slot) {
            uint64_t wait_start_ns = perf_now_ns();
            while ((free_header_slot + 1) % REQUEST_SLOTS == oldest_header_slot) {
                _mm_pause();
            }
            header_wait_ns += perf_now_ns() - wait_start_ns;
            continue;
        }

        pthread_mutex_lock(&ack_mutex);
        available_slot = (free_header_slot + 1) % REQUEST_SLOTS != oldest_header_slot;
        if (available_slot) {
            uint64_t slot_prep_start_ns = perf_now_ns();
            ack_slot = &ack_slots[free_header_slot];
            ack_slot->header_slot_WRITE_ONLY = &request_region->header_slots[free_header_slot];

            uint64_t replica_reset_start_ns = perf_now_ns();
            for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
                replica_ack_t *replica_ack_instance = replica_ack + (free_header_slot * REPLICA_COUNT) + replica_index;
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
            slot_prep_ns += perf_now_ns() - slot_prep_start_ns;

            // Wait for enough space
            block_for_available_space(
                header_data_length,
                &free_data_offset,
                oldest_data_offset,
                &ack_slot->starting_data_offset,
                REQUEST_REGION_DATA_SIZE,
                PROF_CLIENT_ACK_DATA_SPACE_WAIT
            );
            ack_slot->data_size = header_data_length;

            block_for_available_space(
                ack_data_length,
                &free_ack_offset,
                oldest_ack_offset,
                &ack_slot->starting_ack_data_offset,
                ACK_REGION_DATA_SIZE,
                PROF_CLIENT_ACK_ACK_SPACE_WAIT
            );
            ack_slot->ack_data_size = ack_data_length;

            uint64_t commit_start_ns = perf_now_ns();
            clock_gettime(CLOCK_MONOTONIC, &ack_slot->start_time);

            free_header_slot = (free_header_slot + 1) % REQUEST_SLOTS;
            commit_ns += perf_now_ns() - commit_start_ns;
        }
        pthread_mutex_unlock(&ack_mutex);

        if (!available_slot) {
            _mm_pause();
        }
    }

    perf_record_ns(PROF_CLIENT_ACK_HEADER_SLOT_WAIT, header_wait_ns);
    perf_record_ns(PROF_CLIENT_ACK_ALLOC_TOTAL, perf_now_ns() - alloc_start_ns);
    perf_record_ns(PROF_CLIENT_ACK_REPLICA_RESET, replica_reset_ns);
    perf_record_ns(PROF_CLIENT_ACK_PROMISE_ALLOC, promise_alloc_ns);
    perf_record_ns(PROF_CLIENT_ACK_SLOT_PREP, slot_prep_ns);
    perf_record_ns(PROF_CLIENT_ACK_COMMIT, commit_ns);

    return ack_slot;
}

void get_2_sided_decrement(void) {
    current_get_2_sided_requests--;
}

void *ack_thread(__attribute__((unused)) void *_args) {
    while (1) {
        pthread_mutex_lock(&ack_mutex);
        if (oldest_header_slot == free_header_slot) { 
            pthread_mutex_unlock(&ack_mutex);
            _mm_pause();
            continue; 
        }

        ack_slot_t *ack_slot = &ack_slots[oldest_header_slot];

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
            pthread_mutex_unlock(&ack_mutex);
            continue;
        }

        ack_slot->header_slot_WRITE_ONLY->status = HEADER_SLOT_UNUSED;
        oldest_header_slot = (oldest_header_slot + 1) % REQUEST_SLOTS;
        oldest_data_offset = (oldest_data_offset + ack_slot->data_size) % REQUEST_REGION_DATA_SIZE;
        oldest_ack_offset = (oldest_ack_offset + ack_slot->ack_data_size) % ACK_REGION_DATA_SIZE;
        pthread_mutex_unlock(&ack_mutex);

        _mm_pause();
    }

    return NULL;
}

static void block_for_available_space(uint32_t required_space, uint32_t *free_offset, uint32_t oldest_offset, uint32_t *out_starting_offset, uint32_t region_space, perf_metric_id_t wait_metric) {
    uint64_t wait_ns = 0;
    bool is_waiting = false;
    uint64_t wait_start_ns = 0;
    bool available_space = false;
    while (1) {
        uint32_t used = (*free_offset + region_space
                         - oldest_offset)
                        % region_space;

        uint32_t free_space = region_space - used;
        available_space = free_space >= (required_space);

        if (available_space) {
            if (is_waiting) {
                wait_ns += perf_now_ns() - wait_start_ns;
            }
            // There's enough space
            *out_starting_offset = *free_offset;
            *free_offset = (*free_offset + required_space) % region_space;
            break;
        }

        if (!is_waiting) {
            wait_start_ns = perf_now_ns();
            is_waiting = true;
        }
        _mm_pause();
    }

    perf_record_ns(wait_metric, wait_ns);
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
