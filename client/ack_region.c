#include <sisci_api.h>
#include <stdlib.h>
#include <time.h>
#include "ack_region.h"
#include "index_data_protocol.h"
#include "request_region_connection.h"
#include "put.h"
#include "2_phase_2_sided.h"
#include "phase_2_queue.h"

static pthread_mutex_t ack_mutex;

static uint32_t free_header_slot = 0;
static uint32_t oldest_header_slot = 0;

static uint32_t free_ack_offset = 0;
static uint32_t oldest_ack_offset = 0;

static uint32_t current_get_2_sided_requests = 0;

static uint32_t free_data_offset = 0;
static uint32_t oldest_data_offset = 0;

replica_ack_t *replica_ack;
ack_slot_t ack_slots[MAX_REQUEST_SLOTS];

sci_local_segment_t ack_segment;
sci_map_t ack_map;

static void block_for_available_space(uint32_t required_space, uint32_t *free_offset, uint32_t oldest_offset, uint32_t *out_starting_offset, uint32_t region_space);

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
    if (request_type == GET_PHASE1) {
        bool available_get_queue_space = false;
        while (!available_get_queue_space) {
            pthread_mutex_lock(&ack_mutex);
            available_get_queue_space = current_get_2_sided_requests < QUEUE_SPACE;
            if (available_get_queue_space) {
                current_get_2_sided_requests++;
            }
            pthread_mutex_unlock(&ack_mutex);
        }
    }

    bool available_slot = false;
    ack_slot_t *ack_slot;
    while (!available_slot) {
        pthread_mutex_lock(&ack_mutex);
        available_slot = (free_header_slot + 1) % MAX_REQUEST_SLOTS != oldest_header_slot;
        if (available_slot) {
            ack_slot = &ack_slots[free_header_slot];
            ack_slot->header_slot_WRITE_ONLY = &request_region->header_slots[free_header_slot];

            for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
                replica_ack_t *replica_ack_instance = replica_ack + (free_header_slot * REPLICA_COUNT) + replica_index;
                ack_slot->replica_ack_instances[replica_index] = replica_ack_instance;
                replica_ack_instance->replica_ack_type = REPLICA_NOT_ACKED;
                replica_ack_instance->version_number = 0;
            }

            clock_gettime(CLOCK_MONOTONIC, &ack_slot->start_time);

            if (promise == NULL) {
                promise = malloc(sizeof(request_promise_t));
                if (promise == NULL) {
                    perror("malloc");
                    exit(EXIT_FAILURE);
                }
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

            // Wait for enough space
            block_for_available_space(header_data_length, &free_data_offset, oldest_data_offset, &ack_slot->starting_data_offset, REQUEST_REGION_DATA_SIZE);
            ack_slot->data_size = header_data_length;

            block_for_available_space(ack_data_length, &free_ack_offset, oldest_ack_offset, &ack_slot->starting_ack_data_offset, ACK_REGION_DATA_SIZE);
            ack_slot->ack_data_size = ack_data_length;

            free_header_slot = (free_header_slot + 1) % MAX_REQUEST_SLOTS;
        }
        pthread_mutex_unlock(&ack_mutex);
    }

    return ack_slot;
}

void get_2_sided_decrement(void) {
    current_get_2_sided_requests--;
}

void *ack_thread(__attribute__((unused)) void *_args) {
    while (1) {
        pthread_mutex_lock(&ack_mutex);
        if (oldest_header_slot == free_header_slot) { pthread_mutex_unlock(&ack_mutex); continue; }

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
        oldest_header_slot = (oldest_header_slot + 1) % MAX_REQUEST_SLOTS;
        oldest_data_offset = (oldest_data_offset + ack_slot->data_size) % REQUEST_REGION_DATA_SIZE;
        oldest_ack_offset = (oldest_ack_offset + ack_slot->ack_data_size) % ACK_REGION_DATA_SIZE;
        pthread_mutex_unlock(&ack_mutex);

    }

    return NULL;
}

static void block_for_available_space(uint32_t required_space, uint32_t *free_offset, uint32_t oldest_offset, uint32_t *out_starting_offset, uint32_t region_space) {
    bool available_space = false;
    while (!available_space) {
        uint32_t used = (*free_offset + region_space
                         - oldest_offset)
                        % region_space;

        uint32_t free_space = region_space - used;
        available_space = free_space >= (required_space);

        if (available_space) {
            // There's enough space
            *out_starting_offset = *free_offset;
            *free_offset = (*free_offset + required_space) % region_space;
        }
    }
}
