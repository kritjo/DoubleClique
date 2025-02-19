#include "main.h"

#include <stdlib.h>
#include <sisci_api.h>
#include <time.h>
#include <sched.h>

#include "sisci_glob_defs.h"
#include "put_request_region.h"

#include "slots.h"
#include "put_request_region_utils.h"
#include "super_fast_hash.h"
#include "2_phase_read_get.h"
#include "put_ack.h"

static sci_desc_t sd;

static volatile put_request_region_t *put_request_region;

static uint32_t free_header_slot = 0;
static slot_metadata_t *slots[PUT_REQUEST_BUCKETS];

static void put(const char *key, uint8_t key_len, void *value, uint32_t value_len, bool block_for_completion);

static enum replica_ack_type *replica_ack;
static put_ack_slot_t put_ack_slots[MAX_PUT_REQUEST_SLOTS];

static sci_local_segment_t put_ack_segment;
static sci_map_t put_ack_map;

int main(int argc, char* argv[]) {
    if (argc < REPLICA_COUNT + 1) {
        fprintf(stderr, "Usage: %s replica_id[0] ... replica_id[n]\n", argv[0]);
    }
    SEOE(SCIInitialize, NO_FLAGS);
    SEOE(SCIOpen, &sd, NO_FLAGS);

    init_bucket_desc();
    connect_to_put_request_region(sd, &put_request_region); //TODO: Reset some state if client reconnects?
    init_slots(slots, put_request_region);

    sci_error_t sci_error;
    SEOE(SCICreateSegment,
         sd,
         &put_ack_segment,
         PUT_ACK_SEGMENT_ID,
         MAX_PUT_REQUEST_SLOTS * sizeof(enum replica_ack_type) * REPLICA_COUNT,
         NO_CALLBACK,
         NO_ARG,
         NO_FLAGS
    );

    SEOE(SCIPrepareSegment,
         put_ack_segment,
         ADAPTER_NO,
         NO_FLAGS);

    SEOE(SCISetSegmentAvailable,
         put_ack_segment,
         ADAPTER_NO,
         NO_FLAGS);

    replica_ack = (enum replica_ack_type *) SCIMapLocalSegment(
            put_ack_segment,
            &put_ack_map,
            NO_OFFSET,
            MAX_PUT_REQUEST_SLOTS * sizeof(enum replica_ack_type) * REPLICA_COUNT,
            NO_SUGGESTED_ADDRESS,
            NO_FLAGS,
            &sci_error);

    if (sci_error != SCI_ERR_OK) {
        fprintf(stderr, "Could not map local segment: %s\n", SCIGetErrorString(sci_error));
        exit(EXIT_FAILURE);
    }

    uint8_t replica_node_ids[REPLICA_COUNT];

    for (uint8_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        char *endptr;
        long replica_node_id;
        replica_node_id = strtol(argv[1 + replica_index], &endptr, 10);
        if (replica_node_id > UINT8_MAX) {
            fprintf(stderr, "String not convertable to uint8!\n");
            exit(EXIT_FAILURE);
        }

        replica_node_ids[replica_index] = (uint8_t) replica_node_id;
    }

    init_2_phase_read_get(sd, replica_node_ids);
    put_request_region->status = ACTIVE;

    unsigned char sample_data[128];

    for (unsigned char i = 0; i < 128; i++) {
        sample_data[i] = i;
    }

    unsigned char sample_data2[128];

    for (unsigned char i = 0; i < 128; i++) {
        sample_data2[i] = i/2;
    }

    char key[] = "tall";
    char key2[] = "tall2";
    put(key, 4, sample_data, sizeof(sample_data), true);
    put(key2, 5, sample_data2, sizeof(sample_data2), true);
    put(key, 4, sample_data, sizeof(sample_data), true);

    struct timespec start, end;

    clock_gettime(CLOCK_MONOTONIC, &start);
    get_return_t *return_struct1 = get_2_phase_read(key2, 5);
    clock_gettime(CLOCK_MONOTONIC, &end);

    printf("At place 69 of get with data length %u: %u. Took: %ld\n", return_struct1->data_length, ((unsigned char *) return_struct1->data)[69], end.tv_nsec - start.tv_nsec);

    clock_gettime(CLOCK_MONOTONIC, &start);
    get_return_t *return_struct2 = get_2_phase_read(key, 4);
    clock_gettime(CLOCK_MONOTONIC, &end);

    printf("At place 69 of get with data_2 length %u: %u. Took %ld\n", return_struct2->data_length, ((unsigned char *) return_struct2->data)[69], end.tv_nsec - start.tv_nsec);

    free(return_struct1->data);
    free(return_struct2->data);
    free(return_struct1);
    free(return_struct2);

    // TODO: How to free the slots in buddy and in general
    while(1);

    SEOE(SCIClose, sd, NO_FLAGS);
    SCITerminate();

    return EXIT_SUCCESS;
}

static void put(const char *key, uint8_t key_len, void *value, uint32_t value_len, bool block_for_completion) {
    if (!block_for_completion) {
        fprintf(stderr, "NOT SUPPORTED!\n");
        exit(EXIT_FAILURE);
    }
    struct timespec start;
    struct timespec end;

    clock_gettime(CLOCK_MONOTONIC, &start);
    slot_metadata_t *slot = put_into_available_slot(slots, key, key_len, value, value_len);
    clock_gettime(CLOCK_MONOTONIC, &end);
    printf("on client took before shipping: %ld\n", end.tv_nsec - start.tv_nsec);

    uint32_t my_header_slot = free_header_slot;

    for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        *(replica_ack + (my_header_slot * REPLICA_COUNT) + replica_index) = REPLICA_NOT_ACKED;
    }

    put_ack_slot_t *put_ack_slot = &put_ack_slots[my_header_slot];
    clock_gettime(CLOCK_MONOTONIC, &put_ack_slot->start_time);
    put_ack_slot->metadata_slot = slot;

    put_request_region->header_slots[my_header_slot] = (size_t) slot->offset;
    free_header_slot = (free_header_slot + 1) % MAX_PUT_REQUEST_SLOTS;

    // I have also tried pthread cond wait without success
    printf("blocking for c\n");
    while (slot->status == SLOT_STATUS_PUT) {

        uint32_t ack_success_count = 0;
        uint32_t ack_count = 0;
        for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
            if (*(replica_ack + (my_header_slot * REPLICA_COUNT) + replica_index) != REPLICA_NOT_ACKED)
                ack_count++;

            if (*(replica_ack + (my_header_slot * REPLICA_COUNT) + replica_index) == REPLICA_ACK_SUCCESS)
                ack_success_count++;
        }

        // If we got a quorum of success acks, count as success
        if (ack_success_count > (REPLICA_COUNT + 1) / 2) {
            // Success!
            put_ack_slot->metadata_slot->status = SLOT_STATUS_SUCCESS;
            continue;
        }

        if (ack_count == REPLICA_COUNT) {
            // Check what errors we have gotten
            enum replica_ack_type replica_ack_type;
            bool mix = false;
            for (uint32_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
                if (replica_index == 0) {
                    replica_ack_type = *(replica_ack + (my_header_slot * REPLICA_COUNT) + replica_index);
                    continue;
                }
                if (*(replica_ack + (my_header_slot * REPLICA_COUNT) + replica_index) != replica_ack_type) {
                    mix = true;
                }
            }

            if (mix) {
                put_ack_slot->metadata_slot->status = SLOT_STATUS_ERROR_MIX;
                continue;
            }

            switch (replica_ack_type) {
                case REPLICA_ACK_ERROR_OUT_OF_SPACE:
                    put_ack_slot->metadata_slot->status = SLOT_STATUS_ERROR_OUT_OF_SPACE;
                case REPLICA_ACK_SUCCESS:
                case REPLICA_NOT_ACKED:
                default:
                    fprintf(stderr, "Illegal REPLICA_ACK type!\n");
                    exit(EXIT_FAILURE);
            }
        }

        // If we do not have error replies and not a quorum, check for timeout
        struct timespec end_p;
        clock_gettime(CLOCK_MONOTONIC, &end_p);

        if (end_p.tv_sec - put_ack_slot->start_time.tv_sec != 0 ||
            end_p.tv_nsec - put_ack_slot->start_time.tv_nsec >= PUT_TIMEOUT_NS) {
            put_ack_slot->metadata_slot->status = SLOT_STATUS_ERROR_TIMEOUT;
        }

        // Not timeout, let it live on!
    }

    switch (slot->status) {
        case SLOT_STATUS_SUCCESS:
            // Great success!
            break;
        case SLOT_STATUS_ERROR_OUT_OF_SPACE:
            printf("Put error: out of space\n");
            break;
        case SLOT_STATUS_ERROR_MIX:
            printf("Put error: multiple statuses/errors\n");
            break;
        case SLOT_STATUS_ERROR_TIMEOUT:
            printf("Put error: timeout\n");
            break;
        case SLOT_STATUS_FREE:
        case SLOT_STATUS_PUT:
        default:
            fprintf(stderr, "Illegal slot status!\n");
            exit(EXIT_FAILURE);
    }

    put_ack_slots[my_header_slot].metadata_slot->status = SLOT_STATUS_FREE;

    clock_gettime(CLOCK_MONOTONIC, &end);
    printf("    before completion: %ld\n", end.tv_nsec - start.tv_nsec);
}
