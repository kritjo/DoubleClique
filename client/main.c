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

static sci_desc_t sd;

static volatile put_request_region_t *put_request_region;

static uint32_t free_header_slot = 0;
static slot_metadata_t *slots[PUT_REQUEST_BUCKETS];

static void put(const char *key, uint8_t key_len, void *value, uint32_t value_len, bool block_for_completion);


int main(int argc, char* argv[]) {
    if (argc < REPLICA_COUNT + 1) {
        fprintf(stderr, "Usage: %s replica_id[0] ... replica_id[n]\n", argv[0]);
    }
    SEOE(SCIInitialize, NO_FLAGS);
    SEOE(SCIOpen, &sd, NO_FLAGS);

    init_bucket_desc();
    connect_to_put_request_region(sd, &put_request_region); //TODO: Reset some state if client reconnects?
    init_slots(slots, put_request_region);

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
    create_put_ack_data_interrupt(sd, slots);
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

    get_return_t *return_struct1 = get_2_phase_read(key2, 5);
    get_return_t *return_struct2 = get_2_phase_read(key, 4);

    printf("At place 69 of get with data length %u: %u\n", return_struct1->data_length, ((unsigned char *) return_struct1->data)[69]);
    printf("At place 69 of get with data_2 length %u: %u\n", return_struct2->data_length, ((unsigned char *) return_struct2->data)[69]);

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
    struct timespec start;
    struct timespec end;

    clock_gettime(CLOCK_MONOTONIC, &start);
    slot_metadata_t *slot = put_into_available_slot(slots, key, key_len, value, value_len);
    clock_gettime(CLOCK_MONOTONIC, &end);
    printf("on client took before shipping: %ld\n", end.tv_nsec - start.tv_nsec);

    put_request_region->header_slots[free_header_slot] = (size_t) slot->offset;
    free_header_slot = (free_header_slot + 1) % MAX_PUT_REQUEST_SLOTS;

    // I have also tried pthread cond wait without success
    if (block_for_completion) while (slot->status == SLOT_STATUS_PUT) sched_yield();

    clock_gettime(CLOCK_MONOTONIC, &end);
    printf("    before completion: %ld\n", end.tv_nsec - start.tv_nsec);
}
