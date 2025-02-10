#include "main.h"

#include <stdlib.h>
#include <sisci_api.h>

#include "sisci_glob_defs.h"
#include "put_request_region_protocol.h"
#include "index_data_protocol.h"

#include "slots.h"
#include "put.h"

static sci_desc_t sd;

static volatile put_request_region_t *put_request_region;

static uint32_t free_header_slot = 0;
static slot_metadata_t *slots[BUCKET_COUNT];

static void init_replica_data_index_region(uint8_t replica_index, uint8_t replica_node_id);
static void put(const char *key, uint8_t key_len, void *value, uint32_t value_len);

int main(int argc, char* argv[]) {
    if (argc < REPLICA_COUNT + 1) {
        fprintf(stderr, "Usage: %s replica_id[0] ... replica_id[n]\n", argv[0]);
    }
    SEOE(SCIInitialize, NO_FLAGS);
    SEOE(SCIOpen, &sd, NO_FLAGS);

    init_bucket_desc();
    init_put_request_region(sd, &put_request_region);
    init_slots(slots, put_request_region);

    for (uint8_t replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        char *endptr;
        long num;
        num = strtol(argv[1 + replica_index], &endptr, 10);
        if (num > UINT8_MAX) {
            fprintf(stderr, "String not convertable to uint8!\n");
            exit(EXIT_FAILURE);
        }

        init_replica_data_index_region(replica_index, (uint8_t) num);
    }

    init_put_ack_data_interrupt(sd, slots);
    put_request_region->status = ACTIVE;

    unsigned char sample_data[128];

    for (unsigned char i = 0; i < 128; i++) {
        sample_data[i] = i;
    }

    char key[] = "tall";

    put(key, 4, sample_data, sizeof(sample_data));

    // TODO: How to free the slots in buddy and in general
    while(1);

    SEOE(SCIClose, sd, NO_FLAGS);
    SCITerminate();

    return EXIT_SUCCESS;
}

static void init_replica_data_index_region(uint8_t replica_index, uint8_t replica_node_id) {
    sci_error_t sci_error;

    sci_remote_segment_t index_region_segments[REPLICA_COUNT];
    sci_map_t index_region_map[REPLICA_COUNT];
    volatile void *index_region_start[REPLICA_COUNT];

    sci_remote_segment_t data_region_segments[REPLICA_COUNT];
    sci_map_t data_region_map[REPLICA_COUNT];
    volatile void *data_region_start[REPLICA_COUNT];

    SEOE(SCIConnectSegment,
         sd,
         &index_region_segments[replica_index],
         replica_node_id,
         replica_index_segment_id[replica_index],
         ADAPTER_NO,
         NO_CALLBACK,
         NO_ARG,
         SCI_INFINITE_TIMEOUT,
         NO_FLAGS);

    index_region_start[replica_index] = SCIMapRemoteSegment(index_region_segments[replica_index],
                                                            &index_region_map[replica_index],
                                                            NO_OFFSET,
                                                            INDEX_REGION_SIZE,
                                                            NO_SUGGESTED_ADDRESS,
                                                            NO_FLAGS,
                                                            &sci_error);

    if (sci_error != SCI_ERR_OK) {
        fprintf(stderr, "SCIMapLocalSegment failed: %s\n", SCIGetErrorString(sci_error));
        exit(EXIT_FAILURE);
    }

    SEOE(SCIConnectSegment,
         sd,
         &data_region_segments[replica_index],
         replica_node_id,
         replica_data_segment_id[replica_index],
         ADAPTER_NO,
         NO_CALLBACK,
         NO_ARG,
         SCI_INFINITE_TIMEOUT,
         NO_FLAGS);

    data_region_start[replica_index] = SCIMapRemoteSegment(data_region_segments[replica_index],
                                                           &data_region_map[replica_index],
                                                           NO_OFFSET,
                                                           DATA_REGION_SIZE,
                                                           NO_SUGGESTED_ADDRESS,
                                                           NO_FLAGS,
                                                           &sci_error);

    if (sci_error != SCI_ERR_OK) {
        fprintf(stderr, "SCIMapLocalSegment failed: %s\n", SCIGetErrorString(sci_error));
        exit(EXIT_FAILURE);
    }
}

static void put(const char *key, uint8_t key_len, void *value, uint32_t value_len) {
    slot_metadata_t *slot = put_into_available_slot(slots, key, key_len, value, value_len);
    put_request_region->header_slots[free_header_slot] = (size_t) slot->offset;
    free_header_slot = (free_header_slot + 1) % MAX_PUT_REQUEST_SLOTS;
}
