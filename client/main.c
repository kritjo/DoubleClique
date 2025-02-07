#include "main.h"

#include <stdlib.h>
#include <sisci_api.h>

#include "sisci_glob_defs.h"
#include "put_request_region_protocol.h"
#include "index_data_protocol.h"

#define BUDDY_ALLOC_IMPLEMENTATION
#include "buddy_alloc.h"


int main(int argc, char* argv[]) {
    sci_desc_t sd;
    sci_error_t sci_error;
    u_int8_t replica_node_ids[REPLICA_COUNT];

    sci_remote_segment_t put_request_segment;
    sci_map_t put_request_map;
    volatile put_request_region_t *put_request_region;

    sci_remote_segment_t index_region_segments[REPLICA_COUNT];
    sci_map_t index_region_map[REPLICA_COUNT];
    volatile void *index_region_start[REPLICA_COUNT];

    sci_remote_segment_t data_region_segments[REPLICA_COUNT];
    sci_map_t data_region_map[REPLICA_COUNT];
    volatile void *data_region_start[REPLICA_COUNT];

    if (argc < REPLICA_COUNT + 1) {
        fprintf(stderr, "Usage: %s replica_id[0] ... replica_id[n]\n", argv[0]);
    }
    SEOE(SCIInitialize, NO_FLAGS);
    SEOE(SCIOpen, &sd, NO_FLAGS);

    SEOE(SCIConnectSegment,
         sd,
         &put_request_segment,
         DIS_BROADCAST_NODEID_GROUP_ALL,
         PUT_REQUEST_SEGMENT_ID,
         ADAPTER_NO,
         NO_CALLBACK,
         NO_ARG,
         SCI_INFINITE_TIMEOUT,
         SCI_FLAG_BROADCAST);

    printf("Connected to put broadcast segment\n");

    printf("sizeof(put_request_region_t): %ld\n", sizeof(put_request_region_t));
    put_request_region = (volatile put_request_region_t*) SCIMapRemoteSegment(put_request_segment,
                                                                              &put_request_map,
                                                                              NO_OFFSET,
                                                                              sizeof(put_request_region_t),
                                                                              NO_SUGGESTED_ADDRESS,
                                                                              NO_FLAGS,
                                                                              &sci_error);

    if (sci_error != SCI_ERR_OK) {
        fprintf(stderr, "SCIMapLocalSegment failed: %s\n", SCIGetErrorString(sci_error));
        exit(EXIT_FAILURE);
    }

    printf("Mapped broadcast segment\n");

    for (int replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        const char *replica_str_node_id = argv[1 + replica_index];
        char *endptr;
        long num;
        num = strtol(replica_str_node_id, &endptr, 10);
        if (num > UINT8_MAX) {
            fprintf(stderr, "Node id too high!\n");
            exit(EXIT_FAILURE);
        }
        replica_node_ids[replica_index] = (uint8_t) num;

        printf("Handling replica %d with node id %d\n", replica_index, replica_node_ids[replica_index]);
        SEOE(SCIConnectSegment,
             sd,
             &index_region_segments[replica_index],
             replica_node_ids[replica_index],
             replica_index_segment_id[replica_index],
             ADAPTER_NO,
             NO_CALLBACK,
             NO_ARG,
             SCI_INFINITE_TIMEOUT,
             NO_FLAGS);

        printf("Connected to index segment\n");

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

        printf("Mapped to index segment\n");

        SEOE(SCIConnectSegment,
             sd,
             &data_region_segments[replica_index],
             replica_node_ids[replica_index],
             replica_data_segment_id[replica_index],
             ADAPTER_NO,
             NO_CALLBACK,
             NO_ARG,
             SCI_INFINITE_TIMEOUT,
             NO_FLAGS);


        printf("Connected to put data segment\n");

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

        printf("Mapped to data segment\n");
    }

    // Just to test we can try to allocate a single put region

    size_t arena_size = PUT_REQUEST_REGION_SIZE;
    void *buddy_metadata = malloc(buddy_sizeof(arena_size));
    struct volatile_buddy *buddy = volatile_buddy_init(buddy_metadata, put_request_region->units, arena_size);

    size_t sample_size = 2048 + sizeof(put_request_slot_preamble_t);
    volatile void *data = volatile_buddy_malloc(buddy, sample_size);

    put_request_region->status = LOCKED;
    put_request_region->slots_used = 1;
    ptrdiff_t offset = ((volatile uint8_t *) data) - put_request_region->units;
    put_request_region->slot_offset_starts[0] = offset;

    // Load with some sample data, starting with the preamble -- TODO: we should only broadcast using memcpy with a single transaction
    volatile put_request_slot_preamble_t *sample_preamble = (volatile put_request_slot_preamble_t *) data;
    sample_preamble->status = FREE;

    for (int replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        sample_preamble->replica_ack[replica_index] = 0;
    }

    const char key[] = "Hei";
    size_t key_len = strlen(key);
    sample_preamble->key_length = (uint8_t) key_len;

    size_t value_len = sample_size - sizeof(put_request_slot_preamble_t);
    printf("value_len: %ld\n", value_len);
    if (value_len > UINT32_MAX) {
        fprintf(stderr, "Value can at most be of size %ld\n", value_len);
        exit(EXIT_FAILURE);
    }

    sample_preamble->value_length = (uint32_t) value_len;

    //Copy over the key
    for (size_t i = 0; i < key_len; i++) {
        // Just put in some numbers in increasing order as a sanity check, should be easy to validate
        *((volatile char *) (put_request_region->units + offset + sizeof(put_request_slot_preamble_t) + (i * sizeof(char)))) = key[i];
    }

    for (size_t i = 0; i < value_len / sizeof(i); i++) {
        // Just put in some numbers in increasing order as a sanity check, should be easy to validate
        *((volatile size_t *) (put_request_region->units + offset + sizeof(put_request_slot_preamble_t) + key_len + (i * sizeof(i)))) = i;
    }

    sample_preamble->status = PUT;

    put_request_region->status = WALKABLE;

    printf("Put bytes in place\n");

    while(1); // TODO: Should get an ack as the next step. then we need to make the library interface

    free(buddy_metadata);

    SEOE(SCIClose, sd, NO_FLAGS);
    SCITerminate();

    return EXIT_SUCCESS;
}
