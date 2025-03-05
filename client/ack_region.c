#include <sisci_api.h>
#include <stdlib.h>
#include "ack_region.h"
#include "index_data_protocol.h"
#include "request_region_connection.h"

volatile _Atomic uint32_t free_header_slot = 0;
volatile _Atomic uint32_t oldest_header_slot = 0;

volatile _Atomic uint32_t free_ack_data = 0;
volatile _Atomic uint32_t oldest_ack_data = 0;

replica_ack_t *replica_ack;
ack_slot_t ack_slots[MAX_REQUEST_SLOTS];

sci_local_segment_t ack_segment;
sci_map_t ack_map;

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
}

// Critical region function
ack_slot_t *get_ack_slot(enum request_promise_status initial_result, uint8_t key_len, uint32_t value_len, uint32_t version_number) {
    while ((free_header_slot + 1) % MAX_REQUEST_SLOTS == oldest_header_slot); // This complains but I think using atomics should work TODO: try removing volatile

    ack_slot_t *ack_slot = &ack_slots[free_header_slot];
    ack_slot->header_slot_WRITE_ONLY = &request_region->header_slots[free_header_slot];
    free_header_slot = (free_header_slot + 1) % MAX_REQUEST_SLOTS;

    clock_gettime(CLOCK_MONOTONIC, &ack_slot->start_time);

    request_promise_t *promise = malloc(sizeof(request_promise_t));
    if (promise == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    promise->result = initial_result;

    ack_slot->promise = promise;
    ack_slot->key_len = key_len;
    ack_slot->value_len = value_len;
    ack_slot->version_number = version_number;

    return ack_slot;
}
