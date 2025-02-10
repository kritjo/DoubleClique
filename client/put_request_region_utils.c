#include <stdlib.h>
#include <sisci_types.h>
#include <sisci_api.h>
#include "put_request_region_utils.h"
#include "get_node_id.h"
#include "slots.h"
#include "index_data_protocol.h"

void connect_to_put_request_region(sci_desc_t sd, volatile put_request_region_t **put_request_region) {
    sci_error_t sci_error;

    sci_remote_segment_t put_request_segment;
    sci_map_t put_request_map;

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

    *put_request_region = (volatile put_request_region_t*) SCIMapRemoteSegment(put_request_segment,
                                                                              &put_request_map,
                                                                              NO_OFFSET,
                                                                              put_region_size(),
                                                                              NO_SUGGESTED_ADDRESS,
                                                                              NO_FLAGS,
                                                                              &sci_error);

    if (sci_error != SCI_ERR_OK) {
        fprintf(stderr, "SCIMapLocalSegment failed: %s\n", SCIGetErrorString(sci_error));
        exit(EXIT_FAILURE);
    }

    unsigned int node_id = get_node_id();
    if (node_id > UINT8_MAX) {
        fprintf(stderr, "node_id too large!\n");
        exit(EXIT_FAILURE);
    }
    (*put_request_region)->sisci_node_id = (uint8_t) node_id;

    for (uint32_t i = 0; i < MAX_PUT_REQUEST_SLOTS; i++) {
        (*put_request_region)->header_slots[i] = 0;
    }
}

void create_put_ack_data_interrupt(sci_desc_t sd, slot_metadata_t **slots) {
    sci_local_data_interrupt_t ack_data_interrupt;

    uint ack_interrupt_no = ACK_DATA_INTERRUPT_NO;

    SEOE(SCICreateDataInterrupt,
         sd,
         &ack_data_interrupt,
         ADAPTER_NO,
         &ack_interrupt_no,
         put_ack_callback,
         slots,
         SCI_FLAG_USE_CALLBACK | SCI_FLAG_FIXED_INTNO);
}

static sci_callback_action_t put_ack_callback(void *arg, sci_local_data_interrupt_t interrupt, void *data, unsigned int length, sci_error_t status) {
    slot_metadata_t **slots = (slot_metadata_t **) arg;

    if (status != SCI_ERR_OK) {
        fprintf(stderr, "Received error SCI status from delivery: %s\n", SCIGetErrorString(status));
        exit(EXIT_FAILURE);
    }

    if (length != sizeof(put_ack_t)) {
        fprintf(stderr, "Received invalid length %d from delivery\n", length);
        exit(EXIT_FAILURE);
    }

    put_ack_t *put_ack_data = (put_ack_t *) data;
    slot_metadata_t *slot_metadata = &slots[put_ack_data->bucket_no][put_ack_data->slot_no];
    uint8_t ack_count = ++slot_metadata->ack_count;

    if (ack_count < REPLICA_COUNT)
        return SCI_CALLBACK_CONTINUE;
    else if (ack_count > REPLICA_COUNT) {
        fprintf(stderr, "Got more acks than there are replicas, should not be possible\n");
        exit(EXIT_FAILURE);
    }
    // Got same amount of acks as there are replicas, we must make the slot available again
    // We do not need this, as the replicas actually put_into_available_slot this for us: slot_metadata->slot_preamble->status = SLOT_STATUS_FREE;

    slot_metadata->ack_count = 0;
    slot_metadata->status = SLOT_STATUS_FREE;

    return SCI_CALLBACK_CONTINUE;
}

void init_replica_data_index_region(sci_desc_t sd, uint8_t replica_index, uint8_t replica_node_id) {
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
