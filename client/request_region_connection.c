#include "request_region_connection.h"

#include <sisci_types.h>
#include <sisci_api.h>
#include <stdlib.h>
#include "sisci_glob_defs.h"
#include "request_region.h"
#include "get_node_id.h"

volatile request_region_t *request_region;
sci_sequence_t request_sequence;
sci_map_t request_map;

static uint8_t client_id;

void connect_to_request_region(sci_desc_t sd) {
    sci_error_t sci_error;

    sci_remote_segment_t put_request_segment;

    SEOE(SCIConnectSegment,
         sd,
         &put_request_segment,
         DIS_BROADCAST_NODEID_GROUP_ALL,
         REQUEST_SEGMENT_ID,
         ADAPTER_NO,
         NO_CALLBACK,
         NO_ARG,
         SCI_INFINITE_TIMEOUT,
         SCI_FLAG_BROADCAST);

    request_region = (volatile request_region_t*) SCIMapRemoteSegment(put_request_segment,
                                                                      &request_map,
                                                                      NO_OFFSET,
                                                                      REQUEST_REGION_SIZE,
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
    client_id = (uint8_t) node_id;
    request_region->sisci_node_id = client_id;

    for (uint32_t i = 0; i < MAX_REQUEST_SLOTS; i++) {
        request_region->header_slots[i].status = HEADER_SLOT_UNUSED;
    }

    SEOE(SCICreateMapSequence,
         request_map,
         &request_sequence,
         NO_FLAGS);

    sci_error_t error;
    sci_sequence_status_t status;

    status = SCIStartSequence(request_sequence, NO_FLAGS, &error);
    if (error != SCI_ERR_OK) {
        fprintf(stderr, "SCIStartSequence returned non SCI_ERR_OK, which should not be possible: %s\n", SCIGetErrorString(error));
        exit(EXIT_FAILURE);
    }
    if (status != SCI_SEQ_OK) {
        fprintf(stderr, "SCIStartSequence returned non SCI_SEQ_OK: %d\n", status);
        exit(EXIT_FAILURE);
    }

    request_region->status = PUT_REQUEST_REGION_ACTIVE;
}
