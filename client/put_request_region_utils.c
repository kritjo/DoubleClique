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
