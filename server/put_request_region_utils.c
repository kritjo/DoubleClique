#include <sisci_types.h>
#include <sisci_api.h>
#include <stdlib.h>

#include "put_request_region_utils.h"
#include "sisci_glob_defs.h"
#include "put_request_region.h"

void init_put_request_region(sci_desc_t sd, put_request_region_t **put_request_region) {
    sci_error_t sci_error;

    sci_local_segment_t put_request_segment_read;
    sci_map_t put_request_map_read;

    // Set up request region
    SEOE(SCICreateSegment,
         sd,
         &put_request_segment_read,
         PUT_REQUEST_SEGMENT_ID,
         put_region_size(),
         NO_CALLBACK,
         NO_ARG,
         SCI_FLAG_BROADCAST);

    SEOE(SCIPrepareSegment,
         put_request_segment_read,
         ADAPTER_NO,
         NO_FLAGS);

    SEOE(SCISetSegmentAvailable,
         put_request_segment_read,
         ADAPTER_NO,
         NO_FLAGS);

    *put_request_region = SCIMapLocalSegment(put_request_segment_read,
                                                       &put_request_map_read,
                                                       NO_OFFSET,
                                                       put_region_size(),
                                                       NO_SUGGESTED_ADDRESS,
                                                       NO_FLAGS,
                                                       &sci_error);

    if (sci_error != SCI_ERR_OK) {
        fprintf(stderr, "SCIMapLocalSegment failed: %s\n", SCIGetErrorString(sci_error));
        exit(EXIT_FAILURE);
    }
}
