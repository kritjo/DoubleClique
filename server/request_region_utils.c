#include <sisci_types.h>
#include <sisci_api.h>
#include <stdlib.h>

#include "request_region_utils.h"
#include "sisci_glob_defs.h"
#include "request_region.h"

void init_request_region(sci_desc_t sd, request_region_t **request_region) {
    sci_error_t sci_error;

    sci_local_segment_t request_segment_read;
    sci_map_t request_map_read;

    // Set up request region
    SEOE(SCICreateSegment,
         sd,
         &request_segment_read,
         REQUEST_SEGMENT_ID,
         REQUEST_REGION_SIZE,
         NO_CALLBACK,
         NO_ARG,
         SCI_FLAG_BROADCAST);

    SEOE(SCIPrepareSegment,
         request_segment_read,
         ADAPTER_NO,
         NO_FLAGS);

    SEOE(SCISetSegmentAvailable,
         request_segment_read,
         ADAPTER_NO,
         NO_FLAGS);

    *request_region = SCIMapLocalSegment(request_segment_read,
                                         &request_map_read,
                                         NO_OFFSET,
                                         REQUEST_REGION_SIZE,
                                         NO_SUGGESTED_ADDRESS,
                                         NO_FLAGS,
                                         &sci_error);

    if (sci_error != SCI_ERR_OK) {
        fprintf(stderr, "SCIMapLocalSegment failed: %s\n", SCIGetErrorString(sci_error));
        exit(EXIT_FAILURE);
    }
}
