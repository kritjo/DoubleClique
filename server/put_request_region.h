#ifndef DOUBLECLIQUE_PUT_REQUEST_REGION_H
#define DOUBLECLIQUE_PUT_REQUEST_REGION_H

#include <sisci_types.h>
#include "put_request_region_protocol.h"

#define PUT_REQUEST_SEGMENT_SIZE 0x200000
#define PUT_REQUEST_SEGMENT_REGIONS ((int) sizeof(put_request_region_t)/PUT_REQUEST_SEGMENT_SIZE)

typedef struct {
    sci_desc_t sd;
    uint8_t replica_number;
    void *index_region;
    void *data_region;
} put_request_region_poller_thread_args_t;

int put_request_region_poller(void *arg);

#endif //DOUBLECLIQUE_PUT_REQUEST_REGION_H
