#ifndef DOUBLECLIQUE_REQUEST_REGION_THREAD_H
#define DOUBLECLIQUE_REQUEST_REGION_THREAD_H

#include <sisci_types.h>
#include "request_region.h"

typedef struct {
    sci_desc_t sd;
    uint8_t replica_number;
    void *index_region;
    void *data_region;
} request_region_poller_thread_args_t;

int request_region_poller(void *arg);

#endif //DOUBLECLIQUE_REQUEST_REGION_THREAD_H
