#ifndef DOUBLECLIQUE_REQUEST_REGION_CONNECTION_H
#define DOUBLECLIQUE_REQUEST_REGION_CONNECTION_H

#include "request_region.h"
#include <sisci_types.h>

extern volatile request_region_t *request_region;
extern sci_sequence_t request_sequence;

void connect_to_request_region(sci_desc_t sd);

#endif //DOUBLECLIQUE_REQUEST_REGION_CONNECTION_H
