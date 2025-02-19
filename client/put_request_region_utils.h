#ifndef DOUBLECLIQUE_PUT_REQUEST_REGION_UTILS_H
#define DOUBLECLIQUE_PUT_REQUEST_REGION_UTILS_H

#include <stdint.h>
#include <stddef.h>
#include "slots.h"

void connect_to_put_request_region(sci_desc_t sd, volatile put_request_region_t **put_request_region);

#endif //DOUBLECLIQUE_PUT_REQUEST_REGION_UTILS_H
