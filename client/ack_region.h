#ifndef DOUBLECLIQUE_ACK_REGION_H
#define DOUBLECLIQUE_ACK_REGION_H

#include <stdint.h>
#include "request_region.h"
#include "put.h"

extern volatile _Atomic uint32_t free_header_slot;
extern volatile _Atomic uint32_t oldest_header_slot;

extern replica_ack_t *replica_ack;
extern ack_slot_t ack_slots[MAX_REQUEST_SLOTS];

extern sci_local_segment_t ack_segment;
extern sci_map_t ack_map;

void init_ack_region(sci_desc_t sd);

#endif //DOUBLECLIQUE_ACK_REGION_H
