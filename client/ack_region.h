#ifndef DOUBLECLIQUE_ACK_REGION_H
#define DOUBLECLIQUE_ACK_REGION_H

#include <stdint.h>
#include "request_region.h"
#include "put.h"

extern volatile _Atomic uint32_t free_header_slot;
extern volatile _Atomic uint32_t oldest_header_slot;

extern volatile _Atomic uint32_t free_ack_data;
extern volatile _Atomic uint32_t oldest_ack_data;

extern replica_ack_t *replica_ack;
extern ack_slot_t ack_slots[MAX_REQUEST_SLOTS];

extern sci_local_segment_t ack_segment;
extern sci_map_t ack_map;

void init_ack_region(sci_desc_t sd);
ack_slot_t *get_ack_slot_blocking(enum request_type request_type, uint8_t key_len, uint32_t value_len, uint32_t version_number);

void *ack_thread(__attribute__((unused)) void *_args);

#endif //DOUBLECLIQUE_ACK_REGION_H
