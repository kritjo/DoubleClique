#ifndef DOUBLECLIQUE_SLOTS_H
#define DOUBLECLIQUE_SLOTS_H

#include <stddef.h>
#include <stdint.h>
#include "put_request_region_protocol.h"

struct slot_metadata {
    volatile put_request_slot_preamble_t *slot_preamble;
    ptrdiff_t offset;
    uint8_t ack_count;
    uint32_t total_payload_size; //TODO: really just a debug check field
    put_request_slot_status_t status;
};
typedef struct slot_metadata slot_metadata_t;

void init_slots(slot_metadata_t **slots, volatile put_request_region_t *put_request_region);

#endif //DOUBLECLIQUE_SLOTS_H
