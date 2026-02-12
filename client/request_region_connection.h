#ifndef DOUBLECLIQUE_REQUEST_REGION_CONNECTION_H
#define DOUBLECLIQUE_REQUEST_REGION_CONNECTION_H

#include "request_region.h"
#include <sisci_types.h>

extern volatile request_region_t *request_region;
extern sci_sequence_t request_sequence;
extern sci_map_t request_map;

void connect_to_request_region(sci_desc_t sd);
void send_request_region_slot(
    volatile header_slot_t *slot,
    uint8_t key_length,
    uint32_t value_length,
    uint32_t version_number,
    size_t offset,
    size_t return_offset,
    uint8_t replica_write_back_hint,
    uint32_t payload_hash,
    enum header_slot_status status
);
#endif //DOUBLECLIQUE_REQUEST_REGION_CONNECTION_H
