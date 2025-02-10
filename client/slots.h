#ifndef DOUBLECLIQUE_SLOTS_H
#define DOUBLECLIQUE_SLOTS_H

#include <stddef.h>
#include <stdint.h>
#include "put_request_region.h"

typedef enum {
    SLOT_STATUS_FREE,
    SLOT_STATUS_PUT,
} slot_status_t;

struct slot_metadata {
    volatile put_request_slot_preamble_t *slot_preamble;
    ptrdiff_t offset;
    uint8_t ack_count;
    uint32_t total_payload_size; //TODO: really just a debug check field
    slot_status_t status;
};
typedef struct slot_metadata slot_metadata_t;

slot_metadata_t *put_into_available_slot(slot_metadata_t **slots, const char *key, uint8_t key_len, void *value, uint32_t value_len);
void init_slots(slot_metadata_t **slots, volatile put_request_region_t *put_request_region);

static slot_metadata_t *find_available_slot(slot_metadata_t **slots, size_t slot_payload_size);
static void put_into_slot(slot_metadata_t *slot, const char *key, uint8_t key_len, void *value, uint32_t value_len);

#endif //DOUBLECLIQUE_SLOTS_H
