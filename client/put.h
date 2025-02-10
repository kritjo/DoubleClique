#ifndef DOUBLECLIQUE_PUT_H
#define DOUBLECLIQUE_PUT_H

#include <stdint.h>
#include <stddef.h>
#include "slots.h"

slot_metadata_t *put_into_available_slot(slot_metadata_t **slots, const char *key, uint8_t key_len, void *value, uint32_t value_len);
void init_put_request_region(sci_desc_t sd, volatile put_request_region_t **put_request_region);
void init_put_ack_data_interrupt(sci_desc_t sd, slot_metadata_t **slots);

static slot_metadata_t *find_available_slot(slot_metadata_t **slots, size_t slot_payload_size);
static void put_into_slot(slot_metadata_t *slot, const char *key, uint8_t key_len, void *value, uint32_t value_len);
static sci_callback_action_t put_ack(void *arg, sci_local_data_interrupt_t interrupt, void *data, unsigned int length, sci_error_t status);

#endif //DOUBLECLIQUE_PUT_H
