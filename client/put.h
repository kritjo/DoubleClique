#ifndef DOUBLECLIQUE_PUT_H
#define DOUBLECLIQUE_PUT_H

#include <stdint.h>
#include <stddef.h>
#include "slots.h"

void connect_to_put_request_region(sci_desc_t sd, volatile put_request_region_t **put_request_region);
void create_put_ack_data_interrupt(sci_desc_t sd, slot_metadata_t **slots);

static sci_callback_action_t put_ack_callback(void *arg, sci_local_data_interrupt_t interrupt, void *data, unsigned int length, sci_error_t status);

#endif //DOUBLECLIQUE_PUT_H
