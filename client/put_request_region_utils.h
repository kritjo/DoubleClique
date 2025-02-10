#ifndef DOUBLECLIQUE_PUT_REQUEST_REGION_UTILS_H
#define DOUBLECLIQUE_PUT_REQUEST_REGION_UTILS_H

#include <stdint.h>
#include <stddef.h>
#include "slots.h"

void connect_to_put_request_region(sci_desc_t sd, volatile put_request_region_t **put_request_region);
void create_put_ack_data_interrupt(sci_desc_t sd, slot_metadata_t **slots);
void init_replica_data_index_region(sci_desc_t sd, uint8_t replica_index, uint8_t replica_node_id);

static sci_callback_action_t put_ack_callback(void *arg, sci_local_data_interrupt_t interrupt, void *data, unsigned int length, sci_error_t status);

#endif //DOUBLECLIQUE_PUT_REQUEST_REGION_UTILS_H
