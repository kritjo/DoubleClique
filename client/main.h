#ifndef DOUBLECLIQUE_MAIN_H
#define DOUBLECLIQUE_MAIN_H

#include <stdint.h>
#include <sisci_types.h>
#include "put_request_region_protocol.h"

struct slot_metadata {
    volatile put_request_slot_preamble_t *slot_preamble;
    ptrdiff_t offset;
    uint8_t ack_count;
    uint32_t total_payload_size; //TODO: really just a debug check field
    put_request_slot_status_t status;
};
typedef struct slot_metadata slot_metadata_t;

int main(int argc, char* argv[]);
void put_into_slot(slot_metadata_t *slot, const char *key, uint8_t key_len, void *value, uint32_t value_len);
slot_metadata_t *find_available_slot(size_t slot_payload_size); // TODO: Is the pointer return needed, or can we simply return the struct if it actually is copied
sci_callback_action_t put_ack(void *arg, sci_local_data_interrupt_t interrupt, void *data, unsigned int length, sci_error_t status);

#endif //DOUBLECLIQUE_MAIN_H
