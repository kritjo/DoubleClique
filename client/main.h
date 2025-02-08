#ifndef DOUBLECLIQUE_MAIN_H
#define DOUBLECLIQUE_MAIN_H

#include <stdint.h>
#include <sisci_types.h>
#include "put_request_region_protocol.h"

int main(int argc, char* argv[]);
void put_into_slot(uint32_t slot_no, const char *key, uint8_t key_len, void *value, uint32_t value_len);
uint32_t allocate_new_slot(size_t key_size, size_t value_size);
sci_callback_action_t put_ack(void *arg, sci_local_data_interrupt_t interrupt, void *data, unsigned int length, sci_error_t status);

struct slot_metadata{
    size_t total_payload_size;
    volatile put_request_slot_preamble_t *slot_preamble;
    uint8_t ack_count;
    put_request_slot_status_t status;
};
typedef struct slot_metadata slot_metadata_t;

#endif //DOUBLECLIQUE_MAIN_H
