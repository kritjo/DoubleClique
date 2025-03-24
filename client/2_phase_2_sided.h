#ifndef DOUBLECLIQUE_2_PHASE_2_SIDED_H
#define DOUBLECLIQUE_2_PHASE_2_SIDED_H

#define GET_TIMEOUT_2_SIDED_NS 10000000000

#include <stdint.h>
#include "main.h"
#include "put.h"

void init_2_phase_2_sided_get(void);
request_promise_t *get_2_phase_2_sided(const char *key, uint8_t key_len);
bool consume_get_ack_slot_phase1(ack_slot_t *ack_slot);
bool consume_get_ack_slot_phase2(ack_slot_t *ack_slot);
void send_phase_2_get(uint32_t version_number, uint32_t replica_index, uint8_t key_len, uint32_t value_len, ptrdiff_t server_data_offset, request_promise_t *promise);
void get_2_sided_decrement(void);

#endif //DOUBLECLIQUE_2_PHASE_2_SIDED_H
