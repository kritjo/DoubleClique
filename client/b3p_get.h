#ifndef DOUBLECLIQUE_B3P_GET_H
#define DOUBLECLIQUE_B3P_GET_H

#define B3PGET_TIMEOUT_NS 10000000000

#include <stdint.h>
#include "main.h"
#include "put.h"

request_promise_t *get_b3p(const char *key, uint8_t key_len);
bool consume_get_ack_slot_phase1(ack_slot_t *ack_slot);
bool consume_get_ack_slot_phase2(ack_slot_t *ack_slot);

#endif //DOUBLECLIQUE_B3P_GET_H
