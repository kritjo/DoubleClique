#ifndef DOUBLECLIQUE_PUT_H
#define DOUBLECLIQUE_PUT_H

#include <time.h>
#include "sisci_glob_defs.h"
#include "request_region.h"
#include "ack_region.h"

#define PUT_TIMEOUT_NS 1000000000 //TODO: This should probably be a factor of queue length

void init_put(sci_desc_t sd);
request_promise_t *put_blocking_until_available_put_request_region_slot(const char *key, uint8_t key_len, void *value, uint32_t value_len);
bool consume_put_ack_slot(ack_slot_t *ack_slot);

#endif //DOUBLECLIQUE_PUT_H
