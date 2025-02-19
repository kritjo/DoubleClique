#ifndef DOUBLECLIQUE_PUT_ACK_H
#define DOUBLECLIQUE_PUT_ACK_H

#include "sisci_glob_defs.h"
#include "slots.h"

#define PUT_ACK_SEGMENT_ID 2
#define PUT_TIMEOUT_NS 100000000

typedef struct {
    struct timespec start_time;
    slot_metadata_t *metadata_slot;
} put_ack_slot_t;

#endif //DOUBLECLIQUE_PUT_ACK_H
