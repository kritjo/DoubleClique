#ifndef DOUBLECLIQUE_PUT_ACK_H
#define DOUBLECLIQUE_PUT_ACK_H

#include "sisci_glob_defs.h"
#include "slots.h"

#define PUT_ACK_SEGMENT_ID 2
#define PUT_TIMEOUT_NS 100000000

enum feedback_type {
    FEEDBACK_TYPE_SYNC,
    FEEDBACK_TYPE_ASYNC //TODO: NOT IMPLEMENTED - need callback system
};

typedef struct {
    bool header_slot_available;
    struct timespec start_time;
    slot_metadata_t *metadata_slot;
    enum feedback_type feedback_type;
} put_ack_slot_t;

void init_put_ack_region(sci_desc_t sd);
void block_for_available(uint32_t header_slot);
void register_new_put(slot_metadata_t *metadata_slot, uint32_t header_slot, enum feedback_type feedback_type);
void *put_ack_thread_start(__attribute__((unused)) void *_arg);
void client_ack_self(uint32_t header_slot);


//extern _Atomic(enum put_ack_status) *put_ack_status;

#endif //DOUBLECLIQUE_PUT_ACK_H
