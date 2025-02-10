#include <malloc.h>
#include <stdlib.h>
#include "slots.h"

void init_slots(slot_metadata_t **slots, volatile put_request_region_t *put_request_region) {
    for (uint32_t exp_index = 0; exp_index < BUCKET_COUNT; exp_index++) {
        uint32_t exp = MIN_SIZE_ELEMENT_EXP + exp_index;
        size_t slot_size = POWER_OF_TWO(exp);

        slots[exp_index] = malloc(COMPUTE_SLOT_COUNT(slot_size) * sizeof(slot_metadata_t));
        if (slots[exp_index] == NULL) {
            perror("malloc");
            exit(EXIT_FAILURE);
        }
    }

    for (uint32_t exp_index = 0; exp_index < BUCKET_COUNT; exp_index++) {
        uint32_t exp = MIN_SIZE_ELEMENT_EXP + exp_index;
        size_t slot_size = POWER_OF_TWO(exp);

        size_t slot_count = COMPUTE_SLOT_COUNT(slot_size);
        volatile char *start_of_bucket = ((volatile char *) put_request_region) + sizeof(put_request_region_t) + put_region_bucket_desc[exp_index].offset;

        for (uint32_t slot_index = 0; slot_index < slot_count; slot_index++) {
            size_t offset = slot_index * (slot_size + sizeof(put_request_slot_preamble_t));
            slots[exp_index][slot_index].ack_count = 0;
            slots[exp_index][slot_index].status = FREE;
            slots[exp_index][slot_index].total_payload_size = (uint32_t) slot_size;
            slots[exp_index][slot_index].slot_preamble = (volatile put_request_slot_preamble_t *) (start_of_bucket + offset);
            slots[exp_index][slot_index].offset = (ptrdiff_t) (put_region_bucket_desc[exp_index].offset + offset);

            slots[exp_index][slot_index].slot_preamble->status = FREE;
        }
    }
}
