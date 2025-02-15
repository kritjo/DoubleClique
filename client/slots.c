#include <malloc.h>
#include <stdlib.h>
#include "slots.h"

slot_metadata_t *put_into_available_slot(slot_metadata_t **slots, const char *key, uint8_t key_len, void *value, uint32_t value_len) {
    slot_metadata_t *slot = find_available_slot(slots, key_len + value_len);
    if (slot == NULL) {
        fprintf(stderr, "UNHANDLED no available slots!\n");
        // TODO: handle this
    }
    put_into_slot(slot, key, key_len, value, value_len);
    return slot;
}

void init_slots(slot_metadata_t **slots, volatile put_request_region_t *put_request_region) {
    // First allocate space for the metadata slots
    for (uint32_t exp_index = 0; exp_index < PUT_REQUEST_BUCKETS; exp_index++) {
        uint32_t exp = MIN_SIZE_ELEMENT_EXP + exp_index;
        size_t slot_size = POWER_OF_TWO(exp);

        slots[exp_index] = malloc(COMPUTE_SLOT_COUNT(slot_size) * sizeof(slot_metadata_t));
        if (slots[exp_index] == NULL) {
            perror("malloc");
            exit(EXIT_FAILURE);
        }
    }

    // Then populate them with initial correct values
    for (uint32_t exp_index = 0; exp_index < PUT_REQUEST_BUCKETS; exp_index++) {
        uint32_t exp = MIN_SIZE_ELEMENT_EXP + exp_index;
        size_t slot_size = POWER_OF_TWO(exp);

        size_t slot_count = COMPUTE_SLOT_COUNT(slot_size);
        volatile char *start_of_bucket = ((volatile char *) put_request_region) + sizeof(put_request_region_t) + put_region_bucket_desc[exp_index].offset;

        for (uint32_t slot_index = 0; slot_index < slot_count; slot_index++) {
            size_t offset = slot_index * (slot_size + sizeof(put_request_slot_preamble_t));
            slots[exp_index][slot_index].ack_count = 0;
            slots[exp_index][slot_index].status = SLOT_STATUS_FREE;
            slots[exp_index][slot_index].total_payload_size = (uint32_t) slot_size;
            slots[exp_index][slot_index].slot_preamble = (volatile put_request_slot_preamble_t *) (start_of_bucket + offset);
            slots[exp_index][slot_index].offset = (ptrdiff_t) (put_region_bucket_desc[exp_index].offset + offset);
        }
    }
}

// Find an available metadata slot for a given payload size
static slot_metadata_t *find_available_slot(slot_metadata_t **slots, size_t slot_payload_size) {
    if (slot_payload_size > UINT32_MAX) {
        fprintf(stderr, "Too big slot_payload_size\n");
        exit(EXIT_FAILURE);
    }

    int exp = min_twos_complement_bits((uint32_t) slot_payload_size);
    uint32_t exp_index = (uint32_t) (exp - MIN_SIZE_ELEMENT_EXP);
    size_t slot_size = POWER_OF_TWO(exp);

    for (uint32_t i = 0; i < COMPUTE_SLOT_COUNT(slot_size); i++) {
        slot_metadata_t *slot = &(slots[exp_index][i]);
        if (slot->status == SLOT_STATUS_FREE) {
            return slot;
        }
    }

    return NULL;
}

// Put key and value into a slot. Both the key and value will be copied. The preamble must be free when entering.
static void put_into_slot(slot_metadata_t *slot,
                          const char *key,
                          uint8_t key_len,
                          void *value,
                          uint32_t value_len) {
    if (slot->status != SLOT_STATUS_FREE) {
        fprintf(stderr, "Slot got to be free to put_into_available_slot new value into it!\n");
        exit(EXIT_FAILURE);
    }

    if (slot->total_payload_size < key_len + value_len) {
        fprintf(stderr, "Tried to put_into_available_slot too large payload into slot!\n");
        exit(EXIT_FAILURE);
    }

    slot->slot_preamble->key_length = key_len;
    slot->slot_preamble->value_length = value_len;
    slot->slot_preamble->version_number = 0xdeadbeef; //TODO: this needs to be computed

    //Copy over the key
    for (size_t i = 0; i < key_len; i++) {
        // Just put_into_slot in some numbers in increasing order as a sanity check, should be easy to validate
        *(((volatile char *) slot->slot_preamble + sizeof(put_request_slot_preamble_t) + (i * sizeof(char)))) = key[i];
    }

    // Copy over the value
    for (uint32_t i = 0; i < value_len; i++) {
        *(((volatile char *) slot->slot_preamble + sizeof(put_request_slot_preamble_t) + key_len + i)) = ((char *)value)[i];
    }

    slot->status = SLOT_STATUS_PUT;
}
