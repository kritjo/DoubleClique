#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include "index_data_protocol.h"

// Return an existing index slot in the index region for the particular key, NULL if it does not exist
index_entry_t *existing_slot_for_key(void *index_region, void *data_region, uint32_t key_hash, uint32_t key_length, char *key) {
    for (uint8_t slot = 0; slot < INDEX_SLOTS_PR_BUCKET; slot++) {
        index_entry_t *index_slot = (index_entry_t *) GET_SLOT_POINTER((char *) index_region, key_hash % INDEX_BUCKETS, slot);

        // If unused obv not update
        if (index_slot->status == 0) continue;

        // Get the existing data slot for the index_slot
        data_entry_preamble_t *existing_data_slot = (data_entry_preamble_t *) ((char *) data_region + index_slot->offset);

        // Check if we find the same key
        if (existing_data_slot->key_length != key_length) continue;
        char *existing_key = (char *) existing_data_slot + sizeof(data_entry_preamble_t);
        if (strncmp(existing_key, key, existing_data_slot->key_length) != 0) continue;

        // We found a slot for the same key
        return index_slot;
    }

    return NULL;
}

// Returns any index slot with status 0 from the index region given a key_hash
index_entry_t *find_available_index_slot(void *index_region, uint32_t key_hash) {
    for (uint8_t slot = 0; slot < INDEX_SLOTS_PR_BUCKET; slot++) {
        index_entry_t *index_slot = (index_entry_t *) GET_SLOT_POINTER((char *) index_region, key_hash % INDEX_BUCKETS, slot);
        if (index_slot->status == 0) return index_slot;
    }

    return NULL;
}

// Returns a data slot for an index slot. The try_to_use_existing_data_slot parameter specifies whether we should try to
// use the existing data_slot pointed to by the index slot.
data_entry_preamble_t *find_data_slot_for_index_slot(void *data_region, index_entry_t *index_slot, bool try_to_use_existing_data_slot, uint32_t payload_length, void *(*malloc_like)(size_t)) {
    data_entry_preamble_t * data_slot;

    if (try_to_use_existing_data_slot) {
        data_entry_preamble_t *existing_data_slot = (data_entry_preamble_t *) ((char *) data_region + index_slot->offset);

        if (payload_length <= existing_data_slot->key_length + existing_data_slot->data_length) {
            data_slot = (void *) existing_data_slot;
        } else {
            fprintf(stderr, "Not implemented support yet for not possible state with try_to_use_existing_data_slot\n");
            exit(EXIT_FAILURE);
        }
    } else {
        data_slot = malloc_like(payload_length + sizeof(data_entry_preamble_t));
    }

    return data_slot;
}

// Given a data region, index and data slots, insert the key and value into the data table, and update the index slot
// with the correct values
void insert_in_table(void *data_region, index_entry_t *index_slot, data_entry_preamble_t *data_slot, char *key, uint32_t key_length, uint32_t key_hash, void *data, uint32_t data_length, uint32_t version_number) {
    ptrdiff_t offset = (char *) data_slot - (char *) data_region;

    data_slot->key_length = key_length;
    data_slot->data_length = data_length;

    char *key_location_in_table = (char *) data_slot + sizeof(*data_slot);
    strcpy(key_location_in_table, key);

    void *data_location_in_table = (char *) data_slot + sizeof(*data_slot) + key_length;
    memcpy(data_location_in_table, data, data_length);

    index_slot->offset = offset;
    index_slot->hash = key_hash;
    index_slot->version_number = version_number;
    index_slot->status = 1;
}
