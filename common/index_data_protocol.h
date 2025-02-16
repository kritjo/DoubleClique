#ifndef DOUBLECLIQUE_INDEX_DATA_PROTOCOL_H
#define DOUBLECLIQUE_INDEX_DATA_PROTOCOL_H

#include <stdint.h>
#include <stddef.h>

// TODO: These might have some issues for the last bucket/slot maybe
#define INDEX_REGION_SIZE 0x800000
#define INDEX_SLOTS_COUNT (INDEX_REGION_SIZE/sizeof(index_entry_t))
#define INDEX_SLOTS_PR_BUCKET 4
#define INDEX_BUCKETS (INDEX_SLOTS_COUNT/INDEX_SLOTS_PR_BUCKET)

#define DATA_REGION_SIZE 0x400000

typedef struct {
    uint32_t hash;
    uint32_t version_number;
    ptrdiff_t offset;
    uint16_t status; //0 for unused, 1 for used
    uint32_t key_length; // No null byte
    uint32_t data_length;
} index_entry_t;

// This definition takes a pointer to the start of a index region, and given a bucket number and a slot number
// returns the pointer to the slot.
#define GET_SLOT_POINTER(start_addr, bucket_number, slot_number) \
    ((char *) (start_addr)                                                  \
     +(INDEX_SLOTS_PR_BUCKET*sizeof(index_entry_t)*(bucket_number))  \
     +(sizeof(index_entry_t)*(slot_number)))

typedef struct {
    //uint32_t checksum; TODO: maybe should have a checksum here idk if not we at least gotta putt in the version number, so that we know if we have torn reads. A TTL could be future work
} data_entry_preamble_t;

index_entry_t *existing_slot_for_key(void *index_region, void *data_region, uint32_t key_hash, uint32_t key_length, char *key);
index_entry_t *find_available_index_slot(void *index_region, uint32_t key_hash);
data_entry_preamble_t *find_data_slot_for_index_slot(void *data_region, index_entry_t *index_slot, bool try_to_use_existing_data_slot, uint32_t payload_length, void *(*malloc_like)(size_t));
void insert_in_table(void *data_region, index_entry_t *index_slot, data_entry_preamble_t *data_slot, char *key, uint32_t key_length, uint32_t key_hash, void *data, uint32_t data_length, uint32_t version_number);

#endif //DOUBLECLIQUE_INDEX_DATA_PROTOCOL_H
