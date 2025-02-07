#ifndef DOUBLECLIQUE_INDEX_DATA_PROTOCOL_H
#define DOUBLECLIQUE_INDEX_DATA_PROTOCOL_H

#include <stdint.h>

#define INDEX_REGION_SIZE 0x800000
#define INDEX_SLOTS_COUNT (INDEX_REGION_SIZE/sizeof(index_entry_t))
#define INDEX_SLOTS_PR_BUCKET 4
#define INDEX_BUCKETS (INDEX_SLOTS_COUNT/INDEX_SLOTS_PR_BUCKET)

#define DATA_REGION_SIZE 0x400000

typedef struct {
    uint16_t status; //0 for unused, 1 for used
    uint32_t hash;
    uint32_t version_number;
    ptrdiff_t offset;
} index_entry_t;

// This definition takes a pointer to the start of a index region, and given a bucket number and a slot number
// returns the pointer to the slot.
#define GET_SLOT_POINTER(start_addr, bucket_number, slot_number) \
    (start_addr                                                  \
     +(INDEX_SLOTS_PR_BUCKET*sizeof(index_entry_t)*bucket_number)  \
     +(sizeof(index_entry_t)*slot_number))

typedef struct {
    uint32_t key_length; // No null byte
    uint32_t data_length;
} data_entry_preamble_t;

#endif //DOUBLECLIQUE_INDEX_DATA_PROTOCOL_H
