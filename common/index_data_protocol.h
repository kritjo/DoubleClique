#ifndef DOUBLECLIQUE_INDEX_DATA_PROTOCOL_H
#define DOUBLECLIQUE_INDEX_DATA_PROTOCOL_H

#include <stdint.h>

#define INDEX_REGION_SIZE 64000000
#define INDEX_SLOTS_COUNT (INDEX_REGION_SIZE/sizeof(index_entry_t))
#define INDEX_SLOTS_PR_BUCKET 4
#define INDEX_BUCKETS (INDEX_SLOTS_COUNT/INDEX_SLOTS_PR_BUCKET)

#define DATA_REGION_SIZE 32000000

typedef struct {
    uint16_t status; //0 for unused, 1 for used
    uint32_t hash;
    uint32_t version_number;
    uint16_t segment_id;
    uint32_t offset;
} index_entry_t;

#define GET_SLOT_POINTER(start_addr, bucket_number, slot_number) \
    (start_addr                                                  \
     +(INDEX_SLOTS_PR_BUCKET*sizeof(index_entry_t)*bucket_number)  \
     +(sizeof(index_entry_t)*slot_number))

typedef struct {
    uint32_t key_length;
    uint32_t data_length;
} data_entry_preamble_t;

#endif //DOUBLECLIQUE_INDEX_DATA_PROTOCOL_H
