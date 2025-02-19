#include <sisci_types.h>
#include <sisci_api.h>
#include <stdlib.h>
#include "put_request_region.h"
#include "sisci_glob_defs.h"

slot_bucket_t put_region_bucket_desc[PUT_REQUEST_BUCKETS];

size_t put_region_size(void) {
    static size_t size = 0;
    // Cache result
    if (size != 0) return size;

    size = put_region_buckets_size();
    return size + sizeof(put_request_region_t);
}

// This gives the total size of the buckets inside of the put region, that means the entire region, except the preamble
size_t put_region_buckets_size(void) {
    static size_t total_size = 0;
    // Cache result
    if (total_size != 0) return total_size;

    for (size_t slot_size = MIN_SIZE_ELEMENT; slot_size <= MAX_SIZE_ELEMENT; slot_size *= 2) {
        total_size += BUCKET_SIZE(slot_size);
    }

    return total_size;
}

// Gives the amount of slots in total in all buckets
uint32_t total_slots(void) {
    static uint32_t count = 0;
    if (count != 0) return count;

    for (uint32_t i = 0; i < PUT_REQUEST_BUCKETS; i++) {
        size_t exp = MIN_SIZE_ELEMENT_EXP + i;
        size_t slot_size = POWER_OF_TWO(exp);
        count += COMPUTE_SLOT_COUNT(slot_size);
    }

    return count;
}

// Inits the descriptor table that points to buckets of different sizes (exponents)
void init_bucket_desc(void) {
    size_t offset = 0;
    for (uint32_t i = 0; i < PUT_REQUEST_BUCKETS; i++) {
        size_t exp = MIN_SIZE_ELEMENT_EXP + i;
        size_t slot_size = POWER_OF_TWO(exp);

        put_region_bucket_desc[i].slot_size = slot_size;
        put_region_bucket_desc[i].count = COMPUTE_SLOT_COUNT(slot_size);
        put_region_bucket_desc[i].offset = offset;
        offset += BUCKET_SIZE(slot_size);
    }
}

// Returns a bucket number from an offset in the put region, the offset is measured without the preamble
uint32_t get_bucket_no_from_offset(size_t offset) {
    for (uint32_t bucket_no = 0; bucket_no < PUT_REQUEST_BUCKETS - 1; bucket_no++) {
        if (offset < put_region_bucket_desc[bucket_no+1].offset)
            return bucket_no;
    }
    return PUT_REQUEST_BUCKETS - 1;
}

// Given an offset and a bucket number, return the slot number
size_t get_slot_no_from_offset(size_t offset, uint32_t bucket_no) {
    size_t bucket_offset = put_region_bucket_desc[bucket_no].offset;
    size_t slot_size = put_region_bucket_desc[bucket_no].slot_size;
    return (offset - bucket_offset) / (slot_size + sizeof(put_request_slot_preamble_t));
}
