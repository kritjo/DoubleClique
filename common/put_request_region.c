#include <sisci_types.h>
#include <sisci_api.h>
#include <stdlib.h>
#include "put_request_region.h"
#include "sisci_glob_defs.h"

slot_bucket_t put_region_bucket_desc[BUCKET_COUNT];

size_t put_region_size(void) {
    static size_t size = 0;
    // Cache result
    if (size != 0) return size;

    size = put_region_buckets_size();
    return size + sizeof(put_request_region_t);
}

size_t put_region_buckets_size(void) {
    static size_t total_size = 0;
    // Cache result
    if (total_size != 0) return total_size;

    for (size_t slot_size = MIN_SIZE_ELEMENT; slot_size <= MAX_SIZE_ELEMENT; slot_size *= 2) {
        total_size += BUCKET_SIZE(slot_size);
    }

    return total_size;
}

uint32_t total_slots(void) {
    static uint32_t count = 0;
    if (count != 0) return count;

    for (uint32_t i = 0; i < BUCKET_COUNT; i++) {
        size_t exp = MIN_SIZE_ELEMENT_EXP + i;
        size_t slot_size = POWER_OF_TWO(exp);
        count += COMPUTE_SLOT_COUNT(slot_size);
    }

    return count;
}

void init_bucket_desc(void) {
    size_t offset = 0;
    for (uint32_t i = 0; i < BUCKET_COUNT; i++) {
        size_t exp = MIN_SIZE_ELEMENT_EXP + i;
        size_t slot_size = POWER_OF_TWO(exp);

        put_region_bucket_desc[i].slot_size = slot_size;
        put_region_bucket_desc[i].count = COMPUTE_SLOT_COUNT(slot_size);
        put_region_bucket_desc[i].offset = offset;
        offset += BUCKET_SIZE(slot_size);
    }
}

uint32_t get_bucket_no_from_offset(size_t offset) {
    for (uint32_t bucket_no = 0; bucket_no < BUCKET_COUNT - 1; bucket_no++) {
        if (offset < put_region_bucket_desc[bucket_no+1].offset)
            return bucket_no;
    }
    return BUCKET_COUNT - 1;
}

size_t get_slot_no_from_offset(size_t offset, uint32_t bucket_no) {
    size_t bucket_offset = put_region_bucket_desc[bucket_no].offset;
    size_t slot_size = put_region_bucket_desc[bucket_no].slot_size;
    return (offset - bucket_offset) / (slot_size + sizeof(put_request_slot_preamble_t));
}

void connect_to_put_ack_data_interrupt(sci_desc_t sd, sci_remote_data_interrupt_t *ack_data_interrupt, uint32_t remote_id) {
    SEOE(SCIConnectDataInterrupt,
         sd,
         ack_data_interrupt,
         remote_id,
         ADAPTER_NO,
         ACK_DATA_INTERRUPT_NO,
         SCI_INFINITE_TIMEOUT,
         NO_FLAGS);
}

//TODO: Should probably transmit the version number to avoid replay attacks
void send_ack(uint8_t replica_no, sci_remote_data_interrupt_t ack_data_interrupt, size_t offset_to_ack) {
    put_ack_t put_ack;
    put_ack.replica_no = replica_no;
    put_ack.bucket_no = get_bucket_no_from_offset(offset_to_ack);
    put_ack.slot_no = (uint32_t) get_slot_no_from_offset(offset_to_ack, put_ack.bucket_no);

    SEOE(SCITriggerDataInterrupt, ack_data_interrupt, &put_ack, sizeof(put_ack), NO_FLAGS);
}
