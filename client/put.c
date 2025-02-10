#include <stdlib.h>
#include <sisci_types.h>
#include <sisci_api.h>
#include "put.h"
#include "get_node_id.h"

slot_metadata_t *put(slot_metadata_t **slots, const char *key, uint8_t key_len, void *value, uint32_t value_len) {
    slot_metadata_t *slot = find_available_slot(slots, key_len + value_len);
    put_into_slot(slot, key, key_len, value, value_len);
    return slot;
}

void init_put_request_region(sci_desc_t sd, volatile put_request_region_t **put_request_region) {
    sci_error_t sci_error;

    sci_remote_segment_t put_request_segment;
    sci_map_t put_request_map;

    SEOE(SCIConnectSegment,
         sd,
         &put_request_segment,
         DIS_BROADCAST_NODEID_GROUP_ALL,
         PUT_REQUEST_SEGMENT_ID,
         ADAPTER_NO,
         NO_CALLBACK,
         NO_ARG,
         SCI_INFINITE_TIMEOUT,
         SCI_FLAG_BROADCAST);

    *put_request_region = (volatile put_request_region_t*) SCIMapRemoteSegment(put_request_segment,
                                                                              &put_request_map,
                                                                              NO_OFFSET,
                                                                              put_region_size(),
                                                                              NO_SUGGESTED_ADDRESS,
                                                                              NO_FLAGS,
                                                                              &sci_error);

    if (sci_error != SCI_ERR_OK) {
        fprintf(stderr, "SCIMapLocalSegment failed: %s\n", SCIGetErrorString(sci_error));
        exit(EXIT_FAILURE);
    }

    unsigned int node_id = get_node_id();
    if (node_id > UINT8_MAX) {
        fprintf(stderr, "node_id too large!\n");
        exit(EXIT_FAILURE);
    }
    (*put_request_region)->sisci_node_id = (uint8_t) node_id;

    for (uint32_t i = 0; i < MAX_PUT_REQUEST_SLOTS; i++) {
        (*put_request_region)->header_slots[i] = 0;
    }
}

void init_put_ack_data_interrupt(sci_desc_t sd, slot_metadata_t **slots) {
    sci_local_data_interrupt_t ack_data_interrupt;

    uint ack_interrupt_no = ACK_DATA_INTERRUPT_NO;

    SEOE(SCICreateDataInterrupt,
         sd,
         &ack_data_interrupt,
         ADAPTER_NO,
         &ack_interrupt_no,
         put_ack,
         slots,
         SCI_FLAG_USE_CALLBACK | SCI_FLAG_FIXED_INTNO);
}

static sci_callback_action_t put_ack(void *arg, sci_local_data_interrupt_t interrupt, void *data, unsigned int length, sci_error_t status) {
    slot_metadata_t **slots = (slot_metadata_t **) arg;

    if (status != SCI_ERR_OK) {
        fprintf(stderr, "Received error SCI status from delivery: %s\n", SCIGetErrorString(status));
        exit(EXIT_FAILURE);
    }

    if (length != sizeof(put_ack_t)) {
        fprintf(stderr, "Received invalid length %d from delivery\n", length);
        exit(EXIT_FAILURE);
    }

    put_ack_t *put_ack_data = (put_ack_t *) data;
    slot_metadata_t *slot_metadata = &slots[put_ack_data->bucket_no][put_ack_data->slot_no];
    uint8_t ack_count = ++slot_metadata->ack_count;

    if (ack_count < REPLICA_COUNT)
        return SCI_CALLBACK_CONTINUE;
    else if (ack_count > REPLICA_COUNT) {
        fprintf(stderr, "Got more acks than there are replicas, should not be possible\n");
        exit(EXIT_FAILURE);
    }
    // Got same amount of acks as there are replicas, we must make the slot available again
    // We do not need this, as the replicas actually put this for us: slot_metadata->slot_preamble->status = FREE;

    slot_metadata->ack_count = 0;
    slot_metadata->status = FREE;

    return SCI_CALLBACK_CONTINUE;
}

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
        if (slot->status == FREE) {
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
    if (slot->status != FREE) {
        fprintf(stderr, "Slot got to be free to put new value into it!\n");
        exit(EXIT_FAILURE);
    }

    if (slot->total_payload_size < key_len + value_len) {
        fprintf(stderr, "Tried to put too large payload into slot!\n");
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

    slot->slot_preamble->status = PUT;
    slot->status = PUT;
}
