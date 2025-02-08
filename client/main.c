#include "main.h"

#include <stdlib.h>
#include <sisci_api.h>

#include "sisci_glob_defs.h"
#include "put_request_region_protocol.h"
#include "index_data_protocol.h"

#define BUDDY_ALLOC_IMPLEMENTATION
#include "buddy_alloc.h"
#include "get_node_id.h"

static sci_local_data_interrupt_t ack_data_interrupt;
static void *buddy_metadata;
static struct volatile_buddy *buddy;
static volatile put_request_region_t *put_request_region;

static uint32_t allocated_slots = 0;
slot_metadata_t slots[MAX_PUT_REQUEST_SLOTS];

int main(int argc, char* argv[]) {
    sci_desc_t sd;
    sci_error_t sci_error;
    u_int8_t replica_node_ids[REPLICA_COUNT];

    sci_remote_segment_t put_request_segment;
    sci_map_t put_request_map;

    sci_remote_segment_t index_region_segments[REPLICA_COUNT];
    sci_map_t index_region_map[REPLICA_COUNT];
    volatile void *index_region_start[REPLICA_COUNT];

    sci_remote_segment_t data_region_segments[REPLICA_COUNT];
    sci_map_t data_region_map[REPLICA_COUNT];
    volatile void *data_region_start[REPLICA_COUNT];

    if (argc < REPLICA_COUNT + 1) {
        fprintf(stderr, "Usage: %s replica_id[0] ... replica_id[n]\n", argv[0]);
    }
    SEOE(SCIInitialize, NO_FLAGS);
    SEOE(SCIOpen, &sd, NO_FLAGS);

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

    put_request_region = (volatile put_request_region_t*) SCIMapRemoteSegment(put_request_segment,
                                                                              &put_request_map,
                                                                              NO_OFFSET,
                                                                              sizeof(put_request_region_t),
                                                                              NO_SUGGESTED_ADDRESS,
                                                                              NO_FLAGS,
                                                                              &sci_error);

    if (sci_error != SCI_ERR_OK) {
        fprintf(stderr, "SCIMapLocalSegment failed: %s\n", SCIGetErrorString(sci_error));
        exit(EXIT_FAILURE);
    }

    for (int replica_index = 0; replica_index < REPLICA_COUNT; replica_index++) {
        const char *replica_str_node_id = argv[1 + replica_index];
        char *endptr;
        long num;
        num = strtol(replica_str_node_id, &endptr, 10);
        if (num > UINT8_MAX) {
            fprintf(stderr, "Node id too high!\n");
            exit(EXIT_FAILURE);
        }
        replica_node_ids[replica_index] = (uint8_t) num;

        SEOE(SCIConnectSegment,
             sd,
             &index_region_segments[replica_index],
             replica_node_ids[replica_index],
             replica_index_segment_id[replica_index],
             ADAPTER_NO,
             NO_CALLBACK,
             NO_ARG,
             SCI_INFINITE_TIMEOUT,
             NO_FLAGS);

        index_region_start[replica_index] = SCIMapRemoteSegment(index_region_segments[replica_index],
                                                                &index_region_map[replica_index],
                                                                NO_OFFSET,
                                                                INDEX_REGION_SIZE,
                                                                NO_SUGGESTED_ADDRESS,
                                                                NO_FLAGS,
                                                                &sci_error);

        if (sci_error != SCI_ERR_OK) {
            fprintf(stderr, "SCIMapLocalSegment failed: %s\n", SCIGetErrorString(sci_error));
            exit(EXIT_FAILURE);
        }

        SEOE(SCIConnectSegment,
             sd,
             &data_region_segments[replica_index],
             replica_node_ids[replica_index],
             replica_data_segment_id[replica_index],
             ADAPTER_NO,
             NO_CALLBACK,
             NO_ARG,
             SCI_INFINITE_TIMEOUT,
             NO_FLAGS);

        data_region_start[replica_index] = SCIMapRemoteSegment(data_region_segments[replica_index],
                                                               &data_region_map[replica_index],
                                                               NO_OFFSET,
                                                               DATA_REGION_SIZE,
                                                               NO_SUGGESTED_ADDRESS,
                                                               NO_FLAGS,
                                                               &sci_error);

        if (sci_error != SCI_ERR_OK) {
            fprintf(stderr, "SCIMapLocalSegment failed: %s\n", SCIGetErrorString(sci_error));
            exit(EXIT_FAILURE);
        }
    }

    uint ack_interrupt_no = ACK_DATA_INTERRUPT_NO;
    SEOE(SCICreateDataInterrupt,
         sd,
         &ack_data_interrupt,
         ADAPTER_NO,
         &ack_interrupt_no,
         put_ack,
         NO_ARG,
         SCI_FLAG_USE_CALLBACK | SCI_FLAG_FIXED_INTNO);

    // Just to test we can try to allocate a single put_into_slot region

    size_t arena_size = PUT_REQUEST_REGION_SIZE;
    buddy_metadata = malloc(buddy_sizeof(arena_size));
    buddy = volatile_buddy_init(buddy_metadata, put_request_region->units, arena_size);

    unsigned int node_id = get_node_id();
    if (node_id > UINT8_MAX) {
        fprintf(stderr, "node_id too large!\n");
        exit(EXIT_FAILURE);
    }
    put_request_region->sisci_node_id = (uint8_t) node_id;

    int sample_data[256];

    for (int i = 0; i < 256; i++) {
        sample_data[i] = i;
    }

    char key[] = "tall";
    uint32_t slot_no = allocate_new_slot(4, 256*sizeof(int));

    put_into_slot(slot_no, key, 4, sample_data, 256 * sizeof(int));

    // TODO: How to free the slots in buddy and in general
    while(1);

    free(buddy_metadata);

    SEOE(SCIClose, sd, NO_FLAGS);
    SCITerminate();

    return EXIT_SUCCESS;
}

// Put key and value into a slot. Both the key and value will be copied. The preamble must be free when entering.
void put_into_slot(uint32_t slot_no, const char *key, uint8_t key_len, void *value, uint32_t value_len) {
    slot_metadata_t slot = slots[slot_no];
    if (slot.status != FREE) {
        fprintf(stderr, "Slot got to be free to put new value into it!\n");
        exit(EXIT_FAILURE);
    }

    if (slot.total_payload_size < key_len + value_len) {
        fprintf(stderr, "Tried to put too large payload into slot!\n");
        exit(EXIT_FAILURE);
    }

    slot.slot_preamble->key_length = key_len;
    slot.slot_preamble->value_length = value_len;
    slot.slot_preamble->version_number = 0xdeadbeef; //TODO: this needs to be computed

    //Copy over the key
    for (size_t i = 0; i < key_len; i++) {
        // Just put_into_slot in some numbers in increasing order as a sanity check, should be easy to validate
        *(((volatile char *) slot.slot_preamble + sizeof(put_request_slot_preamble_t) + (i * sizeof(char)))) = key[i];
    }

    // Copy over the value
    for (uint32_t i = 0; i < value_len; i++) {
        *(((volatile char *) slot.slot_preamble + sizeof(put_request_slot_preamble_t) + key_len + i)) = ((char *)value)[i];
    }

    slot.slot_preamble->status = PUT;
    slot.status = PUT;
}

uint32_t allocate_new_slot(size_t key_size, size_t value_size) {
    uint32_t slot_no = allocated_slots++;

    put_request_region->status = LOCKED;
    size_t slot_size = key_size + value_size + sizeof(put_request_slot_preamble_t);
    volatile void *slot = volatile_buddy_malloc(buddy, slot_size);
    ptrdiff_t offset = ((volatile uint8_t *) slot) - put_request_region->units;
    put_request_region->slot_offset_starts[slot_no] = offset;

    // Load with some sample slot, starting with the preamble -- TODO: we should only broadcast using memcpy with a single transaction
    volatile put_request_slot_preamble_t *slot_preamble = (volatile put_request_slot_preamble_t *) slot;
    slot_preamble->status = FREE;

    put_request_region->slots_used = allocated_slots;
    put_request_region->status = WALKABLE;

    slot_metadata_t *slot_metadata = &slots[slot_no];
    slot_metadata->slot_preamble = slot_preamble;
    slot_metadata->total_payload_size = key_size + value_size;
    slot_metadata->status = FREE;
    slot_metadata->ack_count = 0;

    return slot_no;
}

// Put ack callback
sci_callback_action_t put_ack(void *arg, sci_local_data_interrupt_t interrupt, void *data, unsigned int length, sci_error_t status) {
    if (status != SCI_ERR_OK) {
        fprintf(stderr, "Received error SCI status from delivery: %s\n", SCIGetErrorString(status));
        exit(EXIT_FAILURE);
    }

    if (length != sizeof(put_ack_t)) {
        fprintf(stderr, "Received invalid length %d from delivery\n", length);
        exit(EXIT_FAILURE);
    }

    put_ack_t *put_ack_data = (put_ack_t *) data;
    slot_metadata_t *slot_metadata = &slots[put_ack_data->slot_no];
    uint8_t ack_count = ++slot_metadata->ack_count;

    if (ack_count < REPLICA_COUNT)
        return SCI_CALLBACK_CONTINUE;
    else if (ack_count > REPLICA_COUNT) {
        fprintf(stderr, "Got more acks than there are replicas, should not be possible\n");
        exit(EXIT_FAILURE);
    }
    // Got same amount of acks as there are replicas, we must make the slot available again
    // We do not need this, as the replicas actually put this for us: slot_metadata->slot_preamble->status = FREE;

    printf("Got ALL acks!!\n");

    slot_metadata->ack_count = 0;
    slot_metadata->status = FREE;

    return SCI_CALLBACK_CONTINUE;
}
