#include "main.h"

#include <stdlib.h>
#include <sisci_api.h>

#include "sisci_glob_defs.h"
#include "put_request_region_protocol.h"
#include "index_data_protocol.h"

#include "get_node_id.h"

static sci_local_data_interrupt_t ack_data_interrupt;
static void *buddy_metadata;
static struct volatile_buddy *buddy;
static volatile put_request_region_t *put_request_region;

static uint32_t free_header_slot = 0;
slot_metadata_t *slots[BUCKET_COUNT];

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

    init_bucket_desc();

    printf("A\n");

    for (uint32_t exp_index = 0; exp_index < BUCKET_COUNT; exp_index++) {
        uint32_t exp = MIN_SIZE_ELEMENT_EXP + exp_index;
        size_t slot_size = POWER_OF_TWO(exp);

        slots[exp_index] = malloc(COMPUTE_SLOT_COUNT(slot_size) * sizeof(slot_metadata_t));
        if (slots[exp_index] == NULL) {
            perror("malloc");
            exit(EXIT_FAILURE);
        }
    }

    printf("B\n");

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
                                                                              put_region_size(),
                                                                              NO_SUGGESTED_ADDRESS,
                                                                              NO_FLAGS,
                                                                              &sci_error);

    printf("pointer man: %p\n", (volatile void *) put_request_region);

    if (sci_error != SCI_ERR_OK) {
        fprintf(stderr, "SCIMapLocalSegment failed: %s\n", SCIGetErrorString(sci_error));
        exit(EXIT_FAILURE);
    }

    printf("C\n");

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

    printf("D\n");

    uint ack_interrupt_no = ACK_DATA_INTERRUPT_NO;
    SEOE(SCICreateDataInterrupt,
         sd,
         &ack_data_interrupt,
         ADAPTER_NO,
         &ack_interrupt_no,
         put_ack,
         NO_ARG,
         SCI_FLAG_USE_CALLBACK | SCI_FLAG_FIXED_INTNO);

    unsigned int node_id = get_node_id();
    if (node_id > UINT8_MAX) {
        fprintf(stderr, "node_id too large!\n");
        exit(EXIT_FAILURE);
    }
    put_request_region->sisci_node_id = (uint8_t) node_id;

    printf("E %d\n", MAX_PUT_REQUEST_SLOTS);

    for (uint32_t i = 0; i < MAX_PUT_REQUEST_SLOTS; i++) {
        put_request_region->header_slots[i] = 0;
    }

    printf("F\n");

    put_request_region->status = ACTIVE;

    printf("pointer man again: %p\n", (volatile void *) put_request_region);

    printf("1\n");
    for (uint32_t exp_index = 0; exp_index < BUCKET_COUNT; exp_index++) {
        printf("11\n");

        uint32_t exp = MIN_SIZE_ELEMENT_EXP + exp_index;
        size_t slot_size = POWER_OF_TWO(exp);
        printf("12\n");

        size_t slot_count = COMPUTE_SLOT_COUNT(slot_size);
        printf("13 %ld\n", put_region_bucket_desc[exp_index].offset);
        volatile char *start_of_bucket = ((volatile char *) put_request_region) + sizeof(put_request_region_t) + put_region_bucket_desc[exp_index].offset;
        printf("12 again\n");

        for (uint32_t slot_index = 0; slot_index < slot_count; slot_index++) {
            //TODO: maybe we should mark the actual slots as free aswell in case not zeroed
            slots[exp_index][slot_index].ack_count = 0;
            slots[exp_index][slot_index].status = FREE;
            slots[exp_index][slot_index].total_payload_size = (uint32_t) slot_size;
            slots[exp_index][slot_index].slot_preamble = (volatile put_request_slot_preamble_t *) start_of_bucket + slot_index * (slot_size + sizeof(put_request_slot_preamble_t));
            slots[exp_index][slot_index].offset = (ptrdiff_t) (put_region_bucket_desc[exp_index].offset +
                                              slot_index * (slot_size + sizeof(put_request_slot_preamble_t)));
        }
    }
    printf("2\n");

    int sample_data[256];

    for (int i = 0; i < 256; i++) {
        sample_data[i] = i;
    }

    char key[] = "tall";

    printf("2a\n");

    slot_metadata_t *slot = find_available_slot(256 * sizeof(int) + 4);

    printf("3\n");

    put_into_slot(slot, key, 4, sample_data, 256 * sizeof(int));

    printf("4\n");


    // TODO: How to free the slots in buddy and in general
    while(1);

    free(buddy_metadata);

    SEOE(SCIClose, sd, NO_FLAGS);
    SCITerminate();

    return EXIT_SUCCESS;
}

// Put key and value into a slot. Both the key and value will be copied. The preamble must be free when entering.
void put_into_slot(slot_metadata_t *slot, const char *key, uint8_t key_len, void *value, uint32_t value_len) {
    printf("Putting tplsize: %u\n", slot->total_payload_size);
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

    put_request_region->header_slots[free_header_slot] = (size_t) slot->offset;
    free_header_slot = (free_header_slot + 1) % MAX_PUT_REQUEST_SLOTS;
}

slot_metadata_t *find_available_slot(size_t slot_payload_size) {
    if (slot_payload_size > UINT32_MAX) {
        fprintf(stderr, "Too big slot_payload_size\n");
        exit(EXIT_FAILURE);
    }
    printf("x3\n");


    int exp = min_twos_complement_bits((uint32_t) slot_payload_size);
    printf("exp: %d\n", exp);
    uint32_t exp_index = (uint32_t) (exp - MIN_SIZE_ELEMENT_EXP);
    size_t slot_size = POWER_OF_TWO(exp);
    printf("x4: %d, slot_payload_size: %ld\n", exp_index, slot_payload_size);

    for (uint32_t i = 0; i < COMPUTE_SLOT_COUNT(slot_size); i++) {
        printf("i: %d\n", i);
        slot_metadata_t *slot = &(slots[exp_index][i]);
        //printf("slots[%d][%d] %p || WEEE: %d\n", exp_index, i, (void *) slot, slots[exp_index][i].total_payload_size);
        if (slot->status == FREE) {
            printf("found free slot at exp_index: %d and slot index: %d that has total payload size: %u\n", exp_index, i, slots[exp_index][i].total_payload_size);
            return slot;
        }
        printf("geth\n");
    }
    printf("x5\n");


    return NULL;
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

    printf("Got ALL acks!!\n");

    slot_metadata->ack_count = 0;
    slot_metadata->status = FREE;

    return SCI_CALLBACK_CONTINUE;
}
