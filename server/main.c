#include "main.h"

#include <stdlib.h>
#include <sisci_api.h>
#include "sisci_glob_defs.h"
#include "put_request_region.h"
#include "index_data_protocol.h"

void create_plain_segment_and_set_available(sci_desc_t sd, sci_local_segment_t *segment, size_t size, int segment_id) {
    SEOE(SCICreateSegment,
         sd,
         segment,
         segment_id,
         size,
         NO_CALLBACK,
         NO_ARG,
         NO_FLAGS);

    SEOE(SCIPrepareSegment,
         *segment,
         ADAPTER_NO,
         NO_FLAGS);

    SEOE(SCISetSegmentAvailable,
         *segment,
         ADAPTER_NO,
         NO_FLAGS);
}

int main(int argc, char* argv[]) {
    sci_desc_t sd;
    sci_error_t sci_error;

    sci_local_segment_t index_segment;
    sci_map_t index_map;
    void *index;

    sci_local_segment_t data_segment;
    sci_map_t data_map;
    void *data;

    //TODO: needs to be checked
    if (argc != 2) {
        fprintf(stderr, "Need to pass replica id!\n");
        exit(EXIT_FAILURE);
    }
    uint8_t replica_id;
    replica_id = (uint8_t) strtol(argv[1], NULL, 10);

    SEOE(SCIInitialize, NO_FLAGS);
    SEOE(SCIOpen, &sd, NO_FLAGS);

    init_bucket_desc();

    create_plain_segment_and_set_available(sd, &index_segment, INDEX_REGION_SIZE, replica_index_segment_id[replica_id]);
    printf("Made index segment\n");
    create_plain_segment_and_set_available(sd, &data_segment, DATA_REGION_SIZE, replica_data_segment_id[replica_id]);
    printf("Made data segment\n");

    index = SCIMapLocalSegment(index_segment, &index_map, NO_OFFSET, INDEX_REGION_SIZE, NO_SUGGESTED_ADDRESS, NO_FLAGS, &sci_error);
    if (sci_error != SCI_ERR_OK) {
        fprintf(stderr, "SCIMapLocalSegment failed: %s\n", SCIGetErrorString(sci_error));
        exit(EXIT_FAILURE);
    }

    printf("Mapped index segment\n");

    data = SCIMapLocalSegment(data_segment, &data_map, NO_OFFSET, DATA_REGION_SIZE, NO_SUGGESTED_ADDRESS, NO_FLAGS, &sci_error);
    if (sci_error != SCI_ERR_OK) {
        fprintf(stderr, "SCIMapLocalSegment failed: %s\n", SCIGetErrorString(sci_error));
        exit(EXIT_FAILURE);
    }

    printf("Mapped data segment\n");

    put_request_region_poller_thread_args_t args;
    args.sd = sd;
    args.replica_number = replica_id;
    args.index_region = index;
    args.data_region = data;
    put_request_region_poller(&args);

    SEOE(SCIClose, sd, NO_FLAGS);
    SCITerminate();

    return EXIT_SUCCESS;
}
