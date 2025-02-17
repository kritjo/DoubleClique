#ifndef DOUBLECLIQUE_MAIN_H
#define DOUBLECLIQUE_MAIN_H

#include <stdint.h>
#include <sisci_types.h>
#include "put_request_region.h"
#include "index_data_protocol.h"

// Must at least have space for the largest possible data size
// Must at least have space for the index entries that we want to read aligned(!)
#define GET_RECEIVE_SEG_SIZE 4096

int main(int argc, char* argv[]);

typedef struct {
    size_t replica_index;
    uint32_t key_hash;
    uint8_t key_len;
    const char *key;
} get_index_response_args_t;

typedef struct {
    uint8_t key_len;
    uint32_t data_len;
    const char *key;
    uint8_t replica_index;
} get_data_response_args_t;

#endif //DOUBLECLIQUE_MAIN_H
