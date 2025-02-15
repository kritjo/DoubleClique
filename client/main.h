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

typedef struct {
    bool completed;
    uint8_t slots_length;
    index_entry_t slots[INDEX_SLOTS_PR_BUCKET];
} stored_index_data_t;

enum get_status {
    NOT_POSTED,
    POSTED,
    COMPLETED_SUCCESS,
    COMPLETED_ERROR
};

typedef struct {
    enum get_status status;
    uint32_t data_length;
    void *data;
} pending_get_status_t;

enum get_return_status {
    GET_RETURN_SUCCESS,
    GET_RETURN_ERROR
};

typedef struct {
    enum get_return_status status;
    uint32_t data_length;
    void *data;
    const char *error_message;
} get_return_t;

const char no_error_msg[] = "";
const char generic_error_msg[] = "Something went wrong";

#endif //DOUBLECLIQUE_MAIN_H
