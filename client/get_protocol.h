#ifndef DOUBLECLIQUE_GET_PROTOCOL_H
#define DOUBLECLIQUE_GET_PROTOCOL_H

#include <stdint.h>

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
    const char *error_message;
} pending_get_status_t;

typedef struct {
    bool completed;
    uint8_t slots_length;
    index_entry_t slots[INDEX_SLOTS_PR_BUCKET];
} stored_index_data_t;

#define NO_ERROR_MSG ""
#define NO_INDEX_ENTRIES "Found no entries in table that match key\n"
#define NO_DATA_MATCH "None of the keys with the correct hash has a key that match\n"
#define CONTINGENCY_ERROR "There was an error receiving the data during the the contingency fetch\n"
#define TIMEOUT "The request timed out\n"

#endif //DOUBLECLIQUE_GET_PROTOCOL_H
