#ifndef DOUBLECLIQUE_2_PHASE_READ_GET_H
#define DOUBLECLIQUE_2_PHASE_READ_GET_H

#include <sisci_types.h>
#include <stdint.h>
#include <stdbool.h>
#include "sisci_glob_defs.h"
#include "index_data_protocol.h"

#define GET_RECEIVE_SEG_SIZE MAX_SIZE_ELEMENT * REPLICA_COUNT * INDEX_SLOTS_PR_BUCKET

#define GetReceiveSegmentIdBase 42
#define GET_TIMEOUT_NS 1000000
#define CONTINGENCY_WAIT_FOR_INDEX_FETCHES_TIMEOUT_NS 100000

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
    const char *error_message;
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

typedef struct {
    uint32_t version_number;
    uint32_t count;
    index_entry_t index_entry[REPLICA_COUNT];
} version_count_t;

typedef struct {
    uint32_t version_number;
    index_entry_t index_entry[REPLICA_COUNT];
} found_candidates_t;

typedef struct {
    uint32_t replica_index;
    uint32_t candidate_index;
    uint32_t offset;
    index_entry_t index_entry;
    const char *key;
} contingency_fetch_completed_args_t;


void init_2_phase_read_get(sci_desc_t sd, uint8_t *replica_node_ids);
get_return_t *get_2_phase_read(const char *key, uint8_t key_len);

static sci_callback_action_t index_fetch_completed_callback(void IN *arg, sci_dma_queue_t queue, sci_error_t status);
static sci_callback_action_t preferred_data_fetch_completed_callback(void IN *arg, sci_dma_queue_t queue, sci_error_t status);
static void contingency_backend_fetch(const uint32_t already_tried_vnr[], uint32_t already_tried_vnr_count, const char *key);
static sci_callback_action_t contingency_data_fetch_completed_callback(void IN *arg, sci_dma_queue_t queue, sci_error_t status);

#define NO_ERROR_MSG ""
#define NO_INDEX_ENTRIES_MSG "Found no entries in table that match key"
#define NO_DATA_MATCH_MSG "None of the keys with the correct hash has a key that match"
#define CONTINGENCY_ERROR_MSG "There was an error receiving the data during the the contingency fetch"
#define TIMEOUT_MSG "The request timed out"

#endif //DOUBLECLIQUE_2_PHASE_READ_GET_H
