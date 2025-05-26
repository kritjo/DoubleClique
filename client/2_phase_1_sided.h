#ifndef DOUBLECLIQUE_2_PHASE_1_SIDED_H
#define DOUBLECLIQUE_2_PHASE_1_SIDED_H

#include <sisci_types.h>
#include <stdint.h>
#include <stdbool.h>
#include "sisci_glob_defs.h"
#include "index_data_protocol.h"
#include "main.h"
#include "ack_region.h"

#define GET_RECEIVE_SEG_SIZE MAX_SIZE_ELEMENT * REPLICA_COUNT * INDEX_SLOTS_PR_BUCKET

#define GetReceiveSegmentIdBase 42
#define GET_TIMEOUT_1_SIDED_NS 10000000
#define CONTINGENCY_WAIT_FOR_INDEX_FETCHES_TIMEOUT_NS 100000

typedef struct {
    size_t replica_index;
    uint32_t key_hash;
    uint8_t key_len;
    const char *key;
    size_t offset;
} get_index_response_args_t;

typedef struct {
    uint8_t key_len;
    uint32_t data_len;
    const char *key;
    uint8_t replica_index;
    uint32_t key_hash;
} get_data_response_args_t;

typedef struct {
    bool completed;
    uint8_t slots_length;
    index_entry_t slots[INDEX_SLOTS_PR_BUCKET];
} stored_index_data_t;

typedef struct {
    _Atomic bool contingency_fetch_started;
    _Atomic (request_promise_t *) promise;
} pending_get_status_t;

typedef struct {
    uint32_t version_number;
    uint32_t count;
    index_entry_t index_entry[REPLICA_COUNT];
    bool write_back;
    uint32_t write_back_offset;
} version_count_t;

typedef struct {
    uint32_t version_number;
    index_entry_t index_entry[REPLICA_COUNT];
    bool write_back;
    uint32_t write_back_offset;
} found_candidates_t;

typedef struct {
    uint32_t replica_index;
    uint32_t candidate_index;
    uint32_t offset;
    index_entry_t index_entry;
    const char *key;
    uint32_t found_contingency_candidates_count;
} contingency_fetch_completed_args_t;


void init_2_phase_1_sided_get(sci_desc_t sd, uint8_t *replica_node_ids, bool use_dma_);
request_promise_t *get_2_phase_1_sided(const char *key, uint8_t key_len);

static sci_callback_action_t index_fetch_completed_callback(void IN *arg, __attribute__((unused)) sci_dma_queue_t queue, sci_error_t status);
static sci_callback_action_t preferred_data_fetch_completed_callback(void IN *arg,
                                                                     __attribute__((unused)) sci_dma_queue_t queue, sci_error_t status);
static void contingency_backend_fetch(const uint32_t already_tried_vnr[], uint32_t already_tried_vnr_count, const char *key,
                                      uint32_t key_hash, uint32_t key_length);
static sci_callback_action_t contingency_data_fetch_completed_callback(void IN *arg,
                                                                       __attribute__((unused)) sci_dma_queue_t queue, sci_error_t status);

#define NO_ERROR_MSG ""
#define NO_INDEX_ENTRIES_MSG "Found no entries in table that match key"
#define NO_DATA_MATCH_MSG "None of the keys with the correct hash has a key that match"
#define CONTINGENCY_ERROR_MSG "There was an error receiving the data during the the contingency fetch"
#define TIMEOUT_MSG "The request timed out"

#endif //DOUBLECLIQUE_2_PHASE_1_SIDED_H
