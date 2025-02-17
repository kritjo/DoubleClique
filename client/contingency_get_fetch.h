#ifndef DOUBLECLIQUE_CONTINGENCY_GET_FETCH_H
#define DOUBLECLIQUE_CONTINGENCY_GET_FETCH_H

#include "index_data_protocol.h"
#include "sisci_glob_defs.h"
#include "get_protocol.h"

#define CONTINGENCY_WAIT_FOR_INDEX_FETCHES_TIMEOUT_NS 100000

#define GET_RECEIVE_SEG_SIZE MAX_SIZE_ELEMENT * REPLICA_COUNT * INDEX_SLOTS_PR_BUCKET

typedef struct {
    uint32_t version_number;
    uint32_t count;
    index_entry_t index_entry[REPLICA_COUNT];
} version_count_t;

typedef struct {
    uint32_t replica_index;
    uint32_t candidate_index;
    uint32_t offset;
    index_entry_t index_entry;
    const char *key;
    pending_get_status_t *pending_get_status;
    pthread_mutex_t *get_in_progress;
} contingency_fetch_completed_args_t;

typedef struct {
    uint32_t version_number;
    index_entry_t index_entry[REPLICA_COUNT];
} found_candidates_t;

void contingency_init(sci_desc_t sd);

void contingency_backend_fetch(pending_get_status_t *pending_get_status,
                               pthread_mutex_t *get_in_progress,

                               const uint8_t *completed_fetches_of_index,
                               pthread_mutex_t *completed_index_fetches_mutex,

                               sci_remote_segment_t *data_segments,
                               stored_index_data_t *stored_index_data,
                               const size_t *completed_index_fetch_replica_index_order,

                               const uint32_t *already_tried_vnr,
                               uint32_t already_tried_vnr_count,

                               const char *key);

sci_callback_action_t contingency_data_fetch_completed_callback(void IN *arg, sci_dma_queue_t queue, sci_error_t status);

#endif //DOUBLECLIQUE_CONTINGENCY_GET_FETCH_H
