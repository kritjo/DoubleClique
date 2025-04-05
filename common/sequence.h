#ifndef DOUBLECLIQUE_SEQUENCE_H
#define DOUBLECLIQUE_SEQUENCE_H

#include <sisci_types.h>
#include "sisci_glob_defs.h"

enum sequence_completed_state {
    SEQ_COMP_STATE_OK,
    SEQ_COMP_STATE_RETRIABLE,
    SEQ_COMP_STATE_NOT_RETRIABLE
};

enum sequence_completed_state check_for_errors(sci_sequence_t sequence);


#endif //DOUBLECLIQUE_SEQUENCE_H
