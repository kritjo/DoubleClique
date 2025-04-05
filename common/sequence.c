#include <sisci_error.h>
#include <sisci_types.h>
#include <sisci_api.h>
#include <stdio.h>
#include <stdlib.h>

#include "sequence.h"

enum sequence_completed_state check_for_errors(sci_sequence_t sequence) {
    sci_error_t error;
    sci_sequence_status_t status;
    status = SCICheckSequence(sequence, NO_FLAGS, &error);
    if (error != SCI_ERR_OK) {
        fprintf(stderr, "SCICheckSequence returned non SCI_ERR_OK, which should not be possible: %s\n", SCIGetErrorString(error));
        exit(EXIT_FAILURE);
    }

    switch (status) {
        case SCI_SEQ_RETRIABLE:
            fprintf(stderr, "Retriable error occured\n");
            return SEQ_COMP_STATE_NOT_RETRIABLE;
        case SCI_SEQ_NOT_RETRIABLE:
            fprintf(stderr, "Non retriable error occured\n");
            return SEQ_COMP_STATE_RETRIABLE;
        case SCI_SEQ_PENDING:
            fprintf(stderr, "Pending error occured\n");
            do {
                status = SCIStartSequence(sequence, NO_FLAGS, &error);
                if (error != SCI_ERR_OK) {
                    fprintf(stderr, "SCICheckSequence returned non SCI_ERR_OK, which should not be possible: %s\n", SCIGetErrorString(error));
                    exit(EXIT_FAILURE);
                }
            } while (status == SCI_SEQ_PENDING);
            switch (status) {
                case SCI_SEQ_OK:
                    fprintf(stderr, "    resolved: OK\n");
                    return SEQ_COMP_STATE_OK;
                case SCI_SEQ_RETRIABLE:
                    fprintf(stderr, "    resolved: retriable\n");
                    return SEQ_COMP_STATE_RETRIABLE;
                case SCI_SEQ_NOT_RETRIABLE:
                    fprintf(stderr, "    resolved: non retriable\n");
                    return SEQ_COMP_STATE_NOT_RETRIABLE;
                case SCI_SEQ_PENDING:
                default:
                    fprintf(stderr, "    resolved: Illegal state for sequence\n");
                    exit(EXIT_FAILURE);
            }
        case SCI_SEQ_OK:
            return SEQ_COMP_STATE_OK;
        default:
            fprintf(stderr, "Illegal state for sequence\n");
            exit(EXIT_FAILURE);
    }

}
