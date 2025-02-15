#include <sisci_types.h>
#include <sisci_api.h>
#include <stdlib.h>

#include "dma_utils.h"
#include "sisci_glob_defs.h"

void block_for_dma(sci_dma_queue_t dma_queue) {
    sci_dma_queue_state_t dma_state;
    dma_state = SCIDMAQueueState(dma_queue);

    while (dma_state == SCI_DMAQUEUE_POSTED) {
        SEOE(SCIWaitForDMAQueue, dma_queue, SCI_INFINITE_TIMEOUT, NO_FLAGS);
        dma_state = SCIDMAQueueState(dma_queue);
    }

    switch (dma_state) {
        case SCI_DMAQUEUE_IDLE:
        case SCI_DMAQUEUE_DONE:
            break;
        case SCI_DMAQUEUE_GATHER:
            fprintf(stderr, "ERROR: DMA queue is in gather state, don't know what to do\n");
            exit(EXIT_FAILURE);
        case SCI_DMAQUEUE_ABORTED:
            fprintf(stderr, "ERROR: DMA queue is in aborted state\n");
            exit(EXIT_FAILURE);
        case SCI_DMAQUEUE_ERROR:
            fprintf(stderr, "ERROR: DMA queue is in error state\n");
            exit(EXIT_FAILURE);
        case SCI_DMAQUEUE_POSTED:
        default:
            // Case to satisfy clang-tidy - and make it explicit that this should not happen
            fprintf(stderr, "ERROR: Logical error, this should not happen\n");
            exit(EXIT_FAILURE);
    }
}
