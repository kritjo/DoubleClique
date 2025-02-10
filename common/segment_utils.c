#include <stddef.h>
#include <stdlib.h>
#include <sisci_types.h>
#include <sisci_api.h>

#include "sisci_glob_defs.h"

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
