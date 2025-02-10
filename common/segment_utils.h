#ifndef DOUBLECLIQUE_SEGMENT_UTILS_H
#define DOUBLECLIQUE_SEGMENT_UTILS_H

#include <stddef.h>
#include <sisci_types.h>

void create_plain_segment_and_set_available(sci_desc_t sd, sci_local_segment_t *segment, size_t size, int segment_id);

#endif //DOUBLECLIQUE_SEGMENT_UTILS_H
