#ifndef DOUBLECLIQUE_SUPER_FAST_HASH_CIRCULAR_H
#define DOUBLECLIQUE_SUPER_FAST_HASH_CIRCULAR_H

#include <stdint.h>

/*
 * Circular buffer variant of Paul Hsieh's SuperFastHash
 * Handles data that may wrap around a circular buffer boundary
 *
 * Parameters:
 *   buffer_start: pointer to the beginning of the circular buffer
 *   buffer_size: total size of the circular buffer
 *   offset: starting offset within the buffer
 *   len: length of data to hash
 */
uint32_t super_fast_hash_circular_optimized(const char *buffer_start, size_t buffer_size,
                                            size_t offset, int len);

uint32_t super_fast_hash_circular_with_uint32(const char *buffer_start, size_t buffer_size,
                                              size_t offset, int len, uint32_t append_data);

#endif //DOUBLECLIQUE_SUPER_FAST_HASH_CIRCULAR_H
