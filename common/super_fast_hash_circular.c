#include <stddef.h>
#include <stdint.h>
#include "super_fast_hash_circular.h"
#include "super_fast_hash.h"

// Safely get a byte from a circular buffer at logical position
static inline uint8_t get_circular_byte(const char *buffer_start, size_t buffer_size,
                                        size_t offset, int pos) {
    size_t actual_pos = (offset + pos) % buffer_size;
    return ((const uint8_t *)buffer_start)[actual_pos];
}

// Safely get 16 bits from a circular buffer at logical position (handles wraparound)
static inline uint16_t get_circular_16bits(const char *buffer_start, size_t buffer_size,
                                           size_t offset, int pos) {
    if (offset + pos + 1 < buffer_size) {
        // Fast path: contiguous, no wraparound
        size_t actual_pos = offset + pos;
#if (defined(__GNUC__) && defined(__i386__)) || defined(__WATCOMC__) \
    || defined(_MSC_VER) || defined (__BORLANDC__) || defined (__TURBOC__)
        return *((const uint16_t *)(buffer_start + actual_pos));
#else
        return ((uint32_t)(((const uint8_t *)(buffer_start + actual_pos))[1]) << 8)
               + (uint32_t)(((const uint8_t *)(buffer_start + actual_pos))[0]);
#endif
    } else {
        // Slow path: handle wraparound
        uint8_t b0 = get_circular_byte(buffer_start, buffer_size, offset, pos);
        uint8_t b1 = get_circular_byte(buffer_start, buffer_size, offset, pos + 1);
        return ((uint32_t)b1 << 8) + (uint32_t)b0;
    }
}

// Final avalanche mix for hash
static inline uint32_t hash_avalanche(uint32_t hash) {
    hash ^= hash << 3;
    hash += hash >> 5;
    hash ^= hash << 4;
    hash += hash >> 17;
    hash ^= hash << 25;
    hash += hash >> 6;
    return hash;
}

// Chunk-processing helper for circular buffer (returns next logical position)
static inline int process_circular_chunks(const char *buffer_start, size_t buffer_size,
                                          size_t offset, int data_pos, int chunks, uint32_t *hash) {
    for (; chunks > 0; --chunks) {
        *hash += get_circular_16bits(buffer_start, buffer_size, offset, data_pos);
        uint32_t tmp = (get_circular_16bits(buffer_start, buffer_size, offset, data_pos + 2) << 11) ^ *hash;
        *hash = (*hash << 16) ^ tmp;
        data_pos += 4;
        *hash += *hash >> 11;
    }
    return data_pos;
}

// Remainder-processing helper for circular buffer (1-3 bytes)
static inline uint32_t process_circular_remainder(const char *buffer_start, size_t buffer_size,
                                                  size_t offset, int data_pos, int rem, uint32_t hash) {
    switch (rem) {
        case 3:
            hash += get_circular_16bits(buffer_start, buffer_size, offset, data_pos);
            hash ^= hash << 16;
            hash ^= ((signed char)get_circular_byte(buffer_start, buffer_size, offset, data_pos + 2)) << 18;
            hash += hash >> 11;
            break;
        case 2:
            hash += get_circular_16bits(buffer_start, buffer_size, offset, data_pos);
            hash ^= hash << 11;
            hash += hash >> 17;
            break;
        case 1:
            hash += (signed char)get_circular_byte(buffer_start, buffer_size, offset, data_pos);
            hash ^= hash << 10;
            hash += hash >> 1;
            break;
    }
    return hash;
}

// Fallback: portable 16-bit read
#undef get16bits
#if (defined(__GNUC__) && defined(__i386__)) || defined(__WATCOMC__) \
    || defined(_MSC_VER) || defined (__BORLANDC__) || defined (__TURBOC__)
#define get16bits(d) (*((const uint16_t *) (d)))
#else
#define get16bits(d) ((((uint32_t)(((const uint8_t *)(d))[1])) << 8)\
                       +(uint32_t)(((const uint8_t *)(d))[0]) )
#endif

// Chunk-processing helper for linear data (in memory)
static inline uint32_t process_linear_chunks(const char *data, int bytes, uint32_t hash) {
    while (bytes >= 4) {
        hash += get16bits(data);
        uint32_t tmp = (get16bits(data + 2) << 11) ^ hash;
        hash = (hash << 16) ^ tmp;
        data += 4;
        hash += hash >> 11;
        bytes -= 4;
    }
    // Handle remainder (0-3 bytes)
    switch (bytes) {
        case 3:
            hash += get16bits(data);
            hash ^= hash << 16;
            hash ^= ((signed char)data[2]) << 18;
            hash += hash >> 11;
            break;
        case 2:
            hash += get16bits(data);
            hash ^= hash << 11;
            hash += hash >> 17;
            break;
        case 1:
            hash += (signed char)*data;
            hash ^= hash << 10;
            hash += hash >> 1;
            break;
    }
    return hash;
}

uint32_t super_fast_hash_circular(const char *buffer_start, size_t buffer_size,
                                  size_t offset, int len) {
    if (len <= 0 || buffer_start == NULL) return 0;

    uint32_t hash = len;
    int data_pos = 0;
    int chunks = len >> 2;
    int rem = len & 3;

    // Process all full 4-byte chunks
    data_pos = process_circular_chunks(buffer_start, buffer_size, offset, data_pos, chunks, &hash);

    // Process any remainder bytes
    hash = process_circular_remainder(buffer_start, buffer_size, offset, data_pos, rem, hash);

    return hash_avalanche(hash);
}

// Optimized: fast path for no wraparound
uint32_t super_fast_hash_circular_optimized(const char *buffer_start, size_t buffer_size,
                                            size_t offset, int len) {
    if (offset + (size_t) len <= buffer_size) {
        // Fast path: contiguous
        return super_fast_hash(buffer_start + offset, len);
    }
    // Slow path: wraparound
    return super_fast_hash_circular(buffer_start, buffer_size, offset, len);
}

// Hash with uint32_t appended: builds a flat buffer for final hash chunk
uint32_t super_fast_hash_circular_with_uint32(const char *buffer_start, size_t buffer_size,
                                              size_t offset, int len, uint32_t append_data) {
    if (len < 0 || buffer_start == NULL) return 0;

    uint32_t hash = len + 4; // Include 4 bytes for appended uint32
    int data_pos = 0;
    int chunks = len >> 2;
    int rem = len & 3;

    // Process all full 4-byte chunks in circular buffer
    data_pos = process_circular_chunks(buffer_start, buffer_size, offset, data_pos, chunks, &hash);

    // Assemble remaining circular bytes and appended uint32 into a temp buffer
    char combined[7]; // 3 remainder + 4 from uint32
    int n = 0;
    for (; n < rem; ++n)
        combined[n] = get_circular_byte(buffer_start, buffer_size, offset, data_pos + n);
    const char *ap_bytes = (const char*)&append_data;
    for (int i = 0; i < 4; ++i)
        combined[n + i] = ap_bytes[i];

    // Use linear helper for the tail+uint32 chunk
    hash = process_linear_chunks(combined, rem + 4, hash);

    return hash_avalanche(hash);
}
