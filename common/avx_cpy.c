#include <immintrin.h>
#include <stddef.h>
#include <stdint.h>

static inline void
_memcpy_nt_avx2_helper(void *__restrict dst,
                       const void *__restrict src,
                       size_t bytes)
{
    unsigned char       *__restrict dest = (unsigned char *)dst;
    const unsigned char *__restrict s    = (const unsigned char *)src;

    size_t i, vec_bytes, vec_end, prefix;
    unsigned long misalign;
    int src_aligned;
    __m256i v0, v1;

    /* Align dest to 64 bytes for stream stores */
    misalign = (unsigned long)dest & 63UL;
    prefix   = misalign ? (64UL - misalign) : 0UL;

    if (prefix > bytes)
        prefix = bytes;

    for (i = 0; i < prefix; ++i) {
        dest[i] = s[i];
    }

    vec_bytes = ((bytes - i) / 64UL) * 64UL;
    vec_end   = i + vec_bytes;

    src_aligned = (((uintptr_t)(s + i)) & 31u) == 0;

    if (src_aligned) {
        for (; i < vec_end; i += 64) {
            v0 = _mm256_load_si256((const __m256i *)(s + i));
            v1 = _mm256_load_si256((const __m256i *)(s + i + 32));
            _mm256_stream_si256((__m256i *)(dest + i), v0);
            _mm256_stream_si256((__m256i *)(dest + i + 32), v1);
        }
    } else {
        for (; i < vec_end; i += 64) {
            v0 = _mm256_loadu_si256((const __m256i *)(s + i));
            v1 = _mm256_loadu_si256((const __m256i *)(s + i + 32));
            _mm256_stream_si256((__m256i *)(dest + i), v0);
            _mm256_stream_si256((__m256i *)(dest + i + 32), v1);
        }
    }

    for (; i < bytes; ++i) {
        dest[i] = s[i];
    }
}

void memcpy_nt_avx2(void *__restrict dst,
                    const void *__restrict src,
                    size_t bytes,
                    unsigned int chunk)
{
    unsigned char *dst_tmp;
    unsigned char *src_tmp;
    size_t j;

    if (bytes < (size_t)chunk * 2) {
        _memcpy_nt_avx2_helper(dst, src, bytes);
        _mm_sfence();
    } else {
        dst_tmp = (unsigned char *)dst;
        src_tmp = (unsigned char *)src;

        for (j = 0; j < bytes; j += chunk) {
            _memcpy_nt_avx2_helper(dst_tmp, src_tmp, chunk);
            _mm_sfence();
            dst_tmp += chunk;
            src_tmp += chunk;
        }
    }
}