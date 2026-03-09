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

    size_t i = 0, vec_bytes, vec_end, prefix;
    uintptr_t misalign;
    int src_aligned;
    __m256i v0, v1;

    /* Align dest to 64 bytes for streaming stores */
    misalign = (uintptr_t)dest & 63u;
    prefix   = misalign ? (64u - misalign) : 0u;

    if (prefix > bytes)
        prefix = bytes;

    for (; i < prefix; ++i) {
        dest[i] = s[i];
    }

    vec_bytes = ((bytes - i) / 64u) * 64u;
    vec_end   = i + vec_bytes;

    src_aligned = ((((uintptr_t)(s + i)) & 31u) == 0);

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
                    size_t chunk)
{
    unsigned char *dst_tmp = (unsigned char *)dst;
    const unsigned char *src_tmp = (const unsigned char *)src;

    if (chunk == 0 || bytes == 0)
        return;

    if (bytes < chunk * 2) {
        _memcpy_nt_avx2_helper(dst, src, bytes);
        _mm_sfence();
        return;
    }

    while (bytes != 0) {
        size_t n = bytes < chunk ? bytes : chunk;
        _memcpy_nt_avx2_helper(dst_tmp, src_tmp, n);
        dst_tmp += n;
        src_tmp += n;
        bytes   -= n;
    }

    _mm_sfence();
}