#include <stdint.h>
#include "power_of_two.h"

unsigned int next_power_of_two(unsigned int num) {
    if (num == 0) return 1;  // Special case for zero

    unsigned int power = 1;
    while (power < num) {
        power <<= 1; // Keep shifting left until we exceed or match num
    }
    return power;
}

// Check if __builtin_clz is available
#if defined(__GNUC__) || defined(__clang__)
#define HAS_CLZ 1
#else
#define HAS_CLZ 0
#endif

int min_twos_complement_bits(uint32_t x) {
#if HAS_CLZ
    return 32 - __builtin_clz(x);  // Uses GCC/Clang built-in
#else
    // Fallback method: Iterative bit counting
    int bits = 0;
    while (x > 0) {
        x >>= 1;
        bits++;
    }
    return bits;
#endif
}
