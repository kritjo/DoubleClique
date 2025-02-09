#ifndef DOUBLECLIQUE_POWER_OF_TWO_H
#define DOUBLECLIQUE_POWER_OF_TWO_H

#include <stdint.h>

#define POWER_OF_TWO(n) (1 << (n))

unsigned int next_power_of_two(unsigned int num);
int min_twos_complement_bits(uint32_t x);

#endif //DOUBLECLIQUE_POWER_OF_TWO_H
