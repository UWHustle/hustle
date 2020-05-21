// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_SRC_BITWISE_H_
#define BITWEAVING_SRC_BITWISE_H_

#include "types.h"

namespace hustle::bitweaving {

/**
 * @brief Count the number of 1-bit in this word.
 * @param word The word to count.
 * @return The number of 1-bit.
 */
inline uint64_t Popcnt(uint64_t word)
{
  uint64_t count = 0;
  for (size_t i = 0; i < NUM_BITS_PER_WORD; i++) {
    if ((word & 1ULL) == 1)
      count++;
    word = word >> 1;
  }
  return count;
  // uint64_t ret;
  // __asm__ __volatile__ ("popcnt %1, %0" : "=r"(ret) : "r"(v));
  // return ret;
}

/**
 * @brief Remove the rightmost 1-bit in this word.
 * @param word The word to manipulate.
 * @return The updated word.
 */
inline uint64_t RemoveRightmostOne(uint64_t word)
{
  return word & (word - 1ULL);
}

/**
 * @brief Remove and smear the rightmost 1-bit in this word.
 * All bits at the left of the rightmost 1-bit in this word are
 * set to be 1-bit.
 * @param word The word to manipulate.
 * @return The updated word.
 */
inline uint64_t RemoveSmearRightmostOne(uint64_t word)
{
  return word ^ (-word);
}

} // namespace bitweaving

#endif // BITWEAVING_SRC_BITWISE_H_
