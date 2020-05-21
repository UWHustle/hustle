// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_SRC_UTILITY_H_
#define BITWEAVING_SRC_UTILITY_H_

#include <iostream>

#include "types.h"

namespace hustle::bitweaving {

/**
 * @brief Convert a word to a string in binary format.
 * @param word The word to convert.
 * @return The binary string.
 */
std::string ToString(WordUnit word);

std::istream& operator>> (std::istream& in, ColumnType & type);

} // namespace bitweaving

#endif // BITWEAVING_SRC_UTILITY_H_
