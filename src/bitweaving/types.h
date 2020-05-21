// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_INCLUDE_TYPES_H_
#define BITWEAVING_INCLUDE_TYPES_H_

#include <stdint.h>
#include <stddef.h>
#include <limits>

/**
 * @namespace bitweaving Namespace for bitweaving library.
 */
namespace hustle::bitweaving {

/**
 * @brief Type for an encoded column value.
 */
typedef uint64_t Code;

/**
 * @brief Type for a processor word unit.
 */
typedef size_t WordUnit;

/**
 * @brief Identifier for a tuple in a table.
 */
typedef size_t TupleId;

/**
 * @brief Identifier for a column in a table.
 */
typedef size_t ColumnId;

const Code NullCode = std::numeric_limits<int64_t>::max(); //int32_t
const size_t NUM_BITS_PER_BYTE = 8;
const size_t NUM_BITS_PER_WORD = sizeof(WordUnit) * NUM_BITS_PER_BYTE;
const size_t NUM_CODES_PER_BLOCK = 1048576; // 1M block

/**
 * @brief Storage method
 */
enum ColumnType
{
  /**
   * @brief A naive array format.
   */
  kNaive,
  /**
   * @brief BitWeaving/H format.
   */
  kBitWeavingH,
  /**
   * @brief BitWeaving/V format.
   */
  kBitWeavingV
};

/**
 * @brief Comparator in predicate
 */
enum Comparator
{
  /**
   * @brief Equality comparator.
   */
  kEqual,
  /**
   * @brief Inequality comparator.
   */
  kInequal,
  /**
   * @brief Greater than comparator.
   */
  kGreater,
  /**
   * @brief Less than comparator.
   */
  kLess,
  /**
   * @brief Greater than or equal comparator.
   */
  kGreaterEqual,
  /**
   * @brief Less than or equal comparator.
   */
  kLessEqual
};

/**
 * @brief Operator for bit-vector
 */
enum BitVectorOpt
{
  /**
   * @brief Set the bit-vector.
   */
  kSet,
  /**
   * @brief Set the bit-vector, and then perform logical AND with another bit-vector.
   */
  kAnd,
  /**
   * @brief Set the bit-vector, and then perform logical OR with another bit-vector.
   */
  kOr
};

#define bceil(X, Y) (((X) + (Y) - 1) / (Y))
#define bfloor(X, Y) ((X) / (Y))

} // namespace bitweaving

#endif // BITWEAVING_INCLUDE_TYPES_H_
