// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_INCLUDE_BITVECTOR_H_
#define BITWEAVING_INCLUDE_BITVECTOR_H_

#include <assert.h>

#include "types.h"
#include "status.h"

namespace hustle::bitweaving {

class BitVectorIterator;
class BitVectorIteratorTest;
class BitVectorBlock;
class IteratorTest;
class BWTable;

/**
 * @brief Class of a bit-vector to indicate the results of a scan.
 */
class BitVector {
public:
  ~BitVector();

  /**
   * @brief Assignment operator.
   * @param bit_vector The bit-vector to be assigned to.
   */
  void operator=(const BitVector & bit_vector);

  /**
   * @brief Comparator
   * @param bit_vector The bit-vector to be compared with.
   * @return True iff the two bit-vectors are identical.
   */
  bool operator==(const BitVector & bit_vector) const;

  /**
   * @brief Get the number of bits in this bit-vector.
   * @return The number of bits in this bit-vector.
   */
  size_t GetNum() const;

  /**
   * @brief Get the number of BitVectorBlock in this bit-vector.
   * Each BitVectorBlock is a block that contains a fixed-length bitmap.
   * @return The block with the given ID.
   */
  size_t GetNumBitVectorBlock() const;

  /**
   * @brief Get a BitVectorBlock by its ID.
   * @param block_id The ID to search for.
   * @return The block with the given ID.
   */
  BitVectorBlock & GetBitVectorBlock(size_t block_id) const;

  /**
   * @brief Set all bits in this bit-vector to be 0-bit.
   */
  void SetZeros();

  /**
   * @brief Set all bits in this bit-vector to be 1-bit.
   */
  void SetOnes();

  /**
   * @brief Count the number of 1-bit in this bit-vector.
   * @return The number of 1-bit.
   */
  size_t Count() const;

  /**
   * @brief Perform logical complement on this bit-vector.
   * Change all 1-bit to 0-bit, and 0-bit to 1-bit.
   */
  void Complement();

  /**
   * @brief Perform logical AND with the input bit-vector.
   * @param bit_vector The input bit-vector for logical AND operation.
   * @return A Status object to indicate success or failure.
   */
  Status And(const BitVector & bit_vector);

  /**
   * @brief Perform logical OR with the input bit-vector.
   * @param bit_vector The input bit-vector for logical OR operation.
   * @return A Status object to indicate success or failure.
   */
  Status Or(const BitVector & bit_vector);

  /**
   * @brief Conver this bit-vector to a string.
   * @warning This function is only used for debugging.
   * @return A string that represents this bit-vector.
   */
  std::string ToString() const;

  friend class BitVectorIterator;
  friend class BitVectorIteratorTest;
  friend class IteratorTest;
  friend class BWTable;
private:
  /**
   * @brief Private constructor. Call Table::CreateBitVector to
   * create a bit-vector
   * @param num The number of bits in this bit-vector.
   */
  BitVector(size_t num);

  struct Rep;
  Rep * rep_;
};

} // namespace bitweaving

#endif // BITWEAVING_INCLUDE_BITVECTOR_H_
