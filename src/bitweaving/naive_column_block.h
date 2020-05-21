// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_SRC_NAIVE_COLUMN_BLOCK_H_
#define BITWEAVING_SRC_NAIVE_COLUMN_BLOCK_H_

#include <cassert>

#include "types.h"
#include "status.h"
#include "column.h"
#include "column_block.h"
#include "bitvector_block.h"

namespace hustle::bitweaving {

class BitVector;
class ColumnBlock;

/**
 * @brief Class for storing and accessing codes in a fixed-length naive column block.
 */
class NaiveColumnBlock : public ColumnBlock {
public:
  /**
   * @brief Constructor.
   */
  NaiveColumnBlock();

  /**
   * @brief Destructor.
   */
  virtual ~NaiveColumnBlock();

  /**
   * @brief Load an set of codes into this column block. Each code in this array
   * should be in the domain of the codes, i.e. the number of bits that are
   * used to represent each code should be less than or equal to GetBitWidth().
   * @param codes The starting address of the input code array.
   * @param num The number of codes in the input code array.
   * @return A Status object to indicate success or failure.
   */
  virtual AppendResult * Append(const Code * codes, size_t num);

  /**
   * @brief Load this column block from the file system.
   * @param file The name of data file.
   * @return A Status object to indicate success or failure.
   */
  virtual Status Load(SequentialReadFile & file);

  /**
   * @brief Save this column block to the file system.
   * @param file The name of data file.
   * @return A Status object to indicate success or failure.
   */
  virtual Status Save(SequentialWriteFile & file);

  /**
   * @brief Scan this column block with an input literal-based predicate. The scan results
   * are combined with the input bit-vector block based on a logical operator.
   * @param comparator The comparator between column codes and the literal.
   * @param code The code to be compared with.
   * @param bitvector The bit-vector block to be updated based on scan results.
   * @param opt The operator indicates how to combine the scan results to the input
   * bit-vector block.
   * @return A Status object to indicate success or failure.
   */
  virtual Status Scan(Comparator comparator, Code code,
      BitVectorBlock & bitvector, BitVectorOpt opt = kSet);

  /**
   * @brief Scan this column block with an predicate that compares this column block with
   * another column block. The scan results are combined with the input bit-vector block
   * based on a logical operator.
   * @param comparator The comparator between column codes and the literal.
   * @param column_block The column block to be compared with.
   * @param bitvector The bit-vector block to be updated based on scan results.
   * @param opt The operator indicates how to combine the scan results to the input
   * bit-vector block.
   * @return A Status object to indicate success or failure.
   */
  virtual Status Scan(Comparator comparator, ColumnBlock & column_block,
      BitVectorBlock & bitvector, BitVectorOpt opt = kSet);

  /**
   * @brief Get a code at a given position.
   * @warning This function is used for debugging. It is much more efficient to use
   *  Iterator::GetCode.
   * @param pos The code position to get.
   * @param code The code value.
   * @return A Status object to indicate success or failure.
   */
  Status GetCode(TupleId pos, Code & code) {
    if (pos >= num_)
      return Status::InvalidArgument("GetCode: position overflow");
    code = data_[pos];
    return Status::OK();
  }

  /**
   * @brief Set a code at a given position.
   * @warning This function is used for debugging. It is much more efficient to use
   *  Iterator::SetCode.
   * @param pos The code position to set.
   * @param code The code value.
   * @return A Status object to indicate success or failure.
   */
  Status SetCode(TupleId pos, Code code) {
    if (pos >= num_)
      return Status::InvalidArgument("GetCode: position overflow");
    data_[pos] = code;
    return Status::OK();
  }

private:
  WordUnit * data_;
};

} // namespace bitweaving

#endif // BITWEAVING_SRC_NAIVE_COLUMN_BLOCK_H_
