// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_SRC_BWH_COLUMN_BLOCK_H_
#define BITWEAVING_SRC_BWH_COLUMN_BLOCK_H_

#include <cassert>

#include "types.h"
#include "status.h"
#include "column.h"
#include "column_block.h"
#include "bitvector_block.h"

namespace hustle::bitweaving {

class BitVector;
class ColumnBlock;
template <size_t CODE_SIZE>
class BwHColumnIterator;

/**
 * @brief Class for storing and accessing codes in a fixed-length
 * BitWeaving/H column block. We use template to generate specialized
 * class for each code size (1~32 bits). The template prarameter (the
 * code size) is a compile-time const, thus enables many compilation
 * optimizations to generate code.
 */
template <size_t CODE_SIZE>
class BwHColumnBlock : public ColumnBlock {
public:
  /**
   * @brief Constructor.
   */
  BwHColumnBlock();

  /**
   * @brief Destructor.
   */
  virtual ~BwHColumnBlock();

  /**
   * @brief Load an set of codes into this column block. Each code in this array
   * should be in the domain of the codes, i.e. the number of bits that are
   * used to represent each code should be less than or equal to GetBitWidth().
   * @param codes The starting address of the input code array.
   * @param num The number of codes in the input code array.
   * @return A Status object to indicate success or failure.
   */
  virtual AppendResult* Append(const Code * codes, size_t num);

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
   * @param literal The code to be compared with.
   * @param bitvector The bit-vector block to be updated based on scan results.
   * @param opt The operator indicates how to combine the scan results to the input
   * bit-vector block.
   * @return A Status object to indicate success or failure.
   */
  virtual Status Scan(Comparator comparator, Code literal,
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
  virtual Status GetCode(TupleId pos, Code & code);

  /**
   * @brief Set a code at a given position.
   * @warning This function is used for debugging. It is much more efficient to use
   *  Iterator::SetCode.
   * @param pos The code position to set.
   * @param code The code value.
   * @return A Status object to indicate success or failure.
   */
  virtual Status SetCode(TupleId pos, Code code);

  friend class BwHColumnIterator<CODE_SIZE>;
private:
  template <Comparator CMP>
  Status Scan(Code literal, BitVectorBlock & bitvector, BitVectorOpt opt);
  template <Comparator CMP, BitVectorOpt OPT>
  Status Scan(Code literal, BitVectorBlock & bitvector);

  template <Comparator CMP>
  Status Scan(ColumnBlock & column_block, BitVectorBlock & bitvector, BitVectorOpt opt);
  template <Comparator CMP, BitVectorOpt OPT>
  Status Scan(ColumnBlock & column_block, BitVectorBlock & bitvector);

  /**
   * @brief The data space.
   */
  WordUnit * data_;
  /**
   * @brief The size of data space.
   */
  size_t num_used_words_;

  static const size_t NUM_BITS_PER_CODE = CODE_SIZE + 1;
  static const size_t NUM_WORDS_PER_SEGMENT = CODE_SIZE + 1;
  static const size_t NUM_CODES_PER_WORD = NUM_BITS_PER_WORD / NUM_BITS_PER_CODE;
  static const size_t NUM_EMPTY_BITS = NUM_BITS_PER_WORD - NUM_CODES_PER_WORD * NUM_BITS_PER_CODE;
  static const size_t NUM_CODES_PER_SEGMENT = NUM_WORDS_PER_SEGMENT * NUM_CODES_PER_WORD;
  static const size_t NUM_SEGMENTS = bceil(NUM_CODES_PER_BLOCK, NUM_CODES_PER_SEGMENT);
  static const size_t NUM_WORDS = NUM_SEGMENTS * NUM_WORDS_PER_SEGMENT;
  static const size_t NUM_WORDS_PER_WORD_GROUP = 7;
  static const size_t NUM_WORD_GROUPS = NUM_WORDS_PER_SEGMENT / NUM_WORDS_PER_WORD_GROUP;
  static const size_t NUM_UNGROUPED_WORDS = NUM_WORDS_PER_SEGMENT - NUM_WORDS_PER_WORD_GROUP * NUM_WORD_GROUPS; 
  static const size_t PREFETCH_DISTANCE = 64;
};

}// namespace bitweaving

#endif // BITWEAVING_SRC_BWH_COLUMN_BLOCK_H_
