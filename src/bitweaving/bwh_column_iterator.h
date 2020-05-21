// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_SRC_BWH_COLUMN_ITERATOR_H_
#define BITWEAVING_SRC_BWH_COLUMN_ITERATOR_H_

#include <cassert>

#include "types.h"
#include "status.h"
#include "column.h"
#include "column_iterator.h"
#include "bwh_column_block.h"

namespace hustle::bitweaving {

/**
 * @brief Class for accessing codes in a BitWeaving/H column.
 * This class stores state information to speedup in-order accesses
 * on codes in this column.
 */
template <size_t CODE_SIZE>
class BwHColumnIterator : public ColumnIterator {
public:
  /**
   * @brief Constructs a new iterator for a column.
   * @ param column The column to access.
   */
  BwHColumnIterator(Column * const column);

  /**
   * @brief Destructor.
   */
  virtual ~BwHColumnIterator();

  /**
   * @brief Gets the code at the current position (TupleId)
   * in this column.
   * @param tuple_id The ID of the code to access.
   * @param code   Code to return.
   * @return A Status object to indicate success or failure.
   */
  virtual Status GetCode(TupleId tuple_id, Code & code);

  /**
   * @brief Sets the code at the current position (TupleId)
   * in this column.
   * @param tuple_id The ID of the code to set.
   * @param code   Code to set.
   * @return A Status object to indicate success or failure.
   */
  virtual Status SetCode(TupleId tuple_id, Code code);

private:
  inline void Seek(TupleId tuple_id);

  static const size_t NUM_BITS_PER_CODE = CODE_SIZE + 1;
  static const size_t NUM_WORDS_PER_SEGMENT = CODE_SIZE + 1;
  static const size_t NUM_CODES_PER_WORD = NUM_BITS_PER_WORD / NUM_BITS_PER_CODE;
  static const size_t NUM_CODES_PER_SEGMENT = NUM_WORDS_PER_SEGMENT * NUM_CODES_PER_WORD;
  static const WordUnit CODE_MASK = (1ULL << CODE_SIZE) - 1;

  TupleId last_tuple_id_;
  size_t segment_word_id_;
  size_t code_id_in_segment_;
  size_t code_id_in_block_;
  size_t column_block_id_;
  BwHColumnBlock<CODE_SIZE> * column_block_;

  struct PrecomputedData {
    size_t word_id_in_segment;
    size_t shift_in_word;
  };

  PrecomputedData pre_data_[NUM_CODES_PER_SEGMENT];
};

} // namespace bitweaving

#endif // BITWEAVING_SRC_BWH_COLUMN_ITERATOR_H_

