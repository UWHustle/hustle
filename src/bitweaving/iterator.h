// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_INCLUDE_ITERATOR_H_
#define BITWEAVING_INCLUDE_ITERATOR_H_

#include <cassert>

#include "types.h"
#include "status.h"
#include "table.h"
#include "arrow/array.h"

namespace hustle::bitweaving {

class BWTable;
class BitVector;
class BitVectorIterator;
class Column;

/**
 * @brief Class for accessing codes in a table for matching
 * tuples defined by a bit vector.
 */
class Iterator {
public:
  ~Iterator();

  /**
   * @brief Seeks to the position of the first tuple in the table.
   */
  void Begin();

  /**
   * @brief Moves to the position of the next matching tuple in the
   * table, whose associated bit is set in the input bit vector.
   * @return Ture iff the next tuple exists.
   */
  bool Next();

  /**
   * @brief Gets the code at the current position (TupleId) in the
   * input column.
   * @param column Column object to access.
   * @param code   Code to return.
   * @return A Status object to indicate success or failure.
   */
  Status GetCode(Column & column , Code & code) const;

  /**
   * @brief Sets the code at the current position (TupleId) in the
   * input column.
   * @param column Column object to set.
   * @param code   Code to set.
   * @return A Status object to indicate success or failure.
   */
  Status SetCode(Column & column, Code code) const;

    /**
   * Fill the result bits into the Arrow data structure passed as an argument
   * @param out ArrayData - The arrow data structure into which the result is
   * to be filled into
   */
    void FillDataIntoArrowFormat(std::shared_ptr<arrow::ArrayData> out);

  friend class BWTable;
private:
  /**
   * @brief Constructs a new iterator for the input bit vector.
   * @ param bitvector BitVector object that is used to indicate
   * matching tuples
   */
  Iterator(const BWTable & table, const BitVector & bitvector);

  struct Rep;
  Rep * rep_;
};

} // namespace bitweaving

#endif // BITWEAVING_INCLUDE_ITERATOR_H_
