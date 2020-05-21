// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_SRC_COLUMN_ITERATOR_H_
#define BITWEAVING_SRC_COLUMN_ITERATOR_H_

#include <cassert>

#include "types.h"
#include "status.h"
#include "column.h"

namespace hustle::bitweaving {

/**
 * @brief Class for accessing codes in a column. This class
 * stores state information to speedup in-order accesses on
 * codes in this column.
 */
class ColumnIterator {
public:
  /**
   * @brief Constructs a new iterator for a column.
   * @ param column The column to access.
   */
  ColumnIterator(Column * const column);

  /**
   * @brief Destructor.
   */
  virtual ~ColumnIterator();

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

protected:
  /**
   * @brief The column to access.
   */
  Column * const column_;
};

} // namespace bitweaving

#endif // BITWEAVING_SRC_COLUMN_ITERATOR_H_

