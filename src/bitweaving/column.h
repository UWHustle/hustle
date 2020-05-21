// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_INCLUDE_COLUMN_H_
#define BITWEAVING_INCLUDE_COLUMN_H_

#include <vector>

#include "types.h"
#include "table.h"
#include "bitvector.h"
#include "iterator.h"
#include "column_block.h"

namespace hustle::bitweaving {

class BitVector;
template <size_t CODE_SIZE>
class BwHColumnIterator;
template <size_t CODE_SIZE>
class BwVColumnIterator;
class ColumnBlock;
class ColumnIterator;
class SequentialWriteFile;

class AppendResult;

/**
 * @brief Class for storing and accessing codes in a column.
 */
class Column {
public:
  ~Column();

  /**
   * @brief Get the type of this column. Naive column, BitWeaving/H
   * column, or BitWeaving/V column.
   * @return The type of this column.
   */
  ColumnType GetType() const;

  /**
   * @brief Get the size of a code in this column.
   * @return The code size
   */
  size_t GetBitWidth() const;

  /**
   * @brief Get the ID of this column.
   * @return The ID of this column
   */
  ColumnId GetColumnId() const;

    /**
     * @brief Get the max code in this column.
     * @return The max code
     */
    size_t GetMaxCode() const;

    /**
     * @brief Get the num of values thus far in this column
     * @return num_ - The num of values thus far in this column
     */
    size_t GetNumValuesInColumn () const;

  /**
   * @brief Load an set of codes into this column. Each code in this array
   * should be in the domain of the codes, i.e. the number of bits that are
   * used to represent each code should be less than or equal to GetBitWidth().
   * @param codes The starting address of the input code array.
   * @param num The number of codes in the input code array.
   * @return A Status object to indicate success or failure.
   */
  AppendResult* Append(const Code * codes, size_t num);

  /**
   * @brief Write/update an set of codes into this column. Each code in this array
   * should be in the domain of the codes, i.e. the number of bits that are
   * used to represent each code should be less than or equal to GetBitWidth().
   * @param pos The starting position(tuple) where the first code will be written to.
   * @param codes The starting address of the input code array.
   * @param num The number of codes in the input code array.
   * @return A Status object to indicate success or failure.
   */
  AppendResult* Write(TupleId pos, const Code * codes, size_t num);

  /**
   * @brief Scan this column with an input literal-based predicate. The scan results
   * are combined with the input bit-vector based on a logical operator.
   * @param comparator The comparator between column codes and the literal.
   * @param literal The code to be compared with.
   * @param bitvector The bit-vector to be updated based on scan results.
   * @param opt The operator indicates how to combine the scan results to the input
   * bit-vector.
   * @return A Status object to indicate success or failure.
   */
  Status Scan(Comparator comparator, Code literal,
      BitVector & bitvector, BitVectorOpt opt = kSet) const;

  /**
   * @brief Scan this column with an predicate that compares this column with another
   * column. The scan results are combined with the input bit-vector based on a logical
   * operator.
   * @param comparator The comparator between column codes and the literal.
   * @param column The column to be compared with.
   * @param bitvector The bit-vector to be updated based on scan results.
   * @param opt The operator indicates how to combine the scan results to the input
   * bit-vector.
   * @return A Status object to indicate success or failure.
   */
  Status Scan(Comparator comparator, Column & column,
      BitVector & bitvector, BitVectorOpt opt = kSet) const;

  friend class Iterator;
  template <size_t CODE_SIZE>
  friend class BwHColumnIterator;
  template <size_t CODE_SIZE>
  friend class BwVColumnIterator;
  friend class ColumnIterator;
private:
  /**
   * @brief Private constructor. Call Table::addColumn to create a column.
   * @param id The ID of this column.
   * @param type The type of this column (Naive/BitWeavingH/BitWeavingV).
   * @param bit_width The number of bits to represent a code.
   * @param num The number of codes in this column.
   */
  Column(ColumnId id, ColumnType type, size_t bit_width, size_t & num);

  /**
   * @brief Get a code at a given position.
   * @warning This function is used for debugging. It is much more efficient to use
   *  Iterator::GetCode.
   * @param pos The code position to get.
   * @param code The code value.
   * @return A Status object to indicate success or failure.
   */
  Status GetCode(TupleId pos, Code & code) const;

  /**
   * @brief Set a code at a given position.
   * @warning This function is used for debugging. It is much more efficient to use
   *  Iterator::SetCode.
   * @param pos The code position to set.
   * @param code The code value.
   * @return A Status object to indicate success or failure.
   */
  Status SetCode(TupleId pos, Code code);

  /**
   * @brief Greate a new column block in GetType() format.
   * @return The new column block.
   */
  ColumnBlock* CreateColumnBlock() const;

  /**
   * @brief Greate an iterator for this column.
   * @return The iterator.
   */
  ColumnIterator* CreateColumnIterator();

  /**
   * @brief Get a ColumnBlock by its ID.
   * @param column_block_id The ID to search for.
   * @return The block with the given ID.
   */
  ColumnBlock* GetColumnBlock(size_t column_block_id) const;

  /**
   * @brief Reset the size of this column. Shrink or extend this column.
   * @param num The number of codes for this column.
   * @return A Status object to indicate success or failure.
   */
  Status Resize(size_t num);

  /**
   * @brief Save the column into a file.
   * @param file The file holds the data of this column.
   * @return A Status object to indicate success or failure.
   */
  Status Save(SequentialWriteFile & file) const;

  struct Rep;
  Rep * rep_;

  friend class Table;
};

} // namespace bitweaving

#endif // BITWEAVING_INCLUDE_COLUMN_H_
