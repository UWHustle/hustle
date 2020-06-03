// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_INCLUDE_TABLE_H_
#define BITWEAVING_INCLUDE_TABLE_H_

#include <string>

#include "types.h"
#include "options.h"
#include "column.h"
#include "bitvector.h"
#include "iterator.h"

namespace hustle::bitweaving {

class Iterator;
class BitVector;
class AppendResult;

/**
 * @brief Class for storing and accessing data in a table, which contains a
 * set of Column.
 */
class BWTable {
public:
  /**
   * @brief Constructor.
   * @param path The location of its associated data file.
   * @param options The options control the table.
   */
  BWTable(const std::string& path, const Options& options);

  /**
   * @brief Destructor.
   */
  ~BWTable();

  /**
   * @brief Open the table. Load meta data if the file exists.
   * @return A Status object to indicate success or failure.
   */
  Status Open();

  /**
   * @brief Close and save the table.
   * @return A Status object to indicate success or failure.
   */
  Status Close();

  /**
   * @brief Write the table to the file.
   * @return A Status object to indicate success or failure.
   */
  Status Save();

  /**
   * @brief Create a bit-vector for this table. The number of bits in the
   * bit-vector matches the number of tuples in the table. The returned bit-
   * vector could be used to store the scan results on any column of this table.
   * @warning Caller should delete the bit-vector when it is no longer needed.
   * The returned bit-vector should be deleted before this table is deleted.
   * @return A bit-vector for this table.
   */
  BitVector* CreateBitVector() const;

  /**
   * @brief Create an iterator for this table. This iterator accesses every
   * tuple in this table.
   * @warning Caller should delete the iterator when it is no longer needed.
   * The returned iterator should be deleted before this table is deleted.
   * @return an iterator for this table.
   */
  Iterator* CreateIterator() const;

  /**
   * @brief Create an iterator for this table. This iterator accesses every
   * tuple whose associated bit is on in the input bit-vector.
   * @warning Caller should delete the iterator when it is no longer needed.
   * The returned iterator should be deleted before this table is deleted.
   * @param bitvector The bit-vector indicates matching tuples.
   * @return an iterator for this table.
   */
  Iterator* CreateIterator(const BitVector& bitvector) const;

  /**
   * @brief Add a new column into this table.
   * @param name The column name.
   * @param type Type type of the column (Naive/BitWeavingH/BitWeavingV).
   * @param bit_width The number of bits to represent a code in this column
   * @return A Status object to indicate success or failure.
   */
  Status AddColumn(const std::string& name, ColumnType type, size_t bit_width);

  /**
   * @brief Remove an existed column from this table.
   * @param name The column name.
   * @return A Status object to indicate success or failure.
   */
  Status RemoveColumn(const std::string& name);

  /**
   * @brief Get an attribute by its column name.
   * @param name The column name to search for.
   * @return The pointer to the attribute with the given name.
   **/
  Column* GetColumn(const std::string& name) const;

    /**
     * @brief Get the number of rows in the table.
     **/
  size_t GetNumRows();

  /**
   * Append the data in the codes array to the column specified
   * @param name - Name of the column to append the data to
   * @param codes - The data to be appended
   * @param num - number of values in the codes array
   * @return The status of the Append
   */
  AppendResult* AppendToColumn(const std::string& name, Code* codes, size_t num);

  friend class Iterator;
private:

  /**
   * @brief Load meta data file.
   * @param filename The name of meta data file.
   * @return A Status object to indicate success or failure.
   */
  Status LoadMetaFile(const std::string& filename);

  /**
   * @brief Save meta data file.
   * @param filename The name of meta data file.
   * @return A Status object to indicate success or failure.
   */
  Status SaveMetaFile(const std::string& filename);

  /**
   * @brief Get the maximum column ID.
   * @return The maximum column ID.
   */
  ColumnId GetMaxColumnId() const;

  /**
   * @brief Get an attribute by its column ID.
   * @param column_id The column ID to search for.
   * @return The pointer to the attribute with the given ID.
   **/
  Column* GetColumn(ColumnId column_id) const;

  /**
   * Helper method to remove the column and add it with the new given bitwidth and copy the codes thus far in the old col
   * @param old_col - The old_col to be removed
   * @param name - Name of the col (old and new)
   * @param old_col_size - Num of succesfully appended codes in old_col thus far
   * @param new_bitwidth - The new bit width
   * @return pointer to the new col
   */
  Column* RemoveAndAddColumn(Column* old_col, const std::string& name, size_t old_col_size, size_t new_bitwidth);

  /**
   * This method retrieves the existing column codes in the column. Used when re-adjusting the bit width for the column
   * @param old_col - The col for which the bit width is being optimized for
   * @param old_col_size - Size of the col
   * @return The pointer to the code array
   */
  Code* GetColumnCodesThusFar(Column* old_col, size_t old_col_size);

  struct Rep;
  Rep * rep_;
};

}// namespace bitweaving

#endif // BITWEAVING_INCLUDE_TABLE_H_
