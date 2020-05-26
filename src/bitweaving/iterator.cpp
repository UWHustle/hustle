// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <cassert>
#include <cassert>
#include <arrow/array.h>

#include "column.h"
#include "bitwise.h"
#include "bitvector_iterator.h"
#include "column_iterator.h"

namespace hustle::bitweaving {

/**
 * @brief Structure for private members of Class Iterator.
 */
struct Iterator::Rep {
  Rep(const BWTable & _table, const BitVector & _bitvector)
      : table(_table), bitvector_iterator(_bitvector)
  {
    max_column_id = table.GetMaxColumnId();
    column_iterators = new ColumnIterator *[max_column_id];
    for (ColumnId id = 0; id < max_column_id; id++) {
      column_iterators[id] = table.GetColumn(id)->CreateColumnIterator();
    }
  }

  ~Rep()
  {
    delete [] column_iterators;
  }

  /**
   * @brief The associated table.
   */
  const BWTable & table;
  /**
   * @brief A bit-vector iterator on its associated bit-vector.
   */
  BitVectorIterator bitvector_iterator;
  /**
   * @brief The number of columns in its associated table.
   */
  ColumnId max_column_id;
  /**
   * @brief A list of column iterators on columns of its associated table.
   */
  ColumnIterator ** column_iterators;
};

Iterator::Iterator(const BWTable & table, const BitVector & bitvector)
  : rep_(new Rep(table, bitvector))
{
}

Iterator::~Iterator()
{
  delete rep_;
}

void Iterator::Begin()
{
  rep_->bitvector_iterator.Begin();
}

bool Iterator::Next()
{
  return rep_->bitvector_iterator.Next();
}

void Iterator::FillDataIntoArrowFormat(std::shared_ptr<arrow::ArrayData> out) {
    rep_->bitvector_iterator.FillDataIntoArrowFormat(out);
}


Status Iterator::GetCode(Column & column, Code & code) const
{
  ColumnId column_id = column.GetColumnId();
  assert(column_id < rep_->max_column_id);
  return rep_->column_iterators[column_id]->GetCode(
      rep_->bitvector_iterator.GetPosition(), code);
}

Status Iterator::SetCode(Column & column, Code code) const
{
  ColumnId column_id = column.GetColumnId();
  assert(column_id < rep_->max_column_id);
  return rep_->column_iterators[column_id]->SetCode(
      rep_->bitvector_iterator.GetPosition(), code);
}

} // namespace bitweaving


