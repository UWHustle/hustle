// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <cassert>

#include "column.h"
#include "column_iterator.h"

namespace hustle::bitweaving {

ColumnIterator::ColumnIterator(Column * const column)
  :column_(column)
{
}

ColumnIterator::~ColumnIterator()
{
}

Status ColumnIterator::GetCode(TupleId tuple_id, Code & code)
{
  return column_->GetCode(tuple_id, code);
}

Status ColumnIterator::SetCode(TupleId tuple_id, Code code)
{
  return column_->SetCode(tuple_id, code);
}

} // namespace bitweaving


