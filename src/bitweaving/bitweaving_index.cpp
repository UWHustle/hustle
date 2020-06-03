//
// Created by Sandhya Kannan on 6/2/20.
//

#include "bitweaving_index.h"
#include "bitweaving_util.h"
#include "bitweaving_compare.h"
#include <table/Index.h>
#include <table/util.h>

namespace hustle::bitweaving {

arrow::compute::Datum BitweavingIndex::scan(std::shared_ptr<Predicate>& p) {

  Column *column = this->bwTable->GetColumn(p->col_ref_.col_name);
  std::shared_ptr<bitweaving::Code> scalar = std::make_shared<bitweaving::Code>
      (((arrow::UInt64Scalar *) p->value_.scalar().get())->value);

  BitweavingCompareOptions options(column, {BitweavingCompareOptionsUnit(scalar,
      GetBitweavingCompareOperator(p->comparator_),  std::make_shared<bitweaving::BitVectorOpt>(bitweaving::kSet))});

  arrow::Status status;
  arrow::compute::FunctionContext function_context(
      arrow::default_memory_pool());
  auto *resultBitVector = new arrow::compute::Datum();

  status = BitweavingCompare(
      &function_context,
      this->bwTable,
      {options},
      resultBitVector
  );
  evaluate_status(status, __FUNCTION__, __LINE__);

  return *resultBitVector;
}

void BitweavingIndex::createIndex(std::shared_ptr<Table> table, std::vector<std::string> cols) {

  std::vector<BitweavingColumnIndexUnit> col_units;
  col_units.reserve(cols.size());
  for(const std::string& col : cols) {
    col_units.emplace_back(col);
  }

  this->bwTable = createBitweavingIndex(table, col_units, true);
  table_index_map[table] = this;
}
bool BitweavingIndex::isColumnIndexed(std::string col_name) {
  assert(this->bwTable != nullptr);
  return this->bwTable->GetColumn(col_name) != nullptr;
}

}
