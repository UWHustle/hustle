#ifndef HUSTLE_SELECT_H
#define HUSTLE_SELECT_H

#include <string>
#include <table/block.h>
#include <arrow/compute/api.h>

#include "Operator.h"

namespace hustle {
namespace operators {

class Select : public Operator{
 public:
  Select(arrow::compute::CompareOperator compare_operator, std::string column_name, arrow::compute::Datum column_value);

  // Operator.h
  std::vector<std::shared_ptr<Block>> runOperator(std::vector<std::vector<std::shared_ptr<Block>>> block_groups) override;

private:
  arrow::compute::CompareOperator compare_operator_;
  std::string column_name_;
  arrow::compute::Datum column_value_;

};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_SELECT_H
