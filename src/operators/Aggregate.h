#ifndef HUSTLE_AGGREGATE_H
#define HUSTLE_AGGREGATE_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>

#include "Operator.h"

namespace hustle {
namespace operators {

enum AggregateKernels {
  SUM,
  COUNT,
  MEAN
};

class Aggregate : public Operator{
 public:
  Aggregate(AggregateKernels aggregate_kernel, std::string column_name,
          std::string group_by_column_name);

    std::unordered_map<std::string, arrow::compute::Datum>  get_groups
    (std::shared_ptr<Table> table);
  // Operator.h
  std::shared_ptr<Table> run_operator(std::vector<std::shared_ptr<Table>>
  tables) override;
//void set_children(
//            std::shared_ptr<Operator> left_child,
//            std::shared_ptr<Operator> right_child,
//            FilterOperator filter_operator) override;

 private:
  AggregateKernels aggregate_kernel_;
  std::string aggregate_column_name_;
  std::string group_by_column_name_;


    };

} // namespace operators
} // namespace hustle

#endif //HUSTLE_AGGREGATE_H
