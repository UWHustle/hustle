#ifndef HUSTLE_AGGREGATE_H
#define HUSTLE_AGGREGATE_H

#include <string>
#include <table/block.h>
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
  Aggregate(AggregateKernels aggregate_kernel, std::string column_name);

  // Operator.h
  std::vector<std::shared_ptr<Block>> runOperator(std::vector<std::vector<std::shared_ptr<Block>>> block_groups) override;

 private:
  AggregateKernels aggregate_kernel_;
  std::string column_name_;

};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_AGGREGATE_H
