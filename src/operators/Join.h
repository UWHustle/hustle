#ifndef HUSTLE_JOIN_H
#define HUSTLE_JOIN_H

#include <string>
#include <table/block.h>
#include <arrow/compute/api.h>

#include "Operator.h"

namespace hustle {
namespace operators {

class Join : public Operator{
 public:
  explicit Join(std::string column_name);

  // Operator.h
  std::vector<std::shared_ptr<Block>> runOperator(std::vector<std::vector<std::shared_ptr<Block>>> block_groups) override;

 private:
  std::string column_name_;

};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_JOIN_H
