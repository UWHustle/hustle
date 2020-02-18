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
    std::shared_ptr<Block> runOperator(
            std::shared_ptr<Block> left, std::shared_ptr<Block> right,
            std::shared_ptr<Block> out);

 private:
  std::string column_name_;

};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_JOIN_H
