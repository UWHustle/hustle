#ifndef HUSTLE_OPERATOR_H
#define HUSTLE_OPERATOR_H

#include <string>
#include <table/block.h>

namespace hustle {
namespace operators {

class Operator{
 public:
  virtual std::vector<std::shared_ptr<Block>> runOperator(std::vector<std::vector<std::shared_ptr<Block>>> block_groups) = 0;

};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_OPERATOR_H
