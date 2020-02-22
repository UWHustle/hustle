#ifndef HUSTLE_JOIN_H
#define HUSTLE_JOIN_H

#include <string>
#include <table/block.h>
#include <table/table.h>
#include <arrow/compute/api.h>

#include "Operator.h"

namespace hustle {
namespace operators {

struct record_id {
    int block_id;
    int row_index;
};

class Join : public Operator{
 public:
  explicit Join(std::string column_name);

  // Operator.h
  std::vector<std::shared_ptr<Block>> runOperator(std::vector<std::vector<std::shared_ptr<Block>>> block_groups) override;
  std::vector<std::shared_ptr<Block>> runOperator(
          std::shared_ptr<arrow::Schema> out_schema,
            arrow::compute::Datum left_join_val, std::shared_ptr<Table> right);

  std::shared_ptr<Table> hash_join(std::shared_ptr<Table> left_table, std::shared_ptr<Table>
          right_table);

 private:
  std::string column_name_;

};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_JOIN_H
