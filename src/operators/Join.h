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
    Join(std::string left_column_name, std::string right_column_name);

  // TODO(nicholas): These function are not implemented.
  std::shared_ptr<Table> runOperator
  (std::vector<std::shared_ptr<Table>> table) override;
  std::vector<std::shared_ptr<Block>> runOperator(
          std::shared_ptr<arrow::Schema> out_schema,
            arrow::compute::Datum left_join_val, std::shared_ptr<Table> right);

  /**
   * Perform a natural join on two tables using hash join. Projections are not
   * yet supported; all columns from both tables will be returned in the
   * resulting table (excluding the duplicate join column).
   *
   * @param left_table The table that will probe the hash table
   * @param right_table The table for which a hash table is built
   * @return A new table containing the results of the join
   */
  std::shared_ptr<Table> hash_join(std::shared_ptr<Table> left_table, std::shared_ptr<Table>
          right_table);

private:
    std::string left_join_column_name_;
    std::string right_join_column_name_;

};

} // namespace operators
} // namespace hustle

#endif //HUSTLE_JOIN_H
