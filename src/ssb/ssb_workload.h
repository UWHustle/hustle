#ifndef HUSTLE_SSB_WORKLOAD_H
#define HUSTLE_SSB_WORKLOAD_H

#include <operators/Predicate.h>

#include "execution/ExecutionPlan.hpp"
#include "table/table.h"
namespace hustle::operators {
class SSB {
 public:
  explicit SSB(int SF = 1, bool print = false);

  void execute(ExecutionPlan &plan,
               std::shared_ptr<OperatorResult> &final_result);

  void q11();
  void q12();
  void q13();

  void q21();
  void q22();
  void q23();

  void q31();
  void q32();
  void q33();
  void q34();

  void q41();
  void q42();
  void q43();

  void q11_lip();
  void q12_lip();
  void q13_lip();

  void q21_lip();
  void q22_lip();
  void q23_lip();

  void q31_lip();
  void q32_lip();
  void q33_lip();
  void q34_lip();

  void q41_lip();
  void q42_lip();
  void q43_lip();

  std::shared_ptr<Table> lo, c, s, p, d;
  std::shared_ptr<arrow::Schema> lo_schema, c_schema, s_schema, p_schema,
      d_schema;

 private:
  bool print_;
  int num_threads_;

  std::shared_ptr<OperatorResult> lo_result_in;
  std::shared_ptr<OperatorResult> d_result_in;
  std::shared_ptr<OperatorResult> p_result_in;
  std::shared_ptr<OperatorResult> s_result_in;
  std::shared_ptr<OperatorResult> c_result_in;

  std::shared_ptr<OperatorResult> lo_select_result_out;
  std::shared_ptr<OperatorResult> d_select_result_out;
  std::shared_ptr<OperatorResult> p_select_result_out;
  std::shared_ptr<OperatorResult> s_select_result_out;
  std::shared_ptr<OperatorResult> c_select_result_out;

  std::vector<std::shared_ptr<OperatorResult>> lip_result_in;
  std::vector<std::shared_ptr<OperatorResult>> join_result_in;

  std::shared_ptr<OperatorResult> lip_result_out;
  std::shared_ptr<OperatorResult> join_result_out;
  std::shared_ptr<OperatorResult> agg_result_out;

  std::shared_ptr<Table> out_table;

  ColumnReference lo_d_ref;
  ColumnReference lo_p_ref;
  ColumnReference lo_s_ref;
  ColumnReference lo_c_ref;

  ColumnReference d_ref;
  ColumnReference p_ref;
  ColumnReference s_ref;
  ColumnReference c_ref;

  JoinPredicate d_join_pred;
  JoinPredicate p_join_pred;
  JoinPredicate s_join_pred;
  JoinPredicate c_join_pred;

  ColumnReference lo_rev_ref;
  ColumnReference d_year_ref;
  ColumnReference p_brand1_ref;
  ColumnReference p_category_ref;
  ColumnReference s_nation_ref;
  ColumnReference s_city_ref;
  ColumnReference c_nation_ref;
  ColumnReference c_city_ref;

  void reset_results();
};

}  // namespace hustle::operators
#endif  // HUSTLE_SSB_WORKLOAD_H
