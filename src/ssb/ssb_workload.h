#ifndef HUSTLE_SSB_WORKLOAD_H
#define HUSTLE_SSB_WORKLOAD_H

#include "table/table.h"
#include "execution/ExecutionPlan.hpp"
namespace hustle::operators {
class SSB {

public:

    SSB();

    void execute(ExecutionPlan &plan, std::shared_ptr<OperatorResult> &final_result);

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

    std::shared_ptr <Table> lo, c, s, p, d;
    std::shared_ptr <arrow::Schema> lo_schema, c_schema, s_schema, p_schema, d_schema;

private:

    std::shared_ptr<OperatorResult> lo_select_result;
    std::shared_ptr<OperatorResult> d_select_result;
    std::shared_ptr<OperatorResult> p_select_result;
    std::shared_ptr<OperatorResult> s_select_result;
    std::shared_ptr<OperatorResult> c_select_result;

    std::shared_ptr<OperatorResult> select_result_out;
    std::shared_ptr<OperatorResult> lip_result_out;
    std::shared_ptr<OperatorResult> join_result_out;
    std::shared_ptr<OperatorResult> agg_result_out;







};

}
#endif //HUSTLE_SSB_WORKLOAD_H
