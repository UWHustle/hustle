#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <table/block.h>
#include <table/util.h>
#include "operators/Aggregate.h"
#include "operators/Join.h"
#include "operators/Select.h"
#include "scheduler/Scheduler.hpp"


#include <arrow/compute/kernels/filter.h>
#include <fstream>
#include <operators/LIP.h>
#include "execution/ExecutionPlan.hpp"
#include "EventProfiler.hpp"
#include "scheduler/SchedulerFlags.hpp"

using namespace hustle;
using namespace hustle::operators;

void execute_q11() {

    //discount >= 1
    auto discount_pred_1 = Predicate{
        {lo,
         "discount"},
        arrow::compute::CompareOperator::GREATER_EQUAL,
        arrow::compute::Datum((int64_t) 1)
    };
    auto discount_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(discount_pred_1));

    //discount <= 3
    auto discount_pred_2 = Predicate{
        {lo,
         "discount"},
        arrow::compute::CompareOperator::LESS_EQUAL,
        arrow::compute::Datum((int64_t) 3)
    };
    auto discount_pred_node_2 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(discount_pred_2));

    auto discount_connective_node = std::make_shared<ConnectiveNode>(
        discount_pred_node_1,
        discount_pred_node_2,
        FilterOperator::AND
    );

    //quantity < 25
    auto quantity_pred_1 = Predicate{
        {lo,
         "quantity"},
        arrow::compute::CompareOperator::LESS,
        arrow::compute::Datum((int64_t) 25)
    };
    auto quantity_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(quantity_pred_1));

    auto lo_root_node = std::make_shared<ConnectiveNode>(
        quantity_pred_node_1,
        discount_connective_node,
        FilterOperator::AND
    );

    auto lo_pred_tree = std::make_shared<PredicateTree>(lo_root_node);

    // date.year = 1993
    ColumnReference year_ref = {d, "year"};
    auto year_pred_1 = Predicate{
        {d,
         "year"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum((int64_t) 1993)
    };
    auto year_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(year_pred_1));
    auto d_pred_tree = std::make_shared<PredicateTree>(year_pred_node_1);

    ////////////////////////////////////////////////////////////////////////////

    auto lo_select_result = std::make_shared<OperatorResult>();
    auto d_select_result = std::make_shared<OperatorResult>();
    lo_select_result->append(lo);
    d_select_result->append(d);
    auto select_result_out = std::make_shared<OperatorResult>();

    Select lo_select_op(0, lo_select_result, select_result_out, lo_pred_tree);
    Select d_select_op(0, d_select_result, select_result_out, d_pred_tree);

    // Join date and lineorder tables
    ColumnReference lo_d_ref = {lo, "order date"};
    ColumnReference d_ref = {d, "date key"};
    ColumnReference revenue_ref = {lo, "revenue"};

    auto join_result_out = std::make_shared<OperatorResult>();
    JoinPredicate join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
    JoinGraph graph({{join_pred}});
    Join join_op(0, select_result_out, join_result_out, graph);

    auto agg_result_out = std::make_shared<OperatorResult>();
    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue",
                                  {lo, "revenue"}};
    Aggregate agg_op(0, join_result_out, agg_result_out, {agg_ref}, {}, {});

    ////////////////////////////////////////////////////////////////////////////

    Scheduler &scheduler = Scheduler::GlobalInstance();

    ExecutionPlan plan(0);
    auto lo_select_id = plan.addOperator(&lo_select_op);
    auto d_select_id = plan.addOperator(&d_select_op);
    auto join_id = plan.addOperator(&join_op);
    auto agg_id = plan.addOperator(&agg_op);

    // Declare join dependency on select operators
    plan.createLink(lo_select_id, join_id);
    plan.createLink(d_select_id, join_id);

    // Declare aggregate dependency on join operator
    plan.createLink(join_id, agg_id);

    scheduler.addTask(&plan);

    auto container = hustle::simple_profiler.getContainer();
    container->startEvent("query execution");
    scheduler.start();
    scheduler.join();
    container->endEvent("query execution");

    ////////////////////////////////////////////////////////////////////////////

    std::shared_ptr<Table> out_table;

    std::cout << std::endl;
    out_table = agg_result_out->materialize({{nullptr, "revenue"}});
    out_table->print();
    hustle::simple_profiler.summarizeToStream(std::cout);
}

int main(int argc, char *argv[]) {

    execute_q11();
}

}