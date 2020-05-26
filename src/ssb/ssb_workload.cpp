#include <operators/Predicate.h>
#include <operators/Select.h>
#include <operators/Join.h>
#include <operators/Aggregate.h>
#include <operators/LIP.h>
#include <operators/JoinGraph.h>
#include <scheduler/Scheduler.hpp>
#include <execution/ExecutionPlan.hpp>
#include "ssb_workload.h"
#include "table/util.h"



namespace hustle::operators {

SSB::SSB() {
    lo = read_from_file("/Users/corrado/hustle/data/ssb-1/lineorder.hsl");
    d = read_from_file("/Users/corrado/hustle/data/ssb-1/date.hsl");
    p = read_from_file("/Users/corrado/hustle/data/ssb-1/part.hsl");
    c = read_from_file("/Users/corrado/hustle/data/ssb-1/customer.hsl");
    s = read_from_file("/Users/corrado/hustle/data/ssb-1/supplier.hsl");

    lo_select_result = std::make_shared<OperatorResult>();
    d_select_result  = std::make_shared<OperatorResult>();
    p_select_result  = std::make_shared<OperatorResult>();
    s_select_result  = std::make_shared<OperatorResult>();
    c_select_result  = std::make_shared<OperatorResult>();

    select_result_out = std::make_shared<OperatorResult>();
    lip_result_out    = std::make_shared<OperatorResult>();
    join_result_out   = std::make_shared<OperatorResult>();
    agg_result_out    = std::make_shared<OperatorResult>();
}

void SSB::execute(ExecutionPlan &plan, std::shared_ptr<OperatorResult> &final_result) {

    Scheduler &scheduler = Scheduler::GlobalInstance();
    scheduler.addTask(&plan);

    auto container = hustle::simple_profiler.getContainer();
    container->startEvent("query execution");
    scheduler.start();
    scheduler.join();
    container->endEvent("query execution");

    std::shared_ptr<Table> out_table;
    out_table = final_result->materialize({{nullptr, "revenue"}});
    out_table->print();
    hustle::simple_profiler.summarizeToStream(std::cout);
}

void SSB::q11() {

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

    lo_select_result->append(lo);
    d_select_result->append(d);

    Select lo_select_op(0, lo_select_result, select_result_out, lo_pred_tree);
    Select d_select_op(0, d_select_result, select_result_out, d_pred_tree);

    // Join date and lineorder tables
    ColumnReference lo_d_ref = {lo, "order date"};
    ColumnReference d_ref = {d, "date key"};
    ColumnReference revenue_ref = {lo, "revenue"};

    JoinPredicate join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
    JoinGraph graph({{join_pred}});
    Join join_op(0, select_result_out, join_result_out, graph);

    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue",
                                  {lo, "revenue"}};
    Aggregate agg_op(0, join_result_out, agg_result_out, {agg_ref}, {}, {});

    ////////////////////////////////////////////////////////////////////////////

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

    execute(plan, agg_result_out);
}


void SSB::q12() {

    //discount >= 4
    auto discount_pred_1 = Predicate{
        {lo,
         "discount"},
        arrow::compute::CompareOperator::GREATER_EQUAL,
        arrow::compute::Datum((int64_t) 4)
    };
    auto discount_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(discount_pred_1));

    //discount <= 6
    auto discount_pred_2 = Predicate{
        {lo,
         "discount"},
        arrow::compute::CompareOperator::LESS_EQUAL,
        arrow::compute::Datum((int64_t) 6)
    };
    auto discount_pred_node_2 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(discount_pred_2));

    auto discount_connective_node = std::make_shared<ConnectiveNode>(
        discount_pred_node_1,
        discount_pred_node_2,
        FilterOperator::AND
    );

    //quantity >= 26
    auto quantity_pred_1 = Predicate{
        {lo,
         "quantity"},
        arrow::compute::CompareOperator::GREATER_EQUAL,
        arrow::compute::Datum((int64_t) 26)
    };
    auto quantity_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(quantity_pred_1));

    //quantity <= 35
    auto quantity_pred_2 = Predicate{
        {lo,
         "quantity"},
        arrow::compute::CompareOperator::LESS_EQUAL,
        arrow::compute::Datum((int64_t) 35)
    };
    auto quantity_pred_node_2 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(quantity_pred_2));

    auto quantity_connective_node = std::make_shared<ConnectiveNode>(
        quantity_pred_node_1,
        quantity_pred_node_2,
        FilterOperator::AND
    );

    auto lo_root_node = std::make_shared<ConnectiveNode>(
        quantity_connective_node,
        discount_connective_node,
        FilterOperator::AND
    );

    auto lo_pred_tree = std::make_shared<PredicateTree>(lo_root_node);

    // date.year month num = 199401
    ColumnReference year_ref = {d, "year month num"};
    auto year_pred_1 = Predicate{
        {d,
         "year month num"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum((int64_t) 199401)
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




}