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

void SSB::q41_lip() {

    auto s_pred_1 = Predicate{
        {s,
         "s region"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                                  ("AMERICA"))
    };
    auto s_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(s_pred_1));

    auto s_pred_tree = std::make_shared<PredicateTree>(s_pred_node_1);

    auto c_pred_1 = Predicate{
        {c,
         "c region"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                                  ("AMERICA"))
    };
    auto c_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(c_pred_1));

    auto c_pred_tree = std::make_shared<PredicateTree>(c_pred_node_1);

    auto p_pred_1 = Predicate{
        {p,
         "mfgr"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                                  ("MFGR#1"))
    };
    auto p_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(p_pred_1));

    auto p_pred_2 = Predicate{
        {p,
         "mfgr"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                                  ("MFGR#2"))
    };
    auto p_pred_node_2 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(p_pred_2));

    auto p_connective_node =
        std::make_shared<ConnectiveNode>(
            p_pred_node_1,
            p_pred_node_2,
            FilterOperator::OR
        );

    auto p_pred_tree = std::make_shared<PredicateTree>(p_connective_node);

    ////////////////////////////////////////////////////////////////////////////

    lo_select_result_out->append(lo);
    d_select_result_out->append(d);

    Select p_select_op(0, p_result_in, p_select_result_out, p_pred_tree);
    Select s_select_op(0, s_result_in, s_select_result_out, s_pred_tree);
    Select c_select_op(0, c_result_in, c_select_result_out, c_pred_tree);

    lip_result_in = {lo_select_result_out, d_select_result_out, p_select_result_out, s_select_result_out, c_select_result_out};

    JoinGraph graph({{s_join_pred, c_join_pred, p_join_pred, d_join_pred}});
    LIP lip_op(0, lip_result_in, lip_result_out, graph);
    Join join_op(0, {lip_result_out}, join_result_out, graph);

    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", lo_rev_ref};
    Aggregate agg_op(0,
                     join_result_out, agg_result_out, {agg_ref},
                     {d_year_ref, c_nation_ref},
                     {d_year_ref, c_nation_ref});

    ////////////////////////////////////////////////////////////////////////////

    ExecutionPlan plan(0);
    auto p_select_id = plan.addOperator(&p_select_op);
    auto s_select_id = plan.addOperator(&s_select_op);
    auto c_select_id = plan.addOperator(&c_select_op);

    auto lip_id = plan.addOperator(&lip_op);
    auto join_id = plan.addOperator(&join_op);
    auto agg_id = plan.addOperator(&agg_op);

    // Declare join dependency on select operators
    plan.createLink(p_select_id, lip_id);
    plan.createLink(s_select_id, lip_id);
    plan.createLink(c_select_id, lip_id);

    plan.createLink(lip_id, join_id);

    // Declare aggregate dependency on join operator
    plan.createLink(join_id, agg_id);

    ////////////////////////////////////////////////////////////////////////////

    Scheduler scheduler = Scheduler(8);
    scheduler.addTask(&plan);

    auto container = simple_profiler.getContainer();
    container->startEvent("query execution");
    scheduler.start();
    scheduler.join();
    container->endEvent("query execution");

    out_table = agg_result_out->materialize({{nullptr, "revenue"}, {nullptr, "year"}, {nullptr, "c nation"}});
//    out_table->print();
    simple_profiler.summarizeToStream(std::cout);

    simple_profiler.clear();
    reset_results();
}

void SSB::q42_lip() {

    auto d_pred_1 = Predicate{
        {d,
         "year"},
        arrow::compute::CompareOperator::GREATER_EQUAL,
        arrow::compute::Datum((int64_t) 1997)
    };

    auto d_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(d_pred_1));

    auto d_pred_2 = Predicate{
        {d,
         "year"},
        arrow::compute::CompareOperator::LESS_EQUAL,
        arrow::compute::Datum((int64_t) 1998)
    };

    auto d_pred_node_2 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(d_pred_2));

    auto d_connective_node =
        std::make_shared<ConnectiveNode>(
            d_pred_node_1,
            d_pred_node_2,
            FilterOperator::AND
        );

    auto d_pred_tree = std::make_shared<PredicateTree>(d_connective_node);

    auto s_pred_1 = Predicate{
        {s,
         "s region"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                                  ("AMERICA"))
    };
    auto s_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(s_pred_1));

    auto s_pred_tree = std::make_shared<PredicateTree>(s_pred_node_1);

    auto c_pred_1 = Predicate{
        {c,
         "c region"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                                  ("AMERICA"))
    };
    auto c_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(c_pred_1));

    auto c_pred_tree = std::make_shared<PredicateTree>(c_pred_node_1);

    auto p_pred_1 = Predicate{
        {p,
         "mfgr"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                                  ("MFGR#1"))
    };
    auto p_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(p_pred_1));

    auto p_pred_2 = Predicate{
        {p,
         "mfgr"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                                  ("MFGR#2"))
    };
    auto p_pred_node_2 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(p_pred_2));

    auto p_connective_node =
        std::make_shared<ConnectiveNode>(
            p_pred_node_1,
            p_pred_node_2,
            FilterOperator::OR
        );

    auto p_pred_tree = std::make_shared<PredicateTree>(p_connective_node);

    ////////////////////////////////////////////////////////////////////////////

    lo_select_result_out->append(lo);

    Select p_select_op(0, p_result_in, p_select_result_out, p_pred_tree);
    Select s_select_op(0, s_result_in, s_select_result_out, s_pred_tree);
    Select c_select_op(0, c_result_in, c_select_result_out, c_pred_tree);
    Select d_select_op(0, d_result_in, d_select_result_out, d_pred_tree);

    lip_result_in = {lo_select_result_out, d_select_result_out, p_select_result_out, s_select_result_out, c_select_result_out};

    JoinGraph graph({{s_join_pred, c_join_pred, p_join_pred, d_join_pred}});
    LIP lip_op(0, lip_result_in, lip_result_out, graph);
    Join join_op(0, {lip_result_out}, join_result_out, graph);

    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", lo_rev_ref};
    Aggregate agg_op(0,
                     join_result_out, agg_result_out, {agg_ref},
                     {d_year_ref, s_nation_ref, p_category_ref},
                     {d_year_ref, s_nation_ref, p_category_ref});

    ////////////////////////////////////////////////////////////////////////////

    ExecutionPlan plan(0);
    auto p_select_id = plan.addOperator(&p_select_op);
    auto s_select_id = plan.addOperator(&s_select_op);
    auto c_select_id = plan.addOperator(&c_select_op);
    auto d_select_id = plan.addOperator(&d_select_op);

    auto lip_id = plan.addOperator(&lip_op);
    auto join_id = plan.addOperator(&join_op);
    auto agg_id = plan.addOperator(&agg_op);

    // Declare join dependency on select operators
    plan.createLink(p_select_id, lip_id);
    plan.createLink(s_select_id, lip_id);
    plan.createLink(c_select_id, lip_id);
    plan.createLink(d_select_id, lip_id);

    plan.createLink(lip_id, join_id);

    // Declare aggregate dependency on join operator
    plan.createLink(join_id, agg_id);

    Scheduler scheduler = Scheduler(8);
    scheduler.addTask(&plan);

    auto container = simple_profiler.getContainer();
    container->startEvent("query execution");
    scheduler.start();
    scheduler.join();
    container->endEvent("query execution");

    out_table = agg_result_out->materialize({{nullptr, "revenue"}, {nullptr, "year"}, {nullptr, "s nation"}, {nullptr, "category"}});
//    out_table->print();
    simple_profiler.summarizeToStream(std::cout);

    simple_profiler.clear();
    reset_results();
}

void SSB::q43_lip() {

    auto d_pred_1 = Predicate{
        {d,
         "year"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum((int64_t) 1997)
    };

    auto d_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(d_pred_1));

    auto d_pred_2 = Predicate{
        {d,
         "year"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum((int64_t) 1998)
    };

    auto d_pred_node_2 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(d_pred_2));

    auto d_connective_node =
        std::make_shared<ConnectiveNode>(
            d_pred_node_1,
            d_pred_node_2,
            FilterOperator::OR
        );

    auto d_pred_tree = std::make_shared<PredicateTree>(d_connective_node);

    auto s_pred_1 = Predicate{
        {s,
         "s nation"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                                  ("UNITED STATES"))
    };
    auto s_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(s_pred_1));

    auto s_pred_tree = std::make_shared<PredicateTree>(s_pred_node_1);

    auto c_pred_1 = Predicate{
        {c,
         "c region"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                                  ("AMERICA"))
    };
    auto c_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(c_pred_1));

    auto c_pred_tree = std::make_shared<PredicateTree>(c_pred_node_1);

    auto p_pred_1 = Predicate{
        {p,
         "category"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                                  ("MFGR#14"))
    };
    auto p_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(p_pred_1));

    auto p_pred_tree = std::make_shared<PredicateTree>(p_pred_node_1);

    ////////////////////////////////////////////////////////////////////////////

    lo_select_result_out->append(lo);

    Select p_select_op(0, p_result_in, p_select_result_out, p_pred_tree);
    Select s_select_op(0, s_result_in, s_select_result_out, s_pred_tree);
    Select c_select_op(0, c_result_in, c_select_result_out, c_pred_tree);
    Select d_select_op(0, d_result_in, d_select_result_out, d_pred_tree);

    lip_result_in = {lo_select_result_out, d_select_result_out, p_select_result_out, s_select_result_out, c_select_result_out};

    JoinGraph graph({{s_join_pred, c_join_pred, p_join_pred, d_join_pred}});
    LIP lip_op(0, lip_result_in, lip_result_out, graph);
    Join join_op(0, {lip_result_out}, join_result_out, graph);

    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", lo_rev_ref};
    Aggregate agg_op(0,
                     join_result_out, agg_result_out, {agg_ref},
                     {d_year_ref, s_city_ref, p_brand1_ref},
                     {d_year_ref, s_city_ref, p_brand1_ref});


    ExecutionPlan plan(0);
    auto p_select_id = plan.addOperator(&p_select_op);
    auto s_select_id = plan.addOperator(&s_select_op);
    auto c_select_id = plan.addOperator(&c_select_op);
    auto d_select_id = plan.addOperator(&d_select_op);

    auto lip_id = plan.addOperator(&lip_op);
    auto join_id = plan.addOperator(&join_op);
    auto agg_id = plan.addOperator(&agg_op);

    // Declare join dependency on select operators
    plan.createLink(p_select_id, lip_id);
    plan.createLink(s_select_id, lip_id);
    plan.createLink(c_select_id, lip_id);
    plan.createLink(d_select_id, lip_id);

    plan.createLink(lip_id, join_id);

    // Declare aggregate dependency on join operator
    plan.createLink(join_id, agg_id);

    Scheduler scheduler = Scheduler(8);
    scheduler.addTask(&plan);

    auto container = simple_profiler.getContainer();
    container->startEvent("query execution");
    scheduler.start();
    scheduler.join();
    container->endEvent("query execution");

    out_table = agg_result_out->materialize({{nullptr, "revenue"}, {nullptr, "year"},  {nullptr, "s city"}, {nullptr, "brand1"}});
//    out_table->print();
    simple_profiler.summarizeToStream(std::cout);

    simple_profiler.clear();
    reset_results();
}

}