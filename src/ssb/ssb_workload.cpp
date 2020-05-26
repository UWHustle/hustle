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

    lo_d_ref = {lo, "order date"};
    lo_p_ref = {lo, "part key"};
    lo_s_ref = {lo, "supp key"};
    lo_c_ref = {lo, "cust key"};

    d_ref = {d, "date key"};
    p_ref = {p, "part key"};
    s_ref = {s, "supp key"};
    c_ref = {c, "cust key"};

    d_join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
    p_join_pred = {lo_p_ref, arrow::compute::EQUAL, p_ref};
    s_join_pred = {lo_s_ref, arrow::compute::EQUAL, s_ref};
    c_join_pred = {lo_c_ref, arrow::compute::EQUAL, c_ref};

    reset_results();
}

void SSB::reset_results() {


}

void SSB::execute(ExecutionPlan &plan, std::shared_ptr<OperatorResult> &final_result) {

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

    auto select_result_out = std::make_shared<OperatorResult>();
    auto lip_result_out    = std::make_shared<OperatorResult>();
    auto join_result_out   = std::make_shared<OperatorResult>();
    auto agg_result_out    = std::make_shared<OperatorResult>();

    auto lo_select_result = std::make_shared<OperatorResult>();
    auto d_select_result  = std::make_shared<OperatorResult>();
    auto p_select_result  = std::make_shared<OperatorResult>();
    auto s_select_result  = std::make_shared<OperatorResult>();
    auto c_select_result  = std::make_shared<OperatorResult>();

    d_select_result->append(d);
    p_select_result->append(p);
    s_select_result->append(s);
    c_select_result->append(c);

    ////////////////////////////////////////////////////////////////////////////

    Select lo_select_op(0, lo_select_result, select_result_out, lo_pred_tree);
    Select d_select_op(0, d_select_result, select_result_out, d_pred_tree);

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

    Scheduler &scheduler = Scheduler::GlobalInstance();
    scheduler.addTask(&plan);

    auto container = simple_profiler.getContainer();
    container->startEvent("query execution");
    scheduler.start();
    scheduler.join();
    container->endEvent("query execution");

    out_table = agg_result_out->materialize({{nullptr, "revenue"}});
    out_table->print();
    simple_profiler.summarizeToStream(std::cout);

    simple_profiler.zero_time();
    reset_results();
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

    auto select_result_out = std::make_shared<OperatorResult>();
    auto lip_result_out    = std::make_shared<OperatorResult>();
    auto join_result_out   = std::make_shared<OperatorResult>();
    auto agg_result_out    = std::make_shared<OperatorResult>();

    auto lo_select_result = std::make_shared<OperatorResult>();
    auto d_select_result  = std::make_shared<OperatorResult>();
    auto p_select_result  = std::make_shared<OperatorResult>();
    auto s_select_result  = std::make_shared<OperatorResult>();
    auto c_select_result  = std::make_shared<OperatorResult>();

    d_select_result->append(d);
    p_select_result->append(p);
    s_select_result->append(s);
    c_select_result->append(c);

    ////////////////////////////////////////////////////////////////////////////

    Select lo_select_op(0, lo_select_result, select_result_out, lo_pred_tree);
    Select d_select_op(0, d_select_result, select_result_out, d_pred_tree);

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

    Scheduler &scheduler = Scheduler::GlobalInstance();
    scheduler.addTask(&plan);

    auto container = simple_profiler.getContainer();
    container->startEvent("query execution");
    scheduler.start();
    scheduler.join();
    container->endEvent("query execution");

    out_table = agg_result_out->materialize({{nullptr, "revenue"}});
    out_table->print();
    simple_profiler.summarizeToStream(std::cout);

    simple_profiler.zero_time();
    reset_results();
}

void SSB::q41() {

    auto s_pred_1 = Predicate{
        {s,
         "region"},
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
         "region"},
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

    auto select_result_out = std::make_shared<OperatorResult>();
    auto lip_result_out    = std::make_shared<OperatorResult>();
    auto join_result_out   = std::make_shared<OperatorResult>();
    auto agg_result_out    = std::make_shared<OperatorResult>();

    auto lo_select_result = std::make_shared<OperatorResult>();
    auto d_select_result  = std::make_shared<OperatorResult>();
    auto p_select_result  = std::make_shared<OperatorResult>();
    auto s_select_result  = std::make_shared<OperatorResult>();
    auto c_select_result  = std::make_shared<OperatorResult>();

    d_select_result->append(d);
    p_select_result->append(p);
    s_select_result->append(s);
    c_select_result->append(c);

    ////////////////////////////////////////////////////////////////////////////

    select_result_out->append(lo);
    select_result_out->append(d);

    Select p_select_op(0, p_select_result, select_result_out, p_pred_tree);
    Select s_select_op(0, s_select_result, select_result_out, s_pred_tree);
    Select c_select_op(0, c_select_result, select_result_out, c_pred_tree);

    JoinGraph graph({{s_join_pred, c_join_pred, p_join_pred, d_join_pred}});
    Join join_op(0, select_result_out, join_result_out, graph);

    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", {lo, "revenue"}};
    Aggregate agg_op(0,
                     join_result_out, agg_result_out, {agg_ref},
                     {{d, "year"}, {c, "nation"}, {p, "category"}},
                     {{d, "year"}, {c, "nation"}, {p, "category"}});

    ////////////////////////////////////////////////////////////////////////////

    ExecutionPlan plan(0);
    auto p_select_id = plan.addOperator(&p_select_op);
    auto s_select_id = plan.addOperator(&s_select_op);
    auto c_select_id = plan.addOperator(&c_select_op);

    auto join_id = plan.addOperator(&join_op);
    auto agg_id = plan.addOperator(&agg_op);

    // Declare join dependency on select operators
    plan.createLink(p_select_id, join_id);
    plan.createLink(s_select_id, join_id);
    plan.createLink(c_select_id, join_id);

    // Declare aggregate dependency on join operator
    plan.createLink(join_id, agg_id);

    Scheduler &scheduler = Scheduler::GlobalInstance();
    scheduler.addTask(&plan);

    auto container = simple_profiler.getContainer();
    container->startEvent("query execution");
    scheduler.start();
    scheduler.join();
    container->endEvent("query execution");

    out_table = agg_result_out->materialize({{nullptr, "revenue"}});
    out_table->print();
    simple_profiler.summarizeToStream(std::cout);

    simple_profiler.zero_time();
    reset_results();
}

void SSB::q42() {

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
         "region"},
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
         "region"},
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

    auto select_result_out = std::make_shared<OperatorResult>();
    auto lip_result_out    = std::make_shared<OperatorResult>();
    auto join_result_out   = std::make_shared<OperatorResult>();
    auto agg_result_out    = std::make_shared<OperatorResult>();

    auto lo_select_result = std::make_shared<OperatorResult>();
    auto d_select_result  = std::make_shared<OperatorResult>();
    auto p_select_result  = std::make_shared<OperatorResult>();
    auto s_select_result  = std::make_shared<OperatorResult>();
    auto c_select_result  = std::make_shared<OperatorResult>();

    d_select_result->append(d);
    p_select_result->append(p);
    s_select_result->append(s);
    c_select_result->append(c);

    ////////////////////////////////////////////////////////////////////////////

    select_result_out->append(lo);

    Select p_select_op(0, p_select_result, select_result_out, p_pred_tree);
    Select s_select_op(0, s_select_result, select_result_out, s_pred_tree);
    Select c_select_op(0, c_select_result, select_result_out, c_pred_tree);
    Select d_select_op(0, d_select_result, select_result_out, d_pred_tree);


    JoinGraph graph({{s_join_pred, c_join_pred, p_join_pred, d_join_pred}});
    Join join_op(0, select_result_out, join_result_out, graph);

    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", {lo, "revenue"}};
    Aggregate agg_op(0,
                     join_result_out, agg_result_out, {agg_ref},
                     {{d, "year"}, {c, "nation"}},
                     {{d, "year"}, {c, "nation"}});

    ////////////////////////////////////////////////////////////////////////////

    ExecutionPlan plan(0);
    auto p_select_id = plan.addOperator(&p_select_op);
    auto s_select_id = plan.addOperator(&s_select_op);
    auto c_select_id = plan.addOperator(&c_select_op);
    auto d_select_id = plan.addOperator(&d_select_op);


    auto join_id = plan.addOperator(&join_op);
    auto agg_id = plan.addOperator(&agg_op);

    // Declare join dependency on select operators
    plan.createLink(p_select_id, join_id);
    plan.createLink(s_select_id, join_id);
    plan.createLink(c_select_id, join_id);
    plan.createLink(d_select_id, join_id);

    // Declare aggregate dependency on join operator
    plan.createLink(join_id, agg_id);

    Scheduler &scheduler = Scheduler::GlobalInstance();
    scheduler.addTask(&plan);

    auto container = simple_profiler.getContainer();
    container->startEvent("query execution");
    scheduler.start();
    scheduler.join();
    container->endEvent("query execution");

    out_table = agg_result_out->materialize({{nullptr, "revenue"}});
    out_table->print();
    simple_profiler.summarizeToStream(std::cout);

    simple_profiler.zero_time();
    reset_results();
}

void SSB::q43() {

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
         "nation"},
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
         "region"},
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

    auto select_result_out = std::make_shared<OperatorResult>();
    auto lip_result_out    = std::make_shared<OperatorResult>();
    auto join_result_out   = std::make_shared<OperatorResult>();
    auto agg_result_out    = std::make_shared<OperatorResult>();

    auto lo_select_result = std::make_shared<OperatorResult>();
    auto d_select_result  = std::make_shared<OperatorResult>();
    auto p_select_result  = std::make_shared<OperatorResult>();
    auto s_select_result  = std::make_shared<OperatorResult>();
    auto c_select_result  = std::make_shared<OperatorResult>();

    d_select_result->append(d);
    p_select_result->append(p);
    s_select_result->append(s);
    c_select_result->append(c);

    ////////////////////////////////////////////////////////////////////////////

    select_result_out->append(lo);

    Select p_select_op(0, p_select_result, select_result_out, p_pred_tree);
    Select s_select_op(0, s_select_result, select_result_out, s_pred_tree);
    Select c_select_op(0, c_select_result, select_result_out, c_pred_tree);
    Select d_select_op(0, d_select_result, select_result_out, d_pred_tree);


    JoinGraph graph({{s_join_pred, c_join_pred, p_join_pred, d_join_pred}});
    Join join_op(0, select_result_out, join_result_out, graph);

    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", {lo, "revenue"}};
    Aggregate agg_op(0,
                     join_result_out, agg_result_out, {agg_ref},
                     {{d, "year"}, {c, "nation"}},
                     {{d, "year"}, {c, "nation"}});

    ////////////////////////////////////////////////////////////////////////////
    
    ExecutionPlan plan(0);
    auto p_select_id = plan.addOperator(&p_select_op);
    auto s_select_id = plan.addOperator(&s_select_op);
    auto c_select_id = plan.addOperator(&c_select_op);
    auto d_select_id = plan.addOperator(&d_select_op);

    auto join_id = plan.addOperator(&join_op);
    auto agg_id = plan.addOperator(&agg_op);

    // Declare join dependency on select operators
    plan.createLink(p_select_id, join_id);
    plan.createLink(s_select_id, join_id);
    plan.createLink(c_select_id, join_id);
    plan.createLink(d_select_id, join_id);

    // Declare aggregate dependency on join operator
    plan.createLink(join_id, agg_id);

    Scheduler &scheduler = Scheduler::GlobalInstance();
    scheduler.addTask(&plan);

    auto container = simple_profiler.getContainer();
    container->startEvent("query execution");
    scheduler.start();
    scheduler.join();
    container->endEvent("query execution");

    out_table = agg_result_out->materialize({{nullptr, "revenue"}});
    out_table->print();
    simple_profiler.summarizeToStream(std::cout);

    simple_profiler.zero_time();
    reset_results();
}

}