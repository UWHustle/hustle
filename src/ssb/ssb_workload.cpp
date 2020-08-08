#include <operators/Predicate.h>
#include <operators/Select.h>
#include <operators/Join.h>
#include <operators/Aggregate.h>
#include <operators/LIP.h>
#include <operators/JoinGraph.h>
#include <scheduler/Scheduler.hpp>
#include <execution/ExecutionPlan.hpp>
#include <scheduler/SchedulerFlags.hpp>
#include "ssb_workload.h"
#include "table/util.h"
#include "utils/arrow_compute_wrappers.h"

using namespace std::chrono;

namespace hustle::operators {

SSB::SSB(int SF, bool print) {

    print_ = print;
    num_threads_ = std::thread::hardware_concurrency();
//    num_threads_ = 1;

    if (SF==0) {
        lo = read_from_file("../../../src/ssb/data/ssb-small/lineorder.hsl");
        d = read_from_file("../../../src/ssb/data/ssb-01/date.hsl");
        p = read_from_file("../../../src/ssb/data/ssb-01/part.hsl");
        c = read_from_file("../../../src/ssb/data/ssb-01/customer.hsl");
        s = read_from_file("../../../src/ssb/data/ssb-01/supplier.hsl");
    }
    if (SF==1) {
        lo = read_from_file("../../../src/ssb/data/ssb-01/lineorder.hsl");
        d = read_from_file("../../../src/ssb/data/ssb-01/date.hsl");
        p = read_from_file("../../../src/ssb/data/ssb-01/part.hsl");
        c = read_from_file("../../../src/ssb/data/ssb-01/customer.hsl");
        s = read_from_file("../../../src/ssb/data/ssb-01/supplier.hsl");
    }
    else if (SF==5) {
        lo = read_from_file("../../../src/ssb/data/ssb-05/lineorder.hsl");
        d = read_from_file("../../../src/ssb/data/ssb-05/date.hsl");
        p = read_from_file("../../../src/ssb/data/ssb-05/part.hsl");
        c = read_from_file("../../../src/ssb/data/ssb-05/customer.hsl");
        s = read_from_file("../../../src/ssb/data/ssb-05/supplier.hsl");
    }
    else if (SF==10) {
//        lo = read_from_file("../../../src/ssb/data/ssb-10-20MB/lineorder.hsl");
//        d = read_from_file("../../../src/ssb/data/ssb-10/date.hsl");
//        p = read_from_file("../../../src/ssb/data/ssb-10/part.hsl");
//        c = read_from_file("../../../src/ssb/data/ssb-10/customer.hsl");
//        s = read_from_file("../../../src/ssb/data/ssb-10/supplier.hsl");
        lo = read_from_file("/Users/corrado/hustle/src/ssb/data/ssb-10-20MB/lineorder.hsl");
        d = read_from_file("/Users/corrado/hustle/src/ssb/data/ssb-10/date.hsl");
        p = read_from_file("/Users/corrado/hustle/src/ssb/data/ssb-10/part.hsl");
        c = read_from_file("/Users/corrado/hustle/src/ssb/data/ssb-10/customer.hsl");
        s = read_from_file("/Users/corrado/hustle/src/ssb/data/ssb-10/supplier.hsl");
    }
    else if (SF==100) {
        d = read_from_file("/mydata/SQL-benchmark-data-generator/ssbgen/date.hsl");
        std::cout << "d" << std::endl;

        p = read_from_file("/mydata/SQL-benchmark-data-generator/ssbgen/part.hsl");
        std::cout << "p" << std::endl;

        c = read_from_file("/mydata/SQL-benchmark-data-generator/ssbgen/customer.hsl");
        std::cout << "d" << std::endl;

        s = read_from_file("/mydata/SQL-benchmark-data-generator/ssbgen/supplier.hsl");
        std::cout << "s" << std::endl;

        lo = read_from_file("/mydata/SQL-benchmark-data-generator/ssbgen/lineorder.hsl");
        std::cout << "lo" << std::endl;

    }
    else if (SF==101) {
        d = read_from_file("/mydata/SQL-benchmark-data-generator/ssbgen/ssb-001/date.hsl");
        std::cout << "d" << std::endl;

        p = read_from_file("/mydata/SQL-benchmark-data-generator/ssbgen/ssb-001/part.hsl");
        std::cout << "p" << std::endl;

        c = read_from_file("/mydata/SQL-benchmark-data-generator/ssbgen/ssb-001/customer.hsl");
        std::cout << "d" << std::endl;

        s = read_from_file("/mydata/SQL-benchmark-data-generator/ssbgen/ssb-001/supplier.hsl");
        std::cout << "s" << std::endl;

        lo = read_from_file("/mydata/SQL-benchmark-data-generator/ssbgen/ssb-001/lineorder.hsl");
        std::cout << "lo" << std::endl;
    }

    lo_d_ref = {lo, "order date"};
    lo_p_ref = {lo, "part key"};
    lo_s_ref = {lo, "supp key"};
    lo_c_ref = {lo, "cust key"};

    d_ref = {d, "date key"};
    p_ref = {p, "part key"};
    s_ref = {s, "s supp key"};
    c_ref = {c, "c cust key"};

    lo_rev_ref   = {lo, "revenue"};
    d_year_ref   = {d, "year"};
    p_brand1_ref = {p, "brand1"};
    p_category_ref = {p, "category"};
    s_nation_ref = {s, "s nation"};
    s_city_ref = {s, "s city"};
    c_nation_ref = {c, "c nation"};
    c_city_ref   = {c, "c city"};

    d_join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
    p_join_pred = {lo_p_ref, arrow::compute::EQUAL, p_ref};
    s_join_pred = {lo_s_ref, arrow::compute::EQUAL, s_ref};
    c_join_pred = {lo_c_ref, arrow::compute::EQUAL, c_ref};

    reset_results();
}

void SSB::reset_results() {

    lo_select_result_out = std::make_shared<OperatorResult>();
    d_select_result_out = std::make_shared<OperatorResult>();
    p_select_result_out = std::make_shared<OperatorResult>();
    s_select_result_out = std::make_shared<OperatorResult>();
    c_select_result_out = std::make_shared<OperatorResult>();

    lip_result_out    = std::make_shared<OperatorResult>();
    join_result_out   = std::make_shared<OperatorResult>();
    agg_result_out    = std::make_shared<OperatorResult>();

    lo_result_in = std::make_shared<OperatorResult>();
    d_result_in  = std::make_shared<OperatorResult>();
    p_result_in  = std::make_shared<OperatorResult>();
    s_result_in  = std::make_shared<OperatorResult>();
    c_result_in  = std::make_shared<OperatorResult>();

    lo_result_in->append(lo);
    d_result_in->append(d);
    p_result_in->append(p);
    s_result_in->append(s);
    c_result_in->append(c);
}

void SSB::execute(ExecutionPlan &plan, std::shared_ptr<OperatorResult> &final_result) {

}

void SSB::q11() {

    auto discount_pred = Predicate{
        lo, "discount",
        arrow::compute::CompareOperator::NOT_EQUAL,
        arrow::Datum((uint8_t) 1),
        arrow::Datum((uint8_t) 3)
    };

    auto discount_node =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(discount_pred));

    //quantity < 25
    auto quantity_pred_1 = Predicate{
        {lo,
         "quantity"},
        arrow::compute::CompareOperator::LESS,
        arrow::Datum((uint8_t) 25)
    };
    auto quantity_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(quantity_pred_1));

    auto lo_root_node = std::make_shared<ConnectiveNode>(
        discount_node,
        quantity_pred_node_1,
        FilterOperator::AND
    );

    auto lo_pred_tree = std::make_shared<PredicateTree>(lo_root_node);

    // date.year = 1993
    ColumnReference year_ref = d_year_ref;
    auto year_pred_1 = Predicate{
        {d,
         "year"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum((int64_t) 1993)
    };
    auto year_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(year_pred_1));
    auto d_pred_tree = std::make_shared<PredicateTree>(year_pred_node_1);

    ////////////////////////////////////////////////////////////////////////////

    Select lo_select_op(0, lo_result_in, lo_select_result_out, lo_pred_tree);
    Select d_select_op(0, d_result_in, d_select_result_out, d_pred_tree);

    join_result_in = {lo_select_result_out, d_select_result_out};

    JoinPredicate join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
    JoinGraph graph({{join_pred}});
    Join join_op(0, join_result_in, join_result_out, graph);

    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue",
                                  lo_rev_ref};
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

    Scheduler scheduler = Scheduler(num_threads_);
    scheduler.addTask(&plan);

    auto container = simple_profiler.getContainer();
    container->startEvent("1.1");
    scheduler.start();
    scheduler.join();
    container->endEvent("1.1");

    if (print_) {
        out_table = agg_result_out->materialize({{nullptr, "revenue"}});
        out_table->print();
    }
    simple_profiler.summarizeToStream(std::cout);

    simple_profiler.clear();
    reset_results();
}

void SSB::q12() {

    auto discount_pred_bt = Predicate{
        lo, "discount",
        arrow::compute::CompareOperator::NOT_EQUAL,
        arrow::Datum((uint8_t) 4),
        arrow::Datum((uint8_t) 6)
    };

    auto discount_node =
    std::make_shared<PredicateNode>(
        std::make_shared<Predicate>(discount_pred_bt));

    auto quantity_pred_bt = Predicate{
        {lo,
         "quantity"},
        arrow::compute::CompareOperator::NOT_EQUAL,
        arrow::Datum((uint8_t) 26),
        arrow::Datum((uint8_t) 35)
    };

    auto quantity_node =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(quantity_pred_bt));

    auto lo_root_node = std::make_shared<ConnectiveNode>(
        quantity_node,
        discount_node,
        FilterOperator::AND
    );

    auto lo_pred_tree = std::make_shared<PredicateTree>(lo_root_node);

    // date.year month num = 199401
    ColumnReference year_ref = {d, "year month num"};
    auto year_pred_1 = Predicate{
        {d,
         "year month num"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum((int64_t) 199401)
    };
    auto year_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(year_pred_1));
    auto d_pred_tree = std::make_shared<PredicateTree>(year_pred_node_1);

    ////////////////////////////////////////////////////////////////////////////

    Select lo_select_op(0, lo_result_in, lo_select_result_out, lo_pred_tree);
    Select d_select_op(0, d_result_in, d_select_result_out, d_pred_tree);

    join_result_in = {lo_select_result_out, d_select_result_out};

    JoinPredicate join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
    JoinGraph graph({{join_pred}});

    join_result_in = {lo_select_result_out, d_select_result_out};

    Join join_op(0, join_result_in, join_result_out, graph);

    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue",
                                  lo_rev_ref};
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

    Scheduler scheduler = Scheduler(num_threads_);
    scheduler.addTask(&plan);

    auto container = simple_profiler.getContainer();
    container->startEvent("1.2");
    scheduler.start();
    scheduler.join();
    container->endEvent("1.2");

    if (print_) {
        out_table = agg_result_out->materialize({{nullptr, "revenue"}});
        out_table->print();
    }
    simple_profiler.summarizeToStream(std::cout);

    simple_profiler.clear();
    reset_results();
}

void SSB::q13() {

    auto discount_pred_bt = Predicate{
        lo, "discount",
        arrow::compute::CompareOperator::NOT_EQUAL,
        arrow::Datum((uint8_t) 5),
        arrow::Datum((uint8_t) 7)
    };

    auto discount_node =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(discount_pred_bt));

    auto quantity_pred_bt = Predicate{
        {lo,
         "quantity"},
        arrow::compute::CompareOperator::NOT_EQUAL,
        arrow::Datum((uint8_t) 36),
        arrow::Datum((uint8_t) 40)
    };

    auto quantity_node =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(quantity_pred_bt));

    auto lo_root_node = std::make_shared<ConnectiveNode>(
        quantity_node,
        discount_node,
        FilterOperator::AND
    );

    auto lo_pred_tree = std::make_shared<PredicateTree>(lo_root_node);

    // date.year month num = 199401
    auto d_pred_1 = Predicate{
        {d,
         "week num in year"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum((int64_t) 6)
    };
    auto d_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(d_pred_1));

    // date.year month num = 199401
    auto d_pred_2 = Predicate{
        {d,
         "year"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum((int64_t) 1994)
    };
    auto d_pred_node_2 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(d_pred_2));

    auto d_connective_node = std::make_shared<ConnectiveNode>(
        d_pred_node_1,
        d_pred_node_2,
        FilterOperator::AND
    );

    auto d_pred_tree = std::make_shared<PredicateTree>(d_connective_node);

    ////////////////////////////////////////////////////////////////////////////

    Select lo_select_op(0, lo_result_in, lo_select_result_out, lo_pred_tree);
    Select d_select_op(0, d_result_in, d_select_result_out, d_pred_tree);

    join_result_in = {lo_select_result_out, d_select_result_out};

    JoinPredicate join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
    JoinGraph graph({{join_pred}});

    join_result_in = {lo_select_result_out, d_select_result_out};

    Join join_op(0, join_result_in, join_result_out, graph);

    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue",
                                  lo_rev_ref};
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

    Scheduler scheduler = Scheduler(num_threads_);
    scheduler.addTask(&plan);

    auto container = simple_profiler.getContainer();
    container->startEvent("1.3");
    scheduler.start();
    scheduler.join();
    container->endEvent("1.3");

    if (print_) {
        out_table = agg_result_out->materialize({{nullptr, "revenue"}});
        out_table->print();
    }
    simple_profiler.summarizeToStream(std::cout);

    simple_profiler.clear();
    reset_results();
}

void SSB::q21() {

    auto s_pred_1 = Predicate{
        {s,
         "s region"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
                                  ("AMERICA"))
    };
    auto s_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(s_pred_1));

    auto s_pred_tree = std::make_shared<PredicateTree>(s_pred_node_1);


    auto p_pred_1 = Predicate{
        {p,
         "category"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
                                  ("MFGR#12"))
    };
    auto p_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(p_pred_1));

    auto p_pred_tree = std::make_shared<PredicateTree>(p_pred_node_1);

    ////////////////////////////////////////////////////////////////////////////

    lo_select_result_out->append(lo);
    d_select_result_out->append(d);

    Select p_select_op(0, p_result_in, p_select_result_out, p_pred_tree);
    Select s_select_op(0, s_result_in, s_select_result_out, s_pred_tree);

    join_result_in = {lo_select_result_out, d_select_result_out, p_select_result_out, s_select_result_out};

    JoinGraph graph({{s_join_pred, p_join_pred, d_join_pred}});
    Join join_op(0, join_result_in, join_result_out, graph);

    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", lo_rev_ref};
    Aggregate agg_op(0,
                     join_result_out, agg_result_out, {agg_ref},
                     {{d, "year"}, {p, "brand1"}},
                     {{d, "year"}, {p, "brand1"}});


    ExecutionPlan plan(0);
    auto p_select_id = plan.addOperator(&p_select_op);
    auto s_select_id = plan.addOperator(&s_select_op);

    auto join_id = plan.addOperator(&join_op);
    auto agg_id = plan.addOperator(&agg_op);

    // Declare join dependency on select operators
    plan.createLink(p_select_id, join_id);
    plan.createLink(s_select_id, join_id);

    // Declare aggregate dependency on join operator
    plan.createLink(join_id, agg_id);

    ////////////////////////////////////////////////////////////////////////////

    Scheduler scheduler = Scheduler(num_threads_);
    scheduler.addTask(&plan);

    auto container = hustle::simple_profiler.getContainer();
    container->startEvent("2.1");
    scheduler.start();
    scheduler.join();
    container->endEvent("2.1");



    if (print_) {
        out_table = agg_result_out->materialize({
                                                    {nullptr, "revenue"},
                                                    {nullptr, "year"},
                                                    {nullptr, "brand1"}
                                                });
        out_table->print();
    }
    simple_profiler.summarizeToStream(std::cout);
    simple_profiler.clear();
    reset_results();
}

void SSB::q22() {

    auto s_pred_1 = Predicate{
        {s,
         "s region"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
                                  ("ASIA"))
    };
    auto s_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(s_pred_1));

    auto s_pred_tree = std::make_shared<PredicateTree>(s_pred_node_1);


//    auto p_pred = Predicate{
//        {p,
//         "brand1"},
//        arrow::compute::CompareOperator::NOT_EQUAL,
//        arrow::Datum(std::make_shared<arrow::StringScalar>("MFGR#2221")),
//        arrow::Datum(std::make_shared<arrow::StringScalar>("MFGR#2228"))
//    };
//
    auto p_pred_1 = Predicate{
        {p,
         "brand1"},
        arrow::compute::CompareOperator::GREATER_EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>("MFGR#2221"))
    };

    auto p_pred_2 = Predicate{
        {p,
         "brand1"},
        arrow::compute::CompareOperator::LESS_EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>("MFGR#2228"))
    };

    auto p_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(p_pred_1));

    auto p_node_2 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(p_pred_2));

    auto p_node = std::make_shared<ConnectiveNode>(
        p_node_1,
        p_node_2,
        FilterOperator::AND
    );

//    auto p_node =
//        std::make_shared<PredicateNode>(
//            std::make_shared<Predicate>(p_pred));

    auto p_pred_tree = std::make_shared<PredicateTree>(p_node);

    ////////////////////////////////////////////////////////////////////////////

    lo_select_result_out->append(lo);
    d_select_result_out->append(d);

    Select p_select_op(0, p_result_in, p_select_result_out, p_pred_tree);
    Select s_select_op(0, s_result_in, s_select_result_out, s_pred_tree);

    join_result_in = {lo_select_result_out, d_select_result_out, p_select_result_out, s_select_result_out};

    JoinGraph graph({{s_join_pred, p_join_pred, d_join_pred}});
    Join join_op(0, join_result_in, join_result_out, graph);

    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", lo_rev_ref};
    Aggregate agg_op(0,
                     join_result_out, agg_result_out, {agg_ref},
                     {{d, "year"}, {p, "brand1"}},
                     {{d, "year"}, {p, "brand1"}});


    ExecutionPlan plan(0);
    auto p_select_id = plan.addOperator(&p_select_op);
    auto s_select_id = plan.addOperator(&s_select_op);

    auto join_id = plan.addOperator(&join_op);
    auto agg_id = plan.addOperator(&agg_op);

    // Declare join dependency on select operators
    plan.createLink(p_select_id, join_id);
    plan.createLink(s_select_id, join_id);

    // Declare aggregate dependency on join operator
    plan.createLink(join_id, agg_id);

    ////////////////////////////////////////////////////////////////////////////

    Scheduler scheduler = Scheduler(num_threads_);
    scheduler.addTask(&plan);

    auto container = hustle::simple_profiler.getContainer();
    container->startEvent("2.2");
    scheduler.start();
    scheduler.join();
    container->endEvent("2.2");

    if (print_) {
        out_table = agg_result_out->materialize({
                                                    {nullptr, "revenue"},
                                                    {nullptr, "year"},
                                                    {nullptr, "brand1"}
                                                });
        out_table->print();
    }
    simple_profiler.summarizeToStream(std::cout);
    simple_profiler.clear();
    reset_results();
}

void SSB::q23() {

    auto s_pred_1 = Predicate{
        {s,
         "s region"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
                                  ("EUROPE"))
    };
    auto s_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(s_pred_1));

    auto s_pred_tree = std::make_shared<PredicateTree>(s_pred_node_1);


    auto p_pred_1 = Predicate{
        {p,
         "brand1"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
                                  ("MFGR#2221"))
    };
    auto p_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(p_pred_1));

    auto p_pred_tree = std::make_shared<PredicateTree>(p_pred_node_1);

    ////////////////////////////////////////////////////////////////////////////

    lo_select_result_out->append(lo);
    d_select_result_out->append(d);

    Select p_select_op(0, p_result_in, p_select_result_out, p_pred_tree);
    Select s_select_op(0, s_result_in, s_select_result_out, s_pred_tree);

    join_result_in = {lo_select_result_out, d_select_result_out, p_select_result_out, s_select_result_out};

    JoinGraph graph({{s_join_pred, p_join_pred, d_join_pred}});
    Join join_op(0, join_result_in, join_result_out, graph);

    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", lo_rev_ref};
    Aggregate agg_op(0,
                     join_result_out, agg_result_out, {agg_ref},
                     {{d, "year"}, {p, "brand1"}},
                     {{d, "year"}, {p, "brand1"}});


    ExecutionPlan plan(0);
    auto p_select_id = plan.addOperator(&p_select_op);
    auto s_select_id = plan.addOperator(&s_select_op);

    auto join_id = plan.addOperator(&join_op);
    auto agg_id = plan.addOperator(&agg_op);

    // Declare join dependency on select operators
    plan.createLink(p_select_id, join_id);
    plan.createLink(s_select_id, join_id);

    // Declare aggregate dependency on join operator
    plan.createLink(join_id, agg_id);

    ////////////////////////////////////////////////////////////////////////////

    Scheduler scheduler = Scheduler(num_threads_);
    scheduler.addTask(&plan);

    auto container = hustle::simple_profiler.getContainer();
    container->startEvent("2.3");
    scheduler.start();
    scheduler.join();
    container->endEvent("2.3");


    if (print_) {
        out_table = agg_result_out->materialize({
                                                    {nullptr, "revenue"},
                                                    {nullptr, "year"},
                                                    {nullptr, "brand1"}
                                                });
        out_table->print();
    }
    simple_profiler.summarizeToStream(std::cout);
    simple_profiler.clear();
    reset_results();
}

void SSB::q31() {

    auto d_pred = Predicate{
        {d,
         "year"},
        arrow::compute::CompareOperator::NOT_EQUAL,
        arrow::Datum((int64_t) 1992),
        arrow::Datum((int64_t) 1997)

    };

    auto d_node =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(d_pred));

    auto d_pred_tree = std::make_shared<PredicateTree>(d_node);

    auto s_pred_1 = Predicate{
        {s,
         "s region"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
                                  ("ASIA"))
    };
    auto s_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(s_pred_1));

    auto s_pred_tree = std::make_shared<PredicateTree>(s_pred_node_1);

    auto c_pred_1 = Predicate{
        {c,
         "c region"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
                                  ("ASIA"))
    };
    auto c_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(c_pred_1));

    auto c_pred_tree = std::make_shared<PredicateTree>(c_pred_node_1);

    ////////////////////////////////////////////////////////////////////////////

    lo_select_result_out->append(lo);

    Select s_select_op(0, s_result_in, s_select_result_out, s_pred_tree);
    Select c_select_op(0, c_result_in, c_select_result_out, c_pred_tree);
    Select d_select_op(0, d_result_in, d_select_result_out, d_pred_tree);

    join_result_in = {lo_select_result_out, d_select_result_out, s_select_result_out, c_select_result_out};

    JoinGraph graph({{s_join_pred, c_join_pred, d_join_pred}});
    Join join_op(0, join_result_in, join_result_out, graph);

    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", lo_rev_ref};
    Aggregate agg_op(0,
                     join_result_out, agg_result_out, {agg_ref},
                     {d_year_ref, c_nation_ref, s_nation_ref},
                     {d_year_ref, {nullptr, "revenue"}});


    ExecutionPlan plan(0);
    auto s_select_id = plan.addOperator(&s_select_op);
    auto c_select_id = plan.addOperator(&c_select_op);
    auto d_select_id = plan.addOperator(&d_select_op);

    auto join_id = plan.addOperator(&join_op);
    auto agg_id = plan.addOperator(&agg_op);

    // Declare join dependency on select operators
    plan.createLink(s_select_id, join_id);
    plan.createLink(c_select_id, join_id);
    plan.createLink(d_select_id, join_id);

    // Declare aggregate dependency on join operator
    plan.createLink(join_id, agg_id);

    Scheduler scheduler = Scheduler(num_threads_);
    scheduler.addTask(&plan);

    auto container = simple_profiler.getContainer();
    container->startEvent("3.1");
    scheduler.start();
    scheduler.join();
    container->endEvent("3.1");

    if (print_) {
        out_table = agg_result_out->materialize({{nullptr, "revenue"}, {nullptr, "year"}, {nullptr, "c nation"}, {nullptr, "s nation"}});
        out_table->print();
    }
    simple_profiler.summarizeToStream(std::cout);

    simple_profiler.clear();
    reset_results();
}

void SSB::q32() {

    auto d_pred = Predicate{
        {d,
         "year"},
        arrow::compute::CompareOperator::NOT_EQUAL,
        arrow::Datum((int64_t) 1992),
        arrow::Datum((int64_t) 1997)

    };

    auto d_node =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(d_pred));

    auto d_pred_tree = std::make_shared<PredicateTree>(d_node);

    auto s_pred_1 = Predicate{
        {s,
         "s nation"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
                                  ("UNITED STATES"))
    };
    auto s_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(s_pred_1));

    auto s_pred_tree = std::make_shared<PredicateTree>(s_pred_node_1);

    auto c_pred_1 = Predicate{
        {c,
         "c nation"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
                                  ("UNITED STATES"))
    };
    auto c_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(c_pred_1));

    auto c_pred_tree = std::make_shared<PredicateTree>(c_pred_node_1);

    ////////////////////////////////////////////////////////////////////////////

    lo_select_result_out->append(lo);

    Select s_select_op(0, s_result_in, s_select_result_out, s_pred_tree);
    Select c_select_op(0, c_result_in, c_select_result_out, c_pred_tree);
    Select d_select_op(0, d_result_in, d_select_result_out, d_pred_tree);

    join_result_in = {lo_select_result_out, d_select_result_out, s_select_result_out, c_select_result_out};

    JoinGraph graph({{s_join_pred, c_join_pred, d_join_pred}});
    Join join_op(0, join_result_in, join_result_out, graph);

    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", lo_rev_ref};
    Aggregate agg_op(0,
                     join_result_out, agg_result_out, {agg_ref},
                     {d_year_ref, c_city_ref, s_city_ref},
                     {d_year_ref, {nullptr, "revenue"}});

    ExecutionPlan plan(0);
    auto s_select_id = plan.addOperator(&s_select_op);
    auto c_select_id = plan.addOperator(&c_select_op);
    auto d_select_id = plan.addOperator(&d_select_op);

    auto join_id = plan.addOperator(&join_op);
    auto agg_id = plan.addOperator(&agg_op);

    // Declare join dependency on select operators
    plan.createLink(s_select_id, join_id);
    plan.createLink(c_select_id, join_id);
    plan.createLink(d_select_id, join_id);

    // Declare aggregate dependency on join operator
    plan.createLink(join_id, agg_id);

    Scheduler scheduler = Scheduler(num_threads_);
    scheduler.addTask(&plan);

    auto container = simple_profiler.getContainer();
    container->startEvent("3.2");
    scheduler.start();
    scheduler.join();
    container->endEvent("3.2");

    if (print_) {
        out_table = agg_result_out->materialize({{nullptr, "revenue"}, {nullptr, "year"}, {nullptr, "c city"}, {nullptr, "s city"}});
        out_table->print();
    }
    simple_profiler.summarizeToStream(std::cout);

    simple_profiler.clear();
    reset_results();
}

void SSB::q33() {

    auto d_pred = Predicate{
        {d,
         "year"},
        arrow::compute::CompareOperator::NOT_EQUAL,
        arrow::Datum((int64_t) 1992),
        arrow::Datum((int64_t) 1997)

    };

    auto d_node =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(d_pred));

    auto d_pred_tree = std::make_shared<PredicateTree>(d_node);

    auto s_pred_1 = Predicate{
        {s,
         "s city"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
                                  ("UNITED KI1"))
    };

    auto s_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(s_pred_1));

    auto s_pred_2 = Predicate{
        {s,
         "s city"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
                                  ("UNITED KI5"))
    };

    auto s_pred_node_2 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(s_pred_2));

    auto s_connective_node =
        std::make_shared<ConnectiveNode>(
            s_pred_node_1,
            s_pred_node_2,
            FilterOperator::OR
        );

    auto s_pred_tree = std::make_shared<PredicateTree>(s_connective_node);

    auto c_pred_1 = Predicate{
        {c,
         "c city"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
                                  ("UNITED KI1"))
    };

    auto c_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(c_pred_1));

    auto c_pred_2 = Predicate{
        {c,
         "c city"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
                                  ("UNITED KI5"))
    };

    auto c_pred_node_2 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(c_pred_2));

    auto c_connective_node =
        std::make_shared<ConnectiveNode>(
            c_pred_node_1,
            c_pred_node_2,
            FilterOperator::OR
        );

    auto c_pred_tree = std::make_shared<PredicateTree>(c_connective_node);

    ////////////////////////////////////////////////////////////////////////////

    lo_select_result_out->append(lo);

    Select s_select_op(0, s_result_in, s_select_result_out, s_pred_tree);
    Select c_select_op(0, c_result_in, c_select_result_out, c_pred_tree);
    Select d_select_op(0, d_result_in, d_select_result_out, d_pred_tree);

    join_result_in = {lo_select_result_out, d_select_result_out, s_select_result_out, c_select_result_out};

    JoinGraph graph({{s_join_pred, c_join_pred, d_join_pred}});
    Join join_op(0, join_result_in, join_result_out, graph);

    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", lo_rev_ref};
    Aggregate agg_op(0,
                     join_result_out, agg_result_out, {agg_ref},
                     {d_year_ref, c_city_ref, s_city_ref},
                     {d_year_ref, {nullptr, "revenue"}});

    ExecutionPlan plan(0);
    auto s_select_id = plan.addOperator(&s_select_op);
    auto c_select_id = plan.addOperator(&c_select_op);
    auto d_select_id = plan.addOperator(&d_select_op);

    auto join_id = plan.addOperator(&join_op);
    auto agg_id = plan.addOperator(&agg_op);

    // Declare join dependency on select operators
    plan.createLink(s_select_id, join_id);
    plan.createLink(c_select_id, join_id);
    plan.createLink(d_select_id, join_id);

    // Declare aggregate dependency on join operator
    plan.createLink(join_id, agg_id);

    Scheduler scheduler = Scheduler(num_threads_);
    scheduler.addTask(&plan);

    auto container = simple_profiler.getContainer();
    container->startEvent("3.3");
    scheduler.start();
    scheduler.join();
    container->endEvent("3.3");

    if (print_) {
        out_table = agg_result_out->materialize({{nullptr, "revenue"}, {nullptr, "year"}, {nullptr, "c city"}, {nullptr, "s city"}});
        out_table->print();
    }
    simple_profiler.summarizeToStream(std::cout);

    simple_profiler.clear();
    reset_results();
}

void SSB::q34() {

    auto d_pred_1 = Predicate{
        {d,
         "year month"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
                                  ("Dec1997"))
    };

    auto d_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(d_pred_1));

    auto d_pred_tree = std::make_shared<PredicateTree>(d_pred_node_1);

    auto s_pred_1 = Predicate{
        {s,
         "s city"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
                                  ("UNITED KI1"))
    };

    auto s_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(s_pred_1));

    auto s_pred_2 = Predicate{
        {s,
         "s city"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
                                  ("UNITED KI5"))
    };

    auto s_pred_node_2 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(s_pred_2));

    auto s_connective_node =
        std::make_shared<ConnectiveNode>(
            s_pred_node_1,
            s_pred_node_2,
            FilterOperator::OR
        );

    auto s_pred_tree = std::make_shared<PredicateTree>(s_connective_node);

    auto c_pred_1 = Predicate{
        {c,
         "c city"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
                                  ("UNITED KI1"))
    };

    auto c_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(c_pred_1));

    auto c_pred_2 = Predicate{
        {c,
         "c city"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
                                  ("UNITED KI5"))
    };

    auto c_pred_node_2 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(c_pred_2));

    auto c_connective_node =
        std::make_shared<ConnectiveNode>(
            c_pred_node_1,
            c_pred_node_2,
            FilterOperator::OR
        );

    auto c_pred_tree = std::make_shared<PredicateTree>(c_connective_node);

    ////////////////////////////////////////////////////////////////////////////

    lo_select_result_out->append(lo);

    Select s_select_op(0, s_result_in, s_select_result_out, s_pred_tree);
    Select c_select_op(0, c_result_in, c_select_result_out, c_pred_tree);
    Select d_select_op(0, d_result_in, d_select_result_out, d_pred_tree);

    join_result_in = {lo_select_result_out, d_select_result_out, s_select_result_out, c_select_result_out};

    JoinGraph graph({{s_join_pred, c_join_pred, d_join_pred}});
    Join join_op(0, join_result_in, join_result_out, graph);

    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", lo_rev_ref};
    Aggregate agg_op(0,
                     join_result_out, agg_result_out, {agg_ref},
                     {d_year_ref, c_city_ref, s_city_ref},
                     {d_year_ref, {nullptr, "revenue"}});

    ExecutionPlan plan(0);
    auto s_select_id = plan.addOperator(&s_select_op);
    auto c_select_id = plan.addOperator(&c_select_op);
    auto d_select_id = plan.addOperator(&d_select_op);

    auto join_id = plan.addOperator(&join_op);
    auto agg_id = plan.addOperator(&agg_op);

    // Declare join dependency on select operators
    plan.createLink(s_select_id, join_id);
    plan.createLink(c_select_id, join_id);
    plan.createLink(d_select_id, join_id);

    // Declare aggregate dependency on join operator
    plan.createLink(join_id, agg_id);

    Scheduler scheduler = Scheduler(num_threads_);
    scheduler.addTask(&plan);

    auto container = simple_profiler.getContainer();
    container->startEvent("3.4");
    scheduler.start();
    scheduler.join();
    container->endEvent("3.4");

    if (print_) {
        out_table = agg_result_out->materialize({{nullptr, "revenue"}, {nullptr, "year"}, {nullptr, "c city"}, {nullptr, "s city"}});
        out_table->print();
    }
    simple_profiler.summarizeToStream(std::cout);

    simple_profiler.clear();
    reset_results();
}

void SSB::q41() {

    auto s_pred_1 = Predicate{
        {s,
         "s region"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
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
        arrow::Datum(std::make_shared<arrow::StringScalar>
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
        arrow::Datum(std::make_shared<arrow::StringScalar>
                                  ("MFGR#1"))
    };
    auto p_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(p_pred_1));

    auto p_pred_2 = Predicate{
        {p,
         "mfgr"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
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

    join_result_in = {lo_select_result_out, d_select_result_out, p_select_result_out, s_select_result_out, c_select_result_out};

    JoinGraph graph({{s_join_pred, c_join_pred, p_join_pred, d_join_pred}});
    Join join_op(0, join_result_in, join_result_out, graph);

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

    auto join_id = plan.addOperator(&join_op);
    auto agg_id = plan.addOperator(&agg_op);

    // Declare join dependency on select operators
    plan.createLink(p_select_id, join_id);
    plan.createLink(s_select_id, join_id);
    plan.createLink(c_select_id, join_id);

    // Declare aggregate dependency on join operator
    plan.createLink(join_id, agg_id);

    ////////////////////////////////////////////////////////////////////////////

    Scheduler scheduler = Scheduler(num_threads_);
    scheduler.addTask(&plan);

    auto container = simple_profiler.getContainer();
    container->startEvent("4.1");
    scheduler.start();
    scheduler.join();
    container->endEvent("4.1");

    if (print_) {
        out_table = agg_result_out->materialize({{nullptr, "revenue"}, {nullptr, "year"}, {nullptr, "c nation"}});
        out_table->print();
    }
    simple_profiler.summarizeToStream(std::cout);

    simple_profiler.clear();
    reset_results();
}

void SSB::q42() {

    auto d_pred = Predicate{
        {d,
         "year"},
        arrow::compute::CompareOperator::NOT_EQUAL,
        arrow::Datum((int64_t) 1997),
        arrow::Datum((int64_t) 1998)
    };

    auto d_node =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(d_pred));

    auto d_pred_tree = std::make_shared<PredicateTree>(d_node);

    auto s_pred_1 = Predicate{
        {s,
         "s region"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
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
        arrow::Datum(std::make_shared<arrow::StringScalar>
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
        arrow::Datum(std::make_shared<arrow::StringScalar>
                                  ("MFGR#1"))
    };
    auto p_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(p_pred_1));

    auto p_pred_2 = Predicate{
        {p,
         "mfgr"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
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

    join_result_in = {lo_select_result_out, d_select_result_out, p_select_result_out, s_select_result_out, c_select_result_out};

    JoinGraph graph({{s_join_pred, c_join_pred, p_join_pred, d_join_pred}});
    Join join_op(0, join_result_in, join_result_out, graph);

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

    auto join_id = plan.addOperator(&join_op);
    auto agg_id = plan.addOperator(&agg_op);

    // Declare join dependency on select operators
    plan.createLink(p_select_id, join_id);
    plan.createLink(s_select_id, join_id);
    plan.createLink(c_select_id, join_id);
    plan.createLink(d_select_id, join_id);

    // Declare aggregate dependency on join operator
    plan.createLink(join_id, agg_id);

    Scheduler scheduler = Scheduler(num_threads_);
    scheduler.addTask(&plan);

    auto container = simple_profiler.getContainer();
    container->startEvent("4.2");
    scheduler.start();
    scheduler.join();
    container->endEvent("4.2");

    if (print_) {
        out_table = agg_result_out->materialize({{nullptr, "revenue"}, {nullptr, "year"}, {nullptr, "s nation"}, {nullptr, "category"}});
        out_table->print();
    }
    simple_profiler.summarizeToStream(std::cout);

    simple_profiler.clear();
    reset_results();
}

void SSB::q43() {

    auto d_pred = Predicate{
        {d,
         "year"},
        arrow::compute::CompareOperator::NOT_EQUAL,
        arrow::Datum((int64_t) 1997),
        arrow::Datum((int64_t) 1998)

    };

    auto d_node =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(d_pred));

    auto d_pred_tree = std::make_shared<PredicateTree>(d_node);

    auto s_pred_1 = Predicate{
        {s,
         "s nation"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>
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
        arrow::Datum(std::make_shared<arrow::StringScalar>
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
        arrow::Datum(std::make_shared<arrow::StringScalar>
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

    join_result_in = {lo_select_result_out, d_select_result_out, p_select_result_out, s_select_result_out, c_select_result_out};

    JoinGraph graph({{s_join_pred, c_join_pred, p_join_pred, d_join_pred}});
    Join join_op(0, join_result_in, join_result_out, graph);

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

    auto join_id = plan.addOperator(&join_op);
    auto agg_id = plan.addOperator(&agg_op);

    // Declare join dependency on select operators
    plan.createLink(p_select_id, join_id);
    plan.createLink(s_select_id, join_id);
    plan.createLink(c_select_id, join_id);
    plan.createLink(d_select_id, join_id);

    // Declare aggregate dependency on join operator
    plan.createLink(join_id, agg_id);

    Scheduler scheduler = Scheduler(num_threads_);
    scheduler.addTask(&plan);

    auto container = simple_profiler.getContainer();
    container->startEvent("4.3");
    scheduler.start();
    scheduler.join();
    container->endEvent("4.3");

    if (print_) {
        out_table = agg_result_out->materialize({{nullptr, "revenue"}, {nullptr, "year"},  {nullptr, "s city"}, {nullptr, "brand1"}});
        out_table->print();
    }
    simple_profiler.summarizeToStream(std::cout);

    simple_profiler.clear();
    reset_results();
}

}