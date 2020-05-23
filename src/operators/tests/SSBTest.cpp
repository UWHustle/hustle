#include "gtest/gtest.h"
#include "gmock/gmock.h"

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

using namespace testing;
using namespace hustle::operators;
using namespace hustle;

class SSBTestFixture : public testing::Test {
protected:

    std::shared_ptr<Table> lo, c, s, p, d;
    std::shared_ptr<arrow::Schema> lo_schema, c_schema, s_schema, p_schema, d_schema;

    void SetUp() override {

        std::shared_ptr<arrow::Field>field1=arrow::field("order key",
                                                         arrow::int64());
        std::shared_ptr<arrow::Field>field2=arrow::field("line number",
                                                         arrow::int64());
        std::shared_ptr<arrow::Field>field3=arrow::field("cust key",
                                                         arrow::int64());
        std::shared_ptr<arrow::Field>field4=arrow::field("part key",
                                                         arrow::int64());
        std::shared_ptr<arrow::Field>field5=arrow::field("supp key",
                                                         arrow::int64());
        std::shared_ptr<arrow::Field>field6=arrow::field("order date",
                                                         arrow::int64());
        std::shared_ptr<arrow::Field>field7=arrow::field("ord priority",
                                                         arrow::utf8());
        std::shared_ptr<arrow::Field>field8=arrow::field("ship priority",
                                                         arrow::int64());
        std::shared_ptr<arrow::Field>field9=arrow::field("quantity",
                                                         arrow::int64());
        std::shared_ptr<arrow::Field>field10=arrow::field("extended price",
                                                          arrow::int64());
        std::shared_ptr<arrow::Field>field11=arrow::field("ord total price",
                                                          arrow::int64());
        std::shared_ptr<arrow::Field>field12=arrow::field("discount",
                                                          arrow::int64());
        std::shared_ptr<arrow::Field>field13=arrow::field("revenue",
                                                          arrow::int64());
        std::shared_ptr<arrow::Field>field14=arrow::field("supply cost",
                                                          arrow::int64());
        std::shared_ptr<arrow::Field>field15=arrow::field("tax",
                                                          arrow::int64());
        std::shared_ptr<arrow::Field>field16=arrow::field("commit date",
                                                          arrow::int64());
        std::shared_ptr<arrow::Field>field17=arrow::field("ship mode",
                                                          arrow::utf8());
        lo_schema=arrow::schema({field1,field2,field3,field4,
                                 field5,
                                 field6,field7,field8,field9,field10,
                                 field11,field12,field13,field14,field15,
                                 field16,field17});
//        auto t = read_from_csv_file("/Users/corrado/hustle/data/ssb-1/lineorder_smallish.tbl", lo_schema, BLOCK_SIZE);
//        write_to_file("/Users/corrado/hustle/data/ssb-1/lineorder_smallish.hsl", *t);

        lo = read_from_file("/Users/corrado/hustle/data/ssb-1/lineorder.hsl");
        d  = read_from_file("/Users/corrado/hustle/data/ssb-1/date.hsl");
        p  = read_from_file("/Users/corrado/hustle/data/ssb-1/part.hsl");
        c  = read_from_file("/Users/corrado/hustle/data/ssb-1/customer.hsl");
        s  = read_from_file("/Users/corrado/hustle/data/ssb-1/supplier.hsl");

    }
};

TEST_F(SSBTestFixture, SSBQ1_1){

    std::shared_ptr<Table> out_table;
    ColumnReference discount_ref = {lo, "discount"};
    ColumnReference quantity_ref = {lo, "quantity"};

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
    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", {lo, "revenue"}};
    Aggregate agg_op(0, join_result_out, agg_result_out, {agg_ref}, {}, {});

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

    std::cout << std::endl;
    out_table = agg_result_out->materialize({{nullptr, "revenue"}});
    out_table->print();
    hustle::simple_profiler.summarizeToStream(std::cout);


}
/*
TEST_F(SSBTestFixture, SSBQ1_2) {

    std::shared_ptr<Table> out_table;
    ColumnReference discount_ref = {lo, "discount"};
    ColumnReference quantity_ref = {lo, "quantity"};

    auto t_start = std::chrono::high_resolution_clock::now();
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


    auto lo_select_result = std::make_shared<OperatorResult>();
    lo_select_result->append(lo);

    auto d_select_result = std::make_shared<OperatorResult>();
    d_select_result->append(d);

    Select lo_select_op(0,lo_select_result, lo_pred_tree);
    Select d_select_op(0,d_select_result, d_pred_tree);

    Scheduler &scheduler = Scheduler::GlobalInstance();

    scheduler.addTask(lo_select_op.createTask());
    scheduler.addTask(d_select_op.createTask());

    auto t1 = std::chrono::high_resolution_clock::now();

    scheduler.start();
    scheduler.join();

    auto result = std::make_shared<OperatorResult>();
    result->append(lo_select_op.finish());
    result->append(d_select_op.finish());

    auto t2 = std::chrono::high_resolution_clock::now();
    std::cout << "Predicate execution time = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t2-t1).count() <<std::endl;

    // Join date and lineorder tables
    ColumnReference lo_d_ref = {lo, "order date"};
    ColumnReference d_ref = {d, "date key"};
    ColumnReference revenue_ref = {lo, "revenue"};

    auto join_result = std::make_shared<OperatorResult>();
    join_result->append(lo_select_result);
    join_result->append(d_select_result);

    JoinPredicate join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
    JoinGraph graph({{join_pred}});
    Join join_op(0, join_result, graph);

//    join_result = join_op.run();

    out_table = join_result->materialize({lo_d_ref, d_ref, revenue_ref});

    std::cout << "Total selected rows " << out_table->get_num_rows() << std::endl;
    ASSERT_EQ(out_table->get_num_rows(), 4301);

    auto t3 = std::chrono::high_resolution_clock::now();
    std::cout << "Join execution time = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t3 - t2).count() << std::endl;

    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());

    std::shared_ptr<arrow::ChunkedArray> revenue = out_table->get_column(2); //REVENUE
    arrow::compute::Datum sum_result;
    arrow::Status status = arrow::compute::Sum(
            &function_context,
            revenue,
            &sum_result
    );
    evaluate_status(status, __FUNCTION__, __LINE__);

    auto t4 = std::chrono::high_resolution_clock::now();
    std::cout << "Query execution time = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t4-t_start).count() <<std::endl;
}

TEST_F(SSBTestFixture, SSBQ1_3) {

    std::shared_ptr<Table> out_table;
    ColumnReference discount_ref = {lo, "discount"};
    ColumnReference quantity_ref = {lo, "quantity"};

    auto t_start = std::chrono::high_resolution_clock::now();
    //discount >= 5
    auto discount_pred_1 = Predicate{
            {lo,
             "discount"},
            arrow::compute::CompareOperator::GREATER_EQUAL,
            arrow::compute::Datum((int64_t) 5)
    };
    auto discount_pred_node_1 =
            std::make_shared<PredicateNode>(
                    std::make_shared<Predicate>(discount_pred_1));

    //discount <= 7
    auto discount_pred_2 = Predicate{
            {lo,
             "discount"},
            arrow::compute::CompareOperator::LESS_EQUAL,
            arrow::compute::Datum((int64_t) 7)
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

    // date.year = 1994
    auto year_pred_1 = Predicate{
            {d,
             "year"},
            arrow::compute::CompareOperator::EQUAL,
            arrow::compute::Datum((int64_t) 1994)
    };
    auto year_pred_node_1 =
            std::make_shared<PredicateNode>(
                    std::make_shared<Predicate>(year_pred_1));

    // d.week num in year = 6
    auto year_pred_2 = Predicate{
            {d,
             "week num in year"},
            arrow::compute::CompareOperator::EQUAL,
            arrow::compute::Datum((int64_t) 6)
    };
    auto year_pred_node_2 =
            std::make_shared<PredicateNode>(
                    std::make_shared<Predicate>(year_pred_2));

    auto d_root_node = std::make_shared<ConnectiveNode>(
            year_pred_node_1,
            year_pred_node_2,
            FilterOperator::AND
    );

    auto d_pred_tree = std::make_shared<PredicateTree>(d_root_node);

    auto lo_select_result = std::make_shared<OperatorResult>();
    lo_select_result->append(lo);

    auto d_select_result = std::make_shared<OperatorResult>();
    d_select_result->append(d);

    Select lo_select_op(0, lo_select_result, lo_pred_tree);
    Select d_select_op(0, d_select_result, d_pred_tree);

    Scheduler &scheduler = Scheduler::GlobalInstance();

    scheduler.addTask(lo_select_op.createTask());
    scheduler.addTask(d_select_op.createTask());

    auto t1 = std::chrono::high_resolution_clock::now();

    scheduler.start();
    scheduler.join();

    auto result = std::make_shared<OperatorResult>();
    result->append(lo_select_op.finish());
    result->append(d_select_op.finish());

    auto t2 = std::chrono::high_resolution_clock::now();
    std::cout << "Predicate execution time = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t2-t1).count() <<std::endl;

    // Join date and lineorder tables
    ColumnReference lo_d_ref = {lo, "order date"};
    ColumnReference d_ref = {d, "date key"};
    ColumnReference revenue_ref = {lo, "revenue"};

    auto join_result = std::make_shared<OperatorResult>();
    join_result->append(lo_select_result);
    join_result->append(d_select_result);

    JoinPredicate join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
    JoinGraph graph({{join_pred}});
    Join join_op(0, join_result, graph);

//    join_result = join_op.run();

    out_table = join_result->materialize({lo_d_ref, d_ref, revenue_ref});

    std::cout << "Total selected rows " << out_table->get_num_rows() << std::endl;
    ASSERT_EQ(out_table->get_num_rows(), 955);

    auto t3 = std::chrono::high_resolution_clock::now();
    std::cout << "Join execution time = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t3 - t2).count() << std::endl;

    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());

    std::shared_ptr<arrow::ChunkedArray> revenue = out_table->get_column(2); //REVENUE
    arrow::compute::Datum sum_result;
    arrow::Status status = arrow::compute::Sum(
            &function_context,
            revenue,
            &sum_result
    );
    evaluate_status(status, __FUNCTION__, __LINE__);

    auto t4 = std::chrono::high_resolution_clock::now();
    std::cout << "Query execution time = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t4-t_start).count() <<std::endl;
}
 */


TEST_F(SSBTestFixture, SSBQ2_1){

    std::shared_ptr<Table> out_table;
    FLAGS_num_threads = 1;
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


    auto p_pred_1 = Predicate{
        {p,
         "category"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                                  ("MFGR#12"))
    };
    auto p_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(p_pred_1));

    auto p_pred_tree = std::make_shared<PredicateTree>(p_pred_node_1);


    auto p_select_result = std::make_shared<OperatorResult>();
    auto s_select_result = std::make_shared<OperatorResult>();

    p_select_result->append(p);
    s_select_result->append(s);

    auto select_result_out = std::make_shared<OperatorResult>();
    // If you join on a table that has no selection, make sure you add it to
    // the select output beforehand
    select_result_out->append(lo);
    select_result_out->append(d);

    Select p_select_op(0, p_select_result, select_result_out, p_pred_tree);
    Select s_select_op(0, s_select_result, select_result_out, s_pred_tree);

    // Join date and lineorder tables
    ColumnReference lo_d_ref = {lo, "order date"};
    ColumnReference lo_p_ref = {lo, "part key"};
    ColumnReference lo_s_ref = {lo, "supp key"};

    ColumnReference d_ref = {d, "date key"};
    ColumnReference p_ref = {p, "part key"};
    ColumnReference s_ref = {s, "supp key"};

    ColumnReference revenue_ref = {lo, "revenue"};

    auto join_result_out = std::make_shared<OperatorResult>();
    JoinPredicate d_join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
    JoinPredicate p_join_pred = {lo_p_ref, arrow::compute::EQUAL, p_ref};
    JoinPredicate s_join_pred = {lo_s_ref, arrow::compute::EQUAL, s_ref};

    JoinGraph graph({{s_join_pred, p_join_pred, d_join_pred}});
    Join join_op(0, select_result_out, join_result_out, graph);

    auto agg_result_out = std::make_shared<OperatorResult>();
    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", {lo, "revenue"}};
    Aggregate agg_op(0,
                     join_result_out, agg_result_out, {agg_ref},
                     {{d, "year"}, {p, "brand1"}},
                     {{d, "year"}, {p, "brand1"}});

    Scheduler &scheduler = Scheduler::GlobalInstance();

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

    scheduler.addTask(&plan);

    auto container = hustle::simple_profiler.getContainer();
    container->startEvent("query execution");
    scheduler.start();
    scheduler.join();
    container->endEvent("query execution");

    std::cout << std::endl;
    out_table = agg_result_out->materialize({
        {nullptr, "revenue"},
        {nullptr, "year"},
        {nullptr, "brand1"}
                                             });
    out_table->print();
    hustle::simple_profiler.summarizeToStream(std::cout);


}


TEST_F(SSBTestFixture, SSBQ2_1_LIP){

    std::shared_ptr<Table> out_table;

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


    auto p_pred_1 = Predicate{
        {p,
         "category"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                                  ("MFGR#12"))
    };
    auto p_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(p_pred_1));

    auto p_pred_tree = std::make_shared<PredicateTree>(p_pred_node_1);


    auto p_select_result = std::make_shared<OperatorResult>();
    auto s_select_result = std::make_shared<OperatorResult>();

    p_select_result->append(p);
    s_select_result->append(s);

    auto select_result_out = std::make_shared<OperatorResult>();
    // If you join on a table that has no selection, make sure you add it to
    // the select output beforehand
    select_result_out->append(lo);
    select_result_out->append(d);

    Select p_select_op(0, p_select_result, select_result_out, p_pred_tree);
    Select s_select_op(0, s_select_result, select_result_out, s_pred_tree);

    // Join date and lineorder tables
    ColumnReference lo_d_ref = {lo, "order date"};
    ColumnReference lo_p_ref = {lo, "part key"};
    ColumnReference lo_s_ref = {lo, "supp key"};

    ColumnReference d_ref = {d, "date key"};
    ColumnReference p_ref = {p, "part key"};
    ColumnReference s_ref = {s, "supp key"};

    ColumnReference revenue_ref = {lo, "revenue"};

    auto join_result_out = std::make_shared<OperatorResult>();
    auto lip_result_out = std::make_shared<OperatorResult>();
    JoinPredicate d_join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
    JoinPredicate p_join_pred = {lo_p_ref, arrow::compute::EQUAL, p_ref};
    JoinPredicate s_join_pred = {lo_s_ref, arrow::compute::EQUAL, s_ref};

    JoinGraph graph({{s_join_pred, p_join_pred, d_join_pred}});
    LIP lip_op(0, select_result_out, lip_result_out, graph);
    Join join_op(0, lip_result_out, join_result_out, graph);

    auto agg_result_out = std::make_shared<OperatorResult>();
    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", {lo, "revenue"}};
    Aggregate agg_op(0,
                     join_result_out, agg_result_out, {agg_ref},
                     {{d, "year"}, {p, "brand1"}},
                     {{d, "year"}, {p, "brand1"}});

    Scheduler &scheduler = Scheduler::GlobalInstance();

    ExecutionPlan plan(0);
    auto p_select_id = plan.addOperator(&p_select_op);
    auto s_select_id = plan.addOperator(&s_select_op);

    auto lip_id = plan.addOperator(&lip_op);
    auto join_id = plan.addOperator(&join_op);
    auto agg_id = plan.addOperator(&agg_op);

    // Declare join dependency on select operators
    plan.createLink(p_select_id, lip_id);
    plan.createLink(s_select_id, lip_id);

    // Declare aggregate dependency on join operator
    plan.createLink(lip_id, join_id);

    // Declare aggregate dependency on join operator
    plan.createLink(join_id, agg_id);

    scheduler.addTask(&plan);

    auto container = hustle::simple_profiler.getContainer();
    container->startEvent("query execution");
    scheduler.start();
    scheduler.join();
    container->endEvent("query execution");

    std::cout << std::endl;
    out_table = agg_result_out->materialize({
                                                {nullptr, "revenue"},
                                                {nullptr, "year"},
                                                {nullptr, "brand1"}
                                            });
    out_table->print();
    hustle::simple_profiler.summarizeToStream(std::cout);


}