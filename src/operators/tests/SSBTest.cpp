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

using namespace testing;
using namespace hustle::operators;
using namespace hustle;

class SSBTestFixture : public testing::Test {
protected:

    std::shared_ptr<Table> lo, c, s, part, d;
    std::shared_ptr<arrow::Schema> lo_schema, c_schema, s_schema, p_schema, d_schema;

    void SetUp() override {



        arrow::Status status;

        std::shared_ptr<arrow::Field> field1 = arrow::field("orderkey",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> field2 = arrow::field("linenumber",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> field3 = arrow::field("custkey",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> field4 = arrow::field("partkey",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> field5 = arrow::field("suppkey",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> field6 = arrow::field("orderdate",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> field7 = arrow::field("ordpriority",
                                                            arrow::utf8());
        std::shared_ptr<arrow::Field> field8 = arrow::field("shippriority",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> field9 = arrow::field("quantity",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> field10 = arrow::field("extendedprice",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> field11 = arrow::field("ordtotalprice",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> field12 = arrow::field("discount",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> field13 = arrow::field("revenue",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> field14 = arrow::field("supplycost",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> field15 = arrow::field("tax",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> field16 = arrow::field("commitdate",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> field17 = arrow::field("shipmode",
                                                             arrow::utf8());
        lo_schema = arrow::schema({field1, field2, field3, field4, field5,
                                   field6, field7, field8, field9, field10,
                                   field11, field12, field13, field14, field15,
                                   field16, field17});


        std::shared_ptr<arrow::Field> d_field1 = arrow::field("datekey",
                                                              arrow::int64());
        std::shared_ptr<arrow::Field> d_field2 = arrow::field("date",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> d_field3 = arrow::field("dayofweek",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> d_field4 = arrow::field("month",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> d_field5 = arrow::field("year",
                                                              arrow::int64());
        std::shared_ptr<arrow::Field> d_field6 = arrow::field("yearmonthnum",
                                                              arrow::int64());
        std::shared_ptr<arrow::Field> d_field7 = arrow::field("yearmonth",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> d_field8 = arrow::field("daynuminweek",
                                                              arrow::int64());
        std::shared_ptr<arrow::Field> d_field9 = arrow::field("daynuminmonth",
                                                              arrow::int64());
        std::shared_ptr<arrow::Field> d_field10 = arrow::field("daynuminyear",
                                                               arrow::int64());
        std::shared_ptr<arrow::Field> d_field11 = arrow::field("monthnuminyear",
                                                               arrow::int64());
        std::shared_ptr<arrow::Field> d_field12 = arrow::field("weeknuminyear",
                                                               arrow::int64());
        std::shared_ptr<arrow::Field> d_field13 = arrow::field("sellingseason",
                                                               arrow::utf8());
        std::shared_ptr<arrow::Field> d_field14 = arrow::field(
                "lastdayinweekfl", arrow::int64());
        std::shared_ptr<arrow::Field> d_field15 = arrow::field(
                "lastdayinmonthfl", arrow::int64());
        std::shared_ptr<arrow::Field> d_field16 = arrow::field("holidayfl",
                                                               arrow::int64());
        std::shared_ptr<arrow::Field> d_field17 = arrow::field("weekdayfl",
                                                               arrow::int64());

        d_schema = arrow::schema(
                {d_field1, d_field2, d_field3, d_field4, d_field5,
                 d_field6, d_field7, d_field8, d_field9, d_field10,
                 d_field11, d_field12, d_field13, d_field14, d_field15,
                 d_field16, d_field17});


        std::shared_ptr<arrow::Field> p_field1 = arrow::field("partkey",
                                                              arrow::int64());
        std::shared_ptr<arrow::Field> p_field2 = arrow::field("name",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> p_field3 = arrow::field("mfgr",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> p_field4 = arrow::field("category",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> p_field5 = arrow::field("brand1",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> p_field6 = arrow::field("color",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> p_field7 = arrow::field("type",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> p_field8 = arrow::field("size",
                                                              arrow::int64());
        std::shared_ptr<arrow::Field> p_field9 = arrow::field("container",
                                                              arrow::utf8());

        p_schema = arrow::schema({p_field1, p_field2, p_field3, p_field4,
                                  p_field5,
                                  p_field6, p_field7, p_field8,
                                  p_field9});


        std::shared_ptr<arrow::Field> s_field1 = arrow::field("suppkey",
                                                              arrow::int64());
        std::shared_ptr<arrow::Field> s_field2 = arrow::field("name",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> s_field3 = arrow::field("address",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> s_field4 = arrow::field("city",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> s_field5 = arrow::field("nation",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> s_field6 = arrow::field("region",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> s_field7 = arrow::field("phone",
                                                              arrow::utf8());

        s_schema = arrow::schema({s_field1, s_field2, s_field3, s_field4,
                                  s_field5,
                                  s_field6, s_field7});


        std::shared_ptr<arrow::Field> c_field1 = arrow::field("custkey",
                                                              arrow::int64());
        std::shared_ptr<arrow::Field> c_field2 = arrow::field("name",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> c_field3 = arrow::field("address",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> c_field4 = arrow::field("city",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> c_field5 = arrow::field("nation",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> c_field6 = arrow::field("region",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> c_field7 = arrow::field("phone",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> c_field8 = arrow::field("mktsegment",
                                                              arrow::utf8());

        c_schema = arrow::schema({c_field1, c_field2, c_field3, c_field4,
                                  c_field5, c_field6, c_field7, c_field8});

        lo = read_from_file("/Users/corrado/hustle/data/ssb-1/lineorder.hsl");
        d  = read_from_file("/Users/corrado/hustle/data/ssb-1/date.hsl");

    }
};

TEST_F(SSBTestFixture, SSBQ1_1){

    std::shared_ptr<Table> out_table;
    ColumnReference discount_ref = {lo, "discount"};
    ColumnReference quantity_ref = {lo, "quantity"};

    auto t_start = std::chrono::high_resolution_clock::now();
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

    for (int l=0; l<100; l++) {
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
                          (t2 - t1).count() << std::endl;

        // Join date and lineorder tables
        ColumnReference lo_d_ref = {lo, "order date"};
        ColumnReference d_ref = {d, "date key"};
        ColumnReference revenue_ref = {lo, "revenue"};

        auto join_result = std::make_shared<OperatorResult>();
        join_result->append(result);

        JoinPredicate join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
        JoinGraph graph({{join_pred}});
        Join join_op(0, join_result, graph);

        scheduler.addTask(join_op.createTask());

        scheduler.start();
        scheduler.join();

        join_result = join_op.finish();

        out_table = join_result->materialize({revenue_ref});

        std::cout << "Total selected rows " << out_table->get_num_rows() << std::endl;
        ASSERT_EQ(out_table->get_num_rows(), 118598);

        auto t3 = std::chrono::high_resolution_clock::now();
        std::cout << "Join execution time = " <<
                  std::chrono::duration_cast<std::chrono::milliseconds>
                          (t3 - t2).count() << std::endl;

        AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", {lo, "revenue"}};
        Aggregate agg_op(0, join_result, {agg_ref}, {}, {});

        scheduler.addTask(agg_op.createTask());

        scheduler.start();
        scheduler.join();

        auto agg_result = agg_op.finish();
        out_table = agg_result->materialize({{nullptr, "revenue"}});

        auto t4 = std::chrono::high_resolution_clock::now();
        std::cout << "Query execution time = " <<
                  std::chrono::duration_cast<std::chrono::milliseconds>
                          (t4 - t_start).count() << std::endl;
        out_table->print();

//        std::cout << sum_result.scalar()->ToString() << "\n" << std::endl;
    }
}

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