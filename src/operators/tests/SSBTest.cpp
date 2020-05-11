#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <table/block.h>
#include <table/util.h>
#include "operators/Aggregate.h"
#include "operators/Join.h"
#include "operators/Select.h"
#include "operators/Predicate.h"
#include "operators/JoinGraph.h"
#include "scheduler/Scheduler.hpp"

#include <arrow/compute/kernels/filter.h>
#include <fstream>
#include <arrow/compute/kernels/match.h>

using namespace testing;
using namespace hustle;
using namespace hustle::operators;


class SSBTestFixture : public testing::Test {
protected:

    std::shared_ptr<arrow::Schema> lo_schema;
    std::shared_ptr<arrow::Schema> d_schema;
    std::shared_ptr<arrow::Schema> p_schema;
    std::shared_ptr<arrow::Schema> s_schema;
    std::shared_ptr<arrow::Schema> cust_schema;


    std::shared_ptr<arrow::BooleanArray> valid;
    std::shared_ptr<arrow::Int64Array> column1;
    std::shared_ptr<arrow::StringArray> column2;
    std::shared_ptr<arrow::StringArray> column3;
    std::shared_ptr<arrow::Int64Array> column4;

    std::shared_ptr<Table> lineorder;
    std::shared_ptr<Table> date;
    std::shared_ptr<Table> part;
    std::shared_ptr<Table> supp;
    std::shared_ptr<Table> cust;

    void SetUp() override {

        arrow::Status status;

        std::shared_ptr<arrow::Field> field1 = arrow::field("order key",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> field2 = arrow::field("line number",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> field3 = arrow::field("cust key",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> field4 = arrow::field("part key",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> field5 = arrow::field("supp key",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> field6 = arrow::field("order date",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> field7 = arrow::field("ord priority",
                                                            arrow::utf8());
        std::shared_ptr<arrow::Field> field8 = arrow::field("ship priority",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> field9 = arrow::field("quantity",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> field10 = arrow::field("extended price",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> field11 = arrow::field("ord total price",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> field12 = arrow::field("discount",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> field13 = arrow::field("revenue",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> field14 = arrow::field("supply cost",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> field15 = arrow::field("tax",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> field16 = arrow::field("commit date",
                                                             arrow::int64());
        std::shared_ptr<arrow::Field> field17 = arrow::field("ship mode",
                                                             arrow::utf8());
        lo_schema = arrow::schema({field1, field2, field3, field4,
                                   field5,
                                   field6, field7, field8, field9, field10,
                                   field11, field12, field13, field14, field15,
                                   field16, field17});


        std::shared_ptr<arrow::Field> d_field1 = arrow::field("date key",
                                                              arrow::int64());
        std::shared_ptr<arrow::Field> d_field2 = arrow::field("date",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> d_field3 = arrow::field("day of week",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> d_field4 = arrow::field("month",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> d_field5 = arrow::field("year",
                                                              arrow::int64());
        std::shared_ptr<arrow::Field> d_field6 = arrow::field("year month num",
                                                              arrow::int64());
        std::shared_ptr<arrow::Field> d_field7 = arrow::field("year month",
                                                              arrow::utf8());
        std::shared_ptr<arrow::Field> d_field8 = arrow::field("day num in week",
                                                              arrow::int64());
        std::shared_ptr<arrow::Field> d_field9 = arrow::field("day num in "
                                                              "month",
                                                              arrow::int64());
        std::shared_ptr<arrow::Field> d_field10 = arrow::field("day num in "
                                                               "year",
                                                               arrow::int64());
        std::shared_ptr<arrow::Field> d_field11 = arrow::field("month num in "
                                                               "year",
                                                               arrow::int64());
        std::shared_ptr<arrow::Field> d_field12 = arrow::field("week num in "
                                                               "year",
                                                               arrow::int64());
        std::shared_ptr<arrow::Field> d_field13 = arrow::field("selling season",
                                                               arrow::utf8());
        std::shared_ptr<arrow::Field> d_field14 = arrow::field("last day in "
                                                               "week fl",
                                                               arrow::int64());
        std::shared_ptr<arrow::Field> d_field15 = arrow::field("last day in "
                                                               "month fl",
                                                               arrow::int64());
        std::shared_ptr<arrow::Field> d_field16 = arrow::field("holiday fl",
                                                               arrow::int64());
        std::shared_ptr<arrow::Field> d_field17 = arrow::field("weekday fl",
                                                               arrow::int64());

        d_schema = arrow::schema({ d_field1,  d_field2,  d_field3,  d_field4,  d_field5,
                                   d_field6,  d_field7,  d_field8,  d_field9,  d_field10,
                                   d_field11,  d_field12,  d_field13,  d_field14,  d_field15,
                                   d_field16,  d_field17});




        std::shared_ptr<arrow::Field> p_field1 = arrow::field("part key",
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

        p_schema = arrow::schema({ p_field1,  p_field2,  p_field3,  p_field4,
                                   p_field5,
                                   p_field6,  p_field7,  p_field8,
                                   p_field9});


        std::shared_ptr<arrow::Field> s_field1 = arrow::field("supp key",
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

        s_schema = arrow::schema({ s_field1,  s_field2,  s_field3,  s_field4,
                                   s_field5,
                                   s_field6,  s_field7});


        std::shared_ptr<arrow::Field> c_field1 = arrow::field("cust key",
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
        std::shared_ptr<arrow::Field> c_field8 = arrow::field("mkt segment",
                                                              arrow::utf8());

        cust_schema = arrow::schema({ c_field1,  c_field2,  c_field3,  c_field4,
                                      c_field5, c_field6, c_field7, c_field8});

//        lineorder = read_from_file
//                ("/Users/corrado/hustle/src/table/tests/lineorder.hsl");
//
//        date = read_from_file
//                ("/Users/corrado/hustle/src/table/tests/date.hsl");
//
//        part = read_from_file
//                ("/Users/corrado/hustle/src/table/tests/part.hsl");
//
//        supp = read_from_file
//                ("/Users/corrado/hustle/src/table/tests/supplier.hsl");
    }
};

TEST_F(SSBTestFixture, CSVtoHSL) {


    date = read_from_csv_file
            ("/Users/corrado/hustle/src/table/tests/date.tbl", d_schema,
             BLOCK_SIZE);

    write_to_file("/Users/corrado/hustle/src/table/tests/date.hsl",
                  *date);

    date = read_from_file
            ("/Users/corrado/hustle/src/table/tests/date.hsl");
}

TEST_F(SSBTestFixture, SSBQ41) {

    lineorder = read_from_file
            ("/Users/corrado/hustle/src/table/tests/lineorder.hsl");
    date = read_from_file
            ("/Users/corrado/hustle/src/table/tests/date.hsl");
    supp = read_from_file
            ("/Users/corrado/hustle/src/table/tests/supplier.hsl");
    cust = read_from_file
            ("/Users/corrado/hustle/src/table/tests/customer.hsl");
    part = read_from_file
            ("/Users/corrado/hustle/src/table/tests/part.hsl");

    ColumnReference lo_c_ref = {lineorder, "cust key"};
    ColumnReference lo_s_ref = {lineorder, "supp key"};
    ColumnReference lo_p_ref = {lineorder, "part key"};
    ColumnReference lo_d_ref = {lineorder, "order date"};

    ColumnReference c_ref = {cust, "cust key"};
    ColumnReference s_ref = {supp, "supp key"};
    ColumnReference p_ref = {part, "part key"};
    ColumnReference d_ref = {date, "date key"};


    auto c_pred_1 = Predicate{
            {cust, "region"},
            arrow::compute::CompareOperator::EQUAL,
            arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                    ("AMERICA"))
    };

    auto c_pred_node_1 =
            std::make_shared<PredicateNode>(
                    std::make_shared<Predicate>(c_pred_1));

    auto c_pred_tree = std::make_shared<PredicateTree>
            (c_pred_node_1);

    auto s_pred_1 = Predicate{
            {supp, "region"},
            arrow::compute::CompareOperator::EQUAL,
            arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                                          ("AMERICA"))
    };

    auto s_pred_node_1 =
        std::make_shared<PredicateNode>(
                std::make_shared<Predicate>(s_pred_1));

    auto s_pred_tree = std::make_shared<PredicateTree>
            (s_pred_node_1);

    auto p_pred_1 = Predicate{
            {part, "mfgr"},
            arrow::compute::CompareOperator::EQUAL,
            arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                                          ("MFGR#1"))
    };
    auto p_pred_2 = Predicate{
            {part, "mfgr"},
            arrow::compute::CompareOperator::EQUAL,
            arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                                          ("MFGR#2"))
    };

    auto p_pred_node_1 =
            std::make_shared<PredicateNode>(
                    std::make_shared<Predicate>(p_pred_1));

    auto p_pred_node_2 =
            std::make_shared<PredicateNode>(
                    std::make_shared<Predicate>(p_pred_2));

    auto p_connective_node = std::make_shared<ConnectiveNode>(
            p_pred_node_1,
            p_pred_node_2,
            hustle::operators::FilterOperator::OR
    );

    auto p_pred_tree = std::make_shared<PredicateTree>
            (p_connective_node);

    auto s_result = std::make_shared<OperatorResult>();
    s_result->append(supp);

    auto c_result = std::make_shared<OperatorResult>();
    c_result->append(cust);

    auto p_result = std::make_shared<OperatorResult>();
    p_result->append(part);


    Select s_select_op(0, s_result, s_pred_tree);
    Select c_select_op(0, c_result, c_pred_tree);
    Select p_select_op(0, p_result, p_pred_tree);

    Scheduler &scheduler = Scheduler::GlobalInstance();

//    scheduler.addTask(s_select_op.createTask());
//    scheduler.addTask(c_select_op.createTask());
    scheduler.addTask(p_select_op.createTask());

    scheduler.start();
    scheduler.join();

    auto result = std::make_shared<OperatorResult>();

//    result->append(s_select_op.finish());
//    result->append(c_select_op.finish());
    result->append(p_select_op.finish());
    result->append(lineorder);
    result->append(date);

    auto out_table = result->materialize({p_ref});
    out_table->print();
    std::cout << out_table->get_num_rows() << std::endl;

}