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


class JoinTestFixture : public testing::Test {
protected:

    std::shared_ptr<arrow::Schema> schema;

    arrow::Int64Builder int_builder;
    arrow::DoubleBuilder double_builder;
    arrow::StringBuilder str_builder;
    std::shared_ptr<arrow::Array> expected_agg_col_1;
    std::shared_ptr<arrow::Array> expected_agg_col_2;
    std::shared_ptr<arrow::Array> expected_S_col_1;
    std::shared_ptr<arrow::Array> expected_S_col_2;
    std::shared_ptr<arrow::Array> expected_T_col_1;
    std::shared_ptr<arrow::Array> expected_T_col_2;

    std::shared_ptr<Table> R, S, T;

    void SetUp() override {

        arrow::Status status;

        auto field_1 = arrow::field("key", arrow::int64());
        auto field_2 = arrow::field("group", arrow::utf8());
        auto field_3 = arrow::field("data", arrow::int64());

        schema = arrow::schema({field_1, field_2, field_3});

        std::ofstream R_csv;
        std::ofstream S_csv;
        std::ofstream T_csv;
        R_csv.open("R.csv");
        S_csv.open("S.csv");
        T_csv.open("T.csv");

        for (int i = 0; i < 6; i++) {
            R_csv << std::to_string(i) << "|";
            R_csv << "R" << std::to_string((5 - i) / 2) << "|";
            R_csv << std::to_string(i * 10) << std::endl;
        }
        R_csv.close();

        for (int i = 0; i < 4; i++) {
            S_csv << std::to_string(3 - i) << "|";
            S_csv << "S" << std::to_string(3 - i) << std::endl;
        }
        S_csv.close();

        for (int i = 0; i < 5; i++) {
            T_csv << std::to_string(i) << "|";
            T_csv << "T" << std::to_string(i) << std::endl;
        }
        T_csv.close();

    }
};

/*
 * SELECT avg(R.data) as data_mean
 * FROM R
 */
TEST_F(JoinTestFixture, MeanTest) {

    R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);

    ColumnReference R_key_ref = {R, "key"};
    ColumnReference R_group_ref = {R, "data"};

    auto result = std::make_shared<OperatorResult>();
    auto out_result = std::make_shared<OperatorResult>();
    result->append(R);

    AggregateReference agg_ref = {AggregateKernels::MEAN, "data_mean", R,
                                  "data"};
    Aggregate agg_op(0, result, out_result, {agg_ref}, {}, {});

    Scheduler &scheduler = Scheduler::GlobalInstance();
    scheduler.addTask(agg_op.createTask());

    scheduler.start();
    scheduler.join();

    auto out_table = out_result->materialize({{nullptr, "data_mean"}});
//    out_table->print();

    // Construct expected results
    arrow::Status status;
    status = double_builder.Append(((double) 150) / 6);
    status = double_builder.Finish(&expected_agg_col_1);

    EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_agg_col_1));
}

/*
 * SELECT sum(R.data) as data_sum
 * FROM R
 */
TEST_F(JoinTestFixture, SumTest) {

    R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);

    ColumnReference R_key_ref = {R, "key"};
    ColumnReference R_group_ref = {R, "data"};

    auto result = std::make_shared<OperatorResult>();
    auto out_result = std::make_shared<OperatorResult>();
    result->append(R);

    AggregateReference agg_ref = {AggregateKernels::SUM, "data_sum", R, "data"};
    Aggregate agg_op(0, result, out_result, {agg_ref}, {}, {});
    Scheduler &scheduler = Scheduler::GlobalInstance();
    scheduler.addTask(agg_op.createTask());

    scheduler.start();
    scheduler.join();

    auto out_table = out_result->materialize({{nullptr, "data_sum"}});
//    out_table->print();

    // Construct expected results
    arrow::Status status;
    status = int_builder.Append(150);
    status = int_builder.Finish(&expected_agg_col_1);

    EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_agg_col_1));
}

/*
 * SELECT sum(R.data) as data_sum
 * FROM R
 * WHERE R.group == "R0"
 */
TEST_F(JoinTestFixture, SumWithSelectTest) {

    R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);

    ColumnReference R_key_ref = {R, "key"};
    ColumnReference R_group_ref = {R, "data"};

    auto select_pred = Predicate{
        {R, "group"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::Datum(std::make_shared<arrow::StringScalar>("R0"))
    };

    auto select_pred_node =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(select_pred));

    auto select_pred_tree = std::make_shared<PredicateTree>(select_pred_node);

    auto result = std::make_shared<OperatorResult>();
    auto out_result_select = std::make_shared<OperatorResult>();
    auto out_result_agg = std::make_shared<OperatorResult>();
    result->append(R);

    Select select_op(0, result, out_result_select, select_pred_tree);

    Scheduler &scheduler = Scheduler::GlobalInstance();

    scheduler.start();
    scheduler.addTask(select_op.createTask());
    scheduler.join();

    AggregateReference agg_ref = {AggregateKernels::SUM, "data_sum", R, "data"};
    Aggregate agg_op(0, out_result_select, out_result_agg, {agg_ref}, {}, {});

    scheduler.start();
    scheduler.addTask(agg_op.createTask());
    scheduler.join();

    auto out_table = out_result_agg->materialize({{nullptr, "data_sum"}});
//    out_table->print();

    // Construct expected results
    arrow::Status status;
    status = int_builder.Append(90);
    status = int_builder.Finish(&expected_agg_col_1);

    EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_agg_col_1));
}

/*
 * SELECT sum(R.data) as data_sum
 * FROM R
 * GROUP BY R.group
 */
TEST_F(JoinTestFixture, SumWithGroupByTest) {

    R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);

    ColumnReference R_key_ref = {R, "key"};
    ColumnReference R_group_ref = {R, "group"};

    auto result = std::make_shared<OperatorResult>();
    auto out_result = std::make_shared<OperatorResult>();
    result->append(R);

    AggregateReference agg_ref = {AggregateKernels::SUM, "data_sum", R, "data"};
    Aggregate agg_op(0, result, out_result, {agg_ref}, {R_group_ref}, {R_group_ref});
    Scheduler &scheduler = Scheduler::GlobalInstance();
    scheduler.addTask(agg_op.createTask());

    scheduler.start();
    scheduler.join();

    auto out_table = out_result->materialize({
                                             {nullptr, "group"},
                                             {nullptr, "data_sum"}});
//    out_table->print();

    // Construct expected results
    arrow::Status status;
    status = str_builder.AppendValues({"R2", "R1", "R0"});
    status = str_builder.Finish(&expected_agg_col_1);

    status = int_builder.AppendValues({10, 50, 90});
    status = int_builder.Finish(&expected_agg_col_2);

    auto group_col = std::static_pointer_cast<arrow::StringArray>(
        out_table->get_column(0)->chunk(0));
    auto agg_col = std::static_pointer_cast<arrow::Int64Array>(
        out_table->get_column(1)->chunk(0));

    for (int i = 0; i < group_col->length(); i++) {
        if (group_col->GetString(i) == "R0") {
            ASSERT_EQ(agg_col->Value(i), 90);
        } else if (group_col->GetString(i) == "R1") {
            ASSERT_EQ(agg_col->Value(i), 50);
        } else if (group_col->GetString(i) == "R2") {
            ASSERT_EQ(agg_col->Value(i), 10);
        } else {
            FAIL();
        }
    }

}

/*
 * SELECT sum(R.data) as data_sum
 * FROM R
 * GROUP BY R.group
 * ORDER BY R.group
 */
TEST_F(JoinTestFixture, SumWithGroupByOrderByTest) {

    R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);

    ColumnReference R_key_ref = {R, "key"};
    ColumnReference R_group_ref = {R, "group"};

    auto result = std::make_shared<OperatorResult>();
    auto out_result = std::make_shared<OperatorResult>();
    result->append(R);

    AggregateReference agg_ref = {AggregateKernels::SUM, "data_sum", R, "data"};
    Aggregate agg_op(0, result, out_result, {agg_ref}, {R_group_ref}, {R_group_ref});
    Scheduler &scheduler = Scheduler::GlobalInstance();
    scheduler.addTask(agg_op.createTask());

    scheduler.start();
    scheduler.join();

    auto out_table = out_result->materialize({
                                             {nullptr, "group"},
                                             {nullptr, "data_sum"}});
//    out_table->print();

    // Construct expected results
    arrow::Status status;
    status = str_builder.AppendValues({"R0", "R1", "R2"});
    status = str_builder.Finish(&expected_agg_col_1);

    status = int_builder.AppendValues({90, 50, 10});
    status = int_builder.Finish(&expected_agg_col_2);

    EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_agg_col_1));
    EXPECT_TRUE(out_table->get_column(1)->chunk(0)->Equals(expected_agg_col_2));
}