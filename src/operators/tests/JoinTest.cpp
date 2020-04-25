#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <table/block.h>
#include <table/util.h>
#include "operators/Aggregate.h"
#include "operators/Join.h"
#include "operators/Select.h"


#include <arrow/compute/kernels/filter.h>
#include <fstream>

using namespace testing;
using namespace hustle::operators;

class JoinTestFixture : public testing::Test {
protected:

    std::shared_ptr<arrow::Schema> schema;

    arrow::Int64Builder int_builder;
    arrow::StringBuilder str_builder;
    std::shared_ptr<arrow::Array> expected_R_col_1;
    std::shared_ptr<arrow::Array> expected_R_col_2;
    std::shared_ptr<arrow::Array> expected_S_col_1;
    std::shared_ptr<arrow::Array> expected_S_col_2;

    std::shared_ptr<Table> R, S, T;

    void SetUp() override {
        arrow::Status status;

        auto field_1 = arrow::field("key",arrow::int64());
        auto field_2 = arrow::field("data", arrow::utf8());
        
        schema = arrow::schema({field_1, field_2});

        std::ofstream R_csv;
        std::ofstream S_csv;
        std::ofstream T_csv;
        R_csv.open("R.csv");
        S_csv.open("S.csv");
        T_csv.open("T.csv");
        
        for (int i = 0; i < 3; i++) {
            R_csv << std::to_string(i) << "|";
            R_csv << "R" << std::to_string(i) <<  std::endl;
        }
        R_csv.close();
        
        for (int i = 0; i < 4; i++) {
            S_csv << std::to_string(3-i) << "|";
            S_csv << "S" << std::to_string(3-i) <<  std::endl;
        }
        S_csv.close();

        for (int i = 0; i < 5; i++) {
            T_csv << std::to_string(i) << "|";
            T_csv << "T" << std::to_string(i) <<  std::endl;
        }
        T_csv.close();

    }
};

TEST_F(JoinTestFixture, Join1) {

    R = read_from_csv_file("R.csv", schema, BLOCK_SIZE);
    S = read_from_csv_file("S.csv", schema, BLOCK_SIZE);

    ColumnReference R_ref_1 = {R, "key"};
    ColumnReference R_ref_2 = {R, "data"};

    ColumnReference S_ref_1 = {S, "key"};
    ColumnReference S_ref_2 = {S, "data"};

    auto result = std::make_shared<OperatorResult>();
    result->append(R);
    result->append(S);

    JoinPredicate join_pred = {R_ref_1, arrow::compute::EQUAL, S_ref_1};
    JoinGraph graph({{join_pred}});
    Join join_op(result, graph);

    result = join_op.run();

    auto out_table = result->materialize({R_ref_1, R_ref_2, S_ref_1, S_ref_2});
    out_table->print();

    arrow::Status status;

    status = int_builder.AppendValues({0,1,2});
    status = int_builder.Finish(&expected_R_col_1);

    status = int_builder.AppendValues({0,1,2});
    status = int_builder.Finish(&expected_S_col_1);

    status = str_builder.AppendValues({"R0","R1","R2"});
    status = str_builder.Finish(&expected_R_col_2);

    status = str_builder.AppendValues({"S0","S1","S2"});
    status = str_builder.Finish(&expected_S_col_2);

    EXPECT_TRUE(out_table->get_column(0)->chunk(0)->Equals(expected_R_col_1));
    EXPECT_TRUE(out_table->get_column(1)->chunk(0)->Equals(expected_R_col_2));
    EXPECT_TRUE(out_table->get_column(2)->chunk(0)->Equals(expected_S_col_1));
    EXPECT_TRUE(out_table->get_column(3)->chunk(0)->Equals(expected_S_col_2));
}
