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
using hustle::operators::Aggregate;
using hustle::operators::Join;
using hustle::operators::Select;
using hustle::operators::AggregateUnit;
using hustle::operators::AggregateKernels ;

class OperatorsTestFixture2 : public testing::Test {
protected:

    std::shared_ptr<arrow::Schema> schema;
    std::shared_ptr<arrow::Schema> schema_2;

    std::shared_ptr<arrow::BooleanArray> valid;
    std::shared_ptr<arrow::Int64Array> column1;
    std::shared_ptr<arrow::StringArray> column2;
    std::shared_ptr<arrow::StringArray> column3;
    std::shared_ptr<arrow::Int64Array> column4;

    std::shared_ptr<Table> in_left_table;
    std::shared_ptr<Table> in_right_table;

    void SetUp() override {
        arrow::Status status;

        std::shared_ptr<arrow::Field> field1 = arrow::field("id",
                                                            arrow::int64());
        std::shared_ptr<arrow::Field> field2 = arrow::field("B", arrow::utf8());
        std::shared_ptr<arrow::Field> field3 = arrow::field("C", arrow::utf8());
        std::shared_ptr<arrow::Field> field4 = arrow::field("D",
                                                            arrow::int64());
        schema = arrow::schema({field1, field2, field3, field4});
        schema_2 = arrow::schema({field1, field2});

        std::ofstream left_table_csv;
        left_table_csv.open("left_table.csv");
        for (int i = 0; i < 9; i++) {

            left_table_csv<< std::to_string(i) + "|Mon dessin ne representait"
                                                 "  pas un chapeau.|Il "
                                                 "representait un serpent boa qui digerait un elephant"
                                                 ".|0\n";
            left_table_csv << "1776|Twice two makes four is an excellent thing"
                        ".|Twice two makes five is sometimes a very charming "
                        "thing too.|0\n";
        }
        left_table_csv.close();


        std::ofstream right_table_csv;
        right_table_csv.open("right_table.csv");
        for (int i = 0; i < 9; i++) {
            right_table_csv<< "4242|Mon dessin ne representait pas un chapeau"
                            ".|Il "
                             "representait un serpent boa qui digerait un elephant"
                             ".|37373737\n";
            right_table_csv << std::to_string(i) + "|Twice two makes four is "
                                                   "an excellent thing"
                              ".|Twice two makes five is sometimes a very charming "
                              "thing too.|1789\n";
        }
        right_table_csv.close();


        std::ofstream left_table_csv_2;
        left_table_csv_2.open("left_table_2.csv");
        for (int i = 0; i < 100; i++) {
            left_table_csv_2 << std::to_string(i) + "|My key is " +
                                std::to_string(i) + "\n";
        }
        left_table_csv_2.close();

        std::ofstream right_table_csv_2;
        right_table_csv_2.open("right_table_2.csv");
        for (int i = 0; i < 100; i++) {
            right_table_csv_2 << std::to_string(i) + "|And my key is also " +
            std::to_string(i)+ "\n";
        }
        right_table_csv_2.close();


    }
};

TEST_F(OperatorsTestFixture2, SelectFromCSV) {

    in_left_table = read_from_csv_file
            ("left_table.csv", schema, BLOCK_SIZE);

    auto *select_op = new hustle::operators::Select(
            arrow::compute::CompareOperator::EQUAL,
            "id",
            arrow::compute::Datum((int64_t) 1776)
    );

    auto out_table = select_op->run_operator({in_left_table});

    for (int i=0; i<out_table->get_num_blocks(); i++) {
        auto block = out_table->get_block(i);

        valid = std::static_pointer_cast<arrow::BooleanArray>
                (block->get_valid_column());
        column1 = std::static_pointer_cast<arrow::Int64Array>
                (block->get_column(0));
        column2 = std::static_pointer_cast<arrow::StringArray>
                (block->get_column(1));
        column3 = std::static_pointer_cast<arrow::StringArray>
                (block->get_column(2));
        column4 = std::static_pointer_cast<arrow::Int64Array>
                (block->get_column(3));

        for (int row = 0; row < block->get_num_rows(); row++) {
            EXPECT_EQ(valid->Value(row), true);
            EXPECT_EQ(column1->Value(row), 1776);
            EXPECT_EQ(column2->GetString(row),
                      "Twice two makes four is an excellent thing.");
            EXPECT_EQ(column3->GetString(row),
                      "Twice two makes five is sometimes a very charming "
                      "thing too.");
            EXPECT_EQ(column4->Value(row), 0);
        }
    }
}


TEST_F(OperatorsTestFixture2, SelectFromCSVTwoConditionsSame) {

    in_left_table = read_from_csv_file
            ("left_table.csv", schema, BLOCK_SIZE);

    auto left_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "id",
            arrow::compute::Datum((int64_t) 1776)
    );
    auto right_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "id",
            arrow::compute::Datum((int64_t) 1776)
    );

    auto composite_select_op = new hustle::operators::SelectComposite(
            left_select_op, right_select_op,
            hustle::operators::FilterOperator::AND
            );

    auto out_table = composite_select_op->run_operator({in_left_table});

    for (int i=0; i<out_table->get_num_blocks(); i++) {
        auto block = out_table->get_block(i);

        valid = std::static_pointer_cast<arrow::BooleanArray>
                (block->get_valid_column());
        column1 = std::static_pointer_cast<arrow::Int64Array>
                (block->get_column(0));
        column2 = std::static_pointer_cast<arrow::StringArray>
                (block->get_column(1));
        column3 = std::static_pointer_cast<arrow::StringArray>
                (block->get_column(2));
        column4 = std::static_pointer_cast<arrow::Int64Array>
                (block->get_column(3));

        for (int row = 0; row < block->get_num_rows(); row++) {
            EXPECT_EQ(valid->Value(row), true);
            EXPECT_EQ(column1->Value(row), 1776);
            EXPECT_EQ(column2->GetString(row),
                      "Twice two makes four is an excellent thing.");
            EXPECT_EQ(column3->GetString(row),
                      "Twice two makes five is sometimes a very charming "
                      "thing too.");
            EXPECT_EQ(column4->Value(row), 0);
        }
    }
}


TEST_F(OperatorsTestFixture2, SelectFromCSVTwoConditionsDifferent) {

    in_left_table = read_from_csv_file
            ("left_table_2.csv", schema_2, BLOCK_SIZE);

    auto left_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "id",
            arrow::compute::Datum((int64_t) 42)
    );


    std::shared_ptr<arrow::Scalar> string =
            std::make_shared<arrow::StringScalar>("My key is 42");

    auto k = string->type->name();

    auto right_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "B",
            arrow::compute::Datum(string)
    );

    auto composite_select_op = new hustle::operators::SelectComposite(
            left_select_op, right_select_op,
            hustle::operators::FilterOperator::AND
    );

    auto out_table = composite_select_op->run_operator({in_left_table});

    auto block = out_table->get_block(0);

    valid = std::static_pointer_cast<arrow::BooleanArray>
            (block->get_valid_column());
    column1 = std::static_pointer_cast<arrow::Int64Array>
            (block->get_column(0));
    column2 = std::static_pointer_cast<arrow::StringArray>
            (block->get_column(1));

    EXPECT_EQ(valid->Value(0), true);
    EXPECT_EQ(column1->Value(0), 42);
    EXPECT_EQ(column2->GetString(0),"My key is 42");

}


//TEST_F(OperatorsTestFixture2, HashJoin) {
//
//    auto left_table = read_from_csv_file
//            ("left_table_2.csv", schema_2, BLOCK_SIZE);
//
//    auto right_table = read_from_csv_file
//            ("right_table_2.csv", schema_2, BLOCK_SIZE);
//
//    auto join_op = hustle::operators::Join("id","id");
//
//    auto out_table = join_op.hash_join(left_table, right_table);
//
//    for (int i=0; i<out_table->get_num_blocks(); i++) {
//
//        auto block = out_table->get_block(i);
//
//        valid = std::static_pointer_cast<arrow::BooleanArray>
//                (block->get_valid_column());
//        column1 = std::static_pointer_cast<arrow::Int64Array>
//                (block->get_column(0));
//        column2 = std::static_pointer_cast<arrow::StringArray>
//                (block->get_column(1));
//        column3 = std::static_pointer_cast<arrow::StringArray>
//                (block->get_column(2));
//
//        int table_row = 0;
//
//        for (int block_row = 0; block_row < block->get_num_rows(); block_row++) {
//
//            table_row = block_row + out_table->get_block_row_offset(i);
//
//            EXPECT_EQ(valid->Value(block_row), true);
//            EXPECT_EQ(column1->Value(block_row), table_row);
//            EXPECT_EQ(column2->GetString(block_row),
//                      "My key is " + std::to_string(table_row));
//            EXPECT_EQ(column3->GetString(block_row),
//                      "And my key is also " + std::to_string(table_row));
//        }
//    }
//}

//TEST_F(OperatorsTestFixture2, HashJoinEmptyResult) {
//
//    auto left_table = read_from_csv_file
//            ("left_table.csv", schema, BLOCK_SIZE);
//
//    auto right_table = read_from_csv_file
//            ("right_table.csv", schema, BLOCK_SIZE);
//
//    auto join_op = hustle::operators::Join("D", "D");
//
//    auto out_table = join_op.hash_join(left_table, right_table);
//
//    EXPECT_EQ(out_table->get_num_rows(), 0);
//    EXPECT_EQ(out_table->get_num_blocks(), 0);
//}




class SSBTestFixture : public testing::Test {
protected:

    std::shared_ptr<arrow::Schema> lineorder_schema;
    std::shared_ptr<arrow::Schema> date_schema;
    std::shared_ptr<arrow::Schema> part_schema;
    std::shared_ptr<arrow::Schema> supp_schema;

    std::shared_ptr<arrow::BooleanArray> valid;
    std::shared_ptr<arrow::Int64Array> column1;
    std::shared_ptr<arrow::StringArray> column2;
    std::shared_ptr<arrow::StringArray> column3;
    std::shared_ptr<arrow::Int64Array> column4;

    std::shared_ptr<Table> lineorder;
    std::shared_ptr<Table> date;
    std::shared_ptr<Table> part;
    std::shared_ptr<Table> supp;


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
        lineorder_schema = arrow::schema({field1, field2, field3, field4,
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

        date_schema = arrow::schema({ d_field1,  d_field2,  d_field3,  d_field4,  d_field5,
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

        part_schema = arrow::schema({ p_field1,  p_field2,  p_field3,  p_field4,
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

        supp_schema = arrow::schema({ s_field1,  s_field2,  s_field3,  s_field4,
                                      s_field5,
                                      s_field6,  s_field7});
    }
};

TEST_F(SSBTestFixture, CSVtoHSL) {


    supp = read_from_csv_file
            ("/Users/corrado/hustle/src/table/tests/supplier.tbl", supp_schema,
                    BLOCK_SIZE);

    write_to_file("/Users/corrado/hustle/src/table/tests/supplier.hsl",
                  *supp);

    supp = read_from_file
            ("/Users/corrado/hustle/src/table/tests/supplier.hsl");
}

TEST_F(SSBTestFixture, GroupByTest) {

//    date = read_from_csv_file
//            ("/Users/corrado/hustle/src/table/tests/date.tbl", date_schema, BLOCK_SIZE);
//
//    write_to_file("/Users/corrado/hustle/src/table/tests/date.hsl",
//                  *date);

    date = read_from_file
            ("/Users/corrado/hustle/src/table/tests/date.hsl");
//
//    std::vector<std::shared_ptr<arrow::Field>> agg_fields =
//            {arrow::field("date key", arrow::int64())};
//    std::vector<std::shared_ptr<arrow::Field>> group_fields =
//            {arrow::field("selling season", arrow::utf8())};
//    std::vector<std::shared_ptr<arrow::Field>> order_fields =
//            {arrow::field("selling season", arrow::utf8())};
//    auto aggregate_op = std::make_shared<hustle::operators::Aggregate>(
//            hustle::operators::AggregateKernels::SUM,
//            agg_fields,
//            group_fields,
//            order_fields);
//
//    // Perform aggregate
//    auto aggregate = aggregate_op->run_operator({date});
//
//    // Print the result. The valid bit will be printed as the first column.
//    if (aggregate != nullptr) aggregate->print();

}

TEST_F(SSBTestFixture, GroupByTest2) {

//    date = read_from_csv_file
//            ("/Users/corrado/hustle/src/table/tests/date.tbl", date_schema, BLOCK_SIZE);
//
//    write_to_file("/Users/corrado/hustle/src/table/tests/date.hsl",
//                  *date);

    date = read_from_file
            ("/Users/corrado/hustle/src/table/tests/date.hsl");

    std::vector<std::shared_ptr<arrow::Field>> group_fields =
            {arrow::field("selling season", arrow::utf8()),
             arrow::field("day of week", arrow::utf8())};
    std::vector<std::string> order_fields =
            {"selling season", "day of week"};

    arrow::Int64Builder indices_builder;
    std::shared_ptr<arrow::Int64Array> indices;
    for(int i=0; i<date->get_num_rows(); i++) {
        indices_builder.Append(i);
    }
    indices_builder.Finish(&indices);


    AggregateUnit agg_unit = {AggregateKernels::SUM,
                              date,
                              indices,
                              "date key"};

    std::vector<AggregateUnit> units = {agg_unit};


    auto aggregate_op = std::make_shared<hustle::operators::Aggregate>(
            units,
            group_fields,
            order_fields);

    // Perform aggregate
    auto aggregate = aggregate_op->run_operator({date});

    // Print the result. The valid bit will be printed as the first column.
    if (aggregate != nullptr) aggregate->print();

}


TEST_F(SSBTestFixture, SSBQ1_1) {

    lineorder = read_from_file
            ("/Users/corrado/hustle/src/table/tests/lineorder.hsl");

    date = read_from_file
            ("/Users/corrado/hustle/src/table/tests/date.hsl");

    // Create select operator for Date.year = 1993
    auto date_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "year",
            arrow::compute::Datum((int64_t) 1993)
    );

    // Create select operator for Lineorder.discount >= 1
    auto lineorder_select_op_1 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::GREATER_EQUAL,
            "discount",
            arrow::compute::Datum((int64_t) 1)
    );

    // Create select operator for Lineorder.discount <= 3
    auto lineorder_select_op_2 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::LESS_EQUAL,
            "discount",
            arrow::compute::Datum((int64_t) 3)
    );

    // Create select operator for Lineorder.quantity < 25
    auto lineorder_select_op_3 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::LESS,
            "quantity",
            arrow::compute::Datum((int64_t) 25)
    );

    // Combine select operators.
    auto lineorder_select_op_composite_1 =
            std::make_shared<hustle::operators::SelectComposite>
                    (lineorder_select_op_1, lineorder_select_op_2,
                     hustle::operators::FilterOperator::AND);
    auto lineorder_select_op_composite_2 =
            std::make_shared<hustle::operators::SelectComposite>
                    (lineorder_select_op_composite_1, lineorder_select_op_3,
                     hustle::operators::FilterOperator::AND);


    // Create natural join operator for left.order date == right.date key
    auto join_op = std::make_shared<hustle::operators::Join>("order date",
            "date key");

    auto t1 = std::chrono::high_resolution_clock::now();

    arrow::compute::Datum left_selection =
            lineorder_select_op_composite_2->get_filter(lineorder);
    arrow::compute::Datum right_selection =
            date_select_op->get_filter(date);

    join_op->hash_join(
            lineorder, left_selection,
            date, right_selection);

    std::cout << "NUM ROWS JOINED = "
              << join_op->get_left_indices().length() << std::endl;

    AggregateUnit agg_unit = {AggregateKernels::SUM,
                              lineorder,
                              join_op->get_left_indices(),
                              "revenue"};

    std::vector<AggregateUnit> units = {agg_unit};
    std::vector<std::shared_ptr<arrow::Field>> group_fields = {};
    std::vector<std::string> order_fields = {};

    auto aggregate_op = std::make_shared<hustle::operators::Aggregate>(
            units,
            group_fields,
            order_fields);

    // Perform aggregate over resulting join table
    auto aggregate = aggregate_op->run_operator({});

    // Print the result. The valid bit will be printed as the first column.
    if (aggregate != nullptr) aggregate->print();
}


TEST_F(SSBTestFixture, SSBQ1_2) {

//    lineorder = read_from_csv_file
//            ("/Users/corrado/hustle/src/table/tests/lineorder.tbl",
//                    lineorder_schema, BLOCK_SIZE);
//
//    write_to_file("/Users/corrado/hustle/src/table/tests/lineorder.hsl",
//            *lineorder);

    auto t11 = std::chrono::high_resolution_clock::now();
    lineorder = read_from_file
            ("/Users/corrado/hustle/src/table/tests/lineorder.hsl");

    auto t22 = std::chrono::high_resolution_clock::now();
    std::cout << "READ FROM HUSTLE FILE TIME = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t22-t11).count
                      () <<
              std::endl;

//    date = read_from_csv_file
//            ("/Users/corrado/hustle/src/table/tests/date.tbl", date_schema, BLOCK_SIZE);
//
//    write_to_file("/Users/corrado/hustle/src/table/tests/date.hsl",
//                  *date);

    date = read_from_file
            ("/Users/corrado/hustle/src/table/tests/date.hsl");

    // Date.year month num = 199401
    auto date_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "year month num",
            arrow::compute::Datum((int64_t) 199401)
    );

    // Create select operator for Lineorder.discount >= 4
    auto lineorder_select_op_1 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::GREATER_EQUAL,
            "discount",
            arrow::compute::Datum((int64_t) 4)
    );

    // Lineorder.discount <= 6
    auto lineorder_select_op_2 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::LESS_EQUAL,
            "discount",
            arrow::compute::Datum((int64_t) 6)
    );

    // Create select operator for Lineorder.quantity >= 26
    auto lineorder_select_op_3 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::GREATER_EQUAL,
            "quantity",
            arrow::compute::Datum((int64_t) 26)
    );

    // Create select operator for Lineorder.quantity <= 35
    auto lineorder_select_op_4 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::LESS_EQUAL,
            "quantity",
            arrow::compute::Datum((int64_t) 35)
    );


    // Combine select operators for lineorder into one composite operator.
    auto lineorder_select_op_composite_1 =
            std::make_shared<hustle::operators::SelectComposite>
                    (lineorder_select_op_1, lineorder_select_op_2,
                     hustle::operators::FilterOperator::AND);

    auto lineorder_select_op_composite_2 =
            std::make_shared<hustle::operators::SelectComposite>
                    (lineorder_select_op_3, lineorder_select_op_4,
                     hustle::operators::FilterOperator::AND);

    auto lineorder_select_op_composite_3 =
            std::make_shared<hustle::operators::SelectComposite>(
                    lineorder_select_op_composite_1,
                    lineorder_select_op_composite_2,
                    hustle::operators::FilterOperator::AND);

    // Create natural join operator for left.order date == right.date key
    // For this query, left corresponds to Lineorder, and right corresponds
    // to Date.
    auto join_op = std::make_shared<hustle::operators::Join>("order date",
                                                             "date key");

    auto t1 = std::chrono::high_resolution_clock::now();

    arrow::compute::Datum left_selection =
            lineorder_select_op_composite_3->get_filter(lineorder);
    arrow::compute::Datum right_selection =
            date_select_op->get_filter(date);

    join_op->hash_join(
            lineorder, left_selection,
            date, right_selection);

    // Create aggregate operator

    std::vector<std::shared_ptr<arrow::Field>> agg_fields =
            {arrow::field("revenue", arrow::utf8())};
//    std::vector<std::shared_ptr<arrow::Field>> group_fields = {};
//    std::vector<std::shared_ptr<arrow::Field>> order_fields = {};
//    auto aggregate_op = std::make_shared<hustle::operators::Aggregate>(
//            hustle::operators::AggregateKernels::SUM,
//            agg_fields,
//            group_fields,
//            order_fields);
//
//    // Perform aggregate over resulting join table
//    auto aggregate = aggregate_op->run_operator({join_table});
//
//    // Print the result. The valid bit will be printed as the first column.
//    if (aggregate != nullptr) aggregate->print();
//
//    auto t2 = std::chrono::high_resolution_clock::now();
//
//    std::cout << "QUERY EXECUTION TIME = " <<
//              std::chrono::duration_cast<std::chrono::milliseconds>
//                      (t2-t1).count() << std::endl;
//
////    join_table->print();
}

TEST_F(SSBTestFixture, SSBQ2_1) {


    lineorder = read_from_file
            ("/Users/corrado/hustle/src/table/tests/lineorder.hsl");

    date = read_from_file
            ("/Users/corrado/hustle/src/table/tests/date.hsl");

    part = read_from_file
            ("/Users/corrado/hustle/src/table/tests/part.hsl");

    supp = read_from_file
            ("/Users/corrado/hustle/src/table/tests/supplier.hsl");

    // IMPORTANT: There is no Datum constructor that accepts a string as a
    // parameter. You must first create a StringScalar and then pass that in.
    // If you pass in a string to the Datum constructor, it will interpet it
    // as boolean.
    auto part_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "category",
            arrow::compute::Datum(std::make_shared<arrow::StringScalar>("MFGR#12"))
    );

    auto supp_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "region",
            arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                    ("AMERICA"))
    );

    auto join_op_1 = std::make_shared<hustle::operators::Join>("order date",
                                                             "date key");
    auto join_op_2 = std::make_shared<hustle::operators::Join>("part key",
                                                               "part key");
    auto join_op_3 = std::make_shared<hustle::operators::Join>("supp key",
                                                               "supp key");

    auto t1 = std::chrono::high_resolution_clock::now();


    arrow::compute::Datum left_selection_1;
    arrow::compute::Datum left_selection_2;
    arrow::compute::Datum left_selection_3;

    arrow::compute::Datum right_selection_1;
    arrow::compute::Datum right_selection_2 =
            part_select_op->get_filter(part);
    arrow::compute::Datum right_selection_3 =
            supp_select_op->get_filter(supp);

//
//    auto join_table_1 = join_op_1->hash_join(
//            lineorder, left_selection_1,
//            date, right_selection_1);
//
//    std::cout << "NUM ROWS = " << join_table_1->get_num_rows() << std::endl;
//
//    auto left_indices_1 = join_op_1->get_left_indices();
//    auto right_indices_1 = join_op_1->get_right_indices();
//
//    auto join_table_2 = join_op_2->hash_join2(
//            lineorder, left_indices_1,
//            part, right_selection_2);
//
////    auto join_table_3 = join_op_3->hash_join2(
////            lineorder, left_indices_1,
////            date, right_selection_2);
//
//    join_table_2->print();
//
////    std::cout << left_indices.make_array()->ToString() << std::endl;
////    std::cout << right_indices.make_array()->ToString() << std::endl;
//
////    auto join_table_2 = join_op->hash_join2(
////            lineorder, left_indices,
////            date, right_indices);
//
//    // Create aggregate operator
//
//    std::vector<std::shared_ptr<arrow::Field>> agg_fields =
//            {arrow::field("revenue", arrow::utf8())};
//    std::vector<std::shared_ptr<arrow::Field>> group_fields = {};
//    std::vector<std::shared_ptr<arrow::Field>> order_fields = {};
//    auto aggregate_op = std::make_shared<hustle::operators::Aggregate>(
//            hustle::operators::AggregateKernels::SUM,
//            agg_fields,
//            group_fields,
//            order_fields);
//
//    // Perform aggregate over resulting join table
////    auto aggregate = aggregate_op->run_operator({join_table_2});
//
//    // Print the result. The valid bit will be printed as the first column.
////    if (aggregate != nullptr) aggregate->print();
//
//    auto t2 = std::chrono::high_resolution_clock::now();
//
//    std::cout << "QUERY EXECUTION TIME = " <<
//              std::chrono::duration_cast<std::chrono::milliseconds>
//                      (t2-t1).count() << std::endl;
}