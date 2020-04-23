#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <table/block.h>
#include <table/util.h>
#include "operators/Aggregate.h"
#include "operators/Join.h"
#include "operators/Select.h"
#include "operators/Project.h"
#include "operators/Predicate.h"

#include <arrow/compute/kernels/filter.h>
#include <fstream>
#include <arrow/compute/kernels/match.h>

using namespace testing;
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
/*
TEST_F(SSBTestFixture, GroupByTest) {



    date = read_from_file
            ("/Users/corrado/hustle/src/table/tests/date.hsl");

    std::shared_ptr<arrow::Int64Builder> value_builder;

    std::shared_ptr<arrow::FixedSizeListBuilder> b;

    std::unique_ptr<arrow::ArrayBuilder> c;
    arrow::MakeBuilder(arrow::default_memory_pool(),
            arrow::fixed_size_list(arrow::int64(),2), &c);

    b.reset(arrow::internal::checked_cast<arrow::FixedSizeListBuilder*>(c
    .release()));
//    auto vals = std::static_pointer_cast<arrow::Int64Builder>
//            (std::make_shared<arrow::Int64Builder>(b->value_builder()));

    auto vals = (arrow::Int64Builder*)(b->value_builder
            ());

    vals->AppendValues({1,2,3,4});

    b->Append();
    b->Append();

    std::shared_ptr<arrow::FixedSizeListArray> out;
    b->Finish(&out);

    std::cout << out->ToString() << std::endl;

    // fetch array, downcast that, then

//    arrow::StructBuilder x(ar)
//    x.




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
//    auto aggregate = aggregate_op->select({date});
//
//    // Print the result. The valid bit will be printed as the first column.
//    if (aggregate != nullptr) aggregate->print();

}

TEST_F(SSBTestFixture, GroupByTest2) {

//    date = read_from_csv_file
//            ("/Users/corrado/hustle/src/table/tests/date.tbl", d_schema, BLOCK_SIZE);
//
//    write_to_file("/Users/corrado/hustle/src/table/tests/date.hsl",
//                  *date);

    date = read_from_file
            ("/Users/corrado/hustle/src/table/tests/date.hsl");

    std::vector<ColumnReference> col_refs;
    col_refs.push_back({date, "selling season"});
    col_refs.push_back({date, "day of week"});
//    col_refs.push_back({date, "weekday fl"});

    std::vector<ColumnReference> order_fields =
            {"selling season", "day of week"};

    arrow::Int64Builder indices_builder;
    std::shared_ptr<arrow::Int64Array> indices;
    for(int i=0; i<date->get_num_rows(); i++) {
        indices_builder.Append(i);
    }
    indices_builder.Finish(&indices);

    std::shared_ptr<arrow::ChunkedArray> filter;

    std::vector<JoinResult> join_result = {
            {date, date->get_column_by_name("date key"), filter, indices}
    };

    AggregateUnit agg_unit = {AggregateKernels::SUM,
                              date,
                              indices,
                              "date key"};

    std::vector<AggregateUnit> units = {agg_unit};

    auto aggregate_op = std::make_shared<hustle::operators::Aggregate>(
            join_result,
            units,
            col_refs,
            order_fields);

    // Perform aggregate
    auto aggregate = aggregate_op->aggregate();

    // Print the result. The valid bit will be printed as the first column.
    if (aggregate != nullptr) aggregate->print();

}
*/
/*
TEST_F(SSBTestFixture, SSBQ1_1) {

    lineorder = read_from_file
            ("/Users/corrado/hustle/src/table/tests/lineorder.hsl");

    date = read_from_file
            ("/Users/corrado/hustle/src/table/tests/date.hsl");

    // date.year = 1993
    auto d_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "year",
            arrow::compute::Datum((int64_t) 1993)
    );

    // lineorder.discount >= 1
    auto lo_select_op_1 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::GREATER_EQUAL,
            "discount",
            arrow::compute::Datum((int64_t) 1)
    );

    // lineorder.discount <= 3
    auto lo_select_op_2 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::LESS_EQUAL,
            "discount",
            arrow::compute::Datum((int64_t) 3)
    );

    // lineorder.quantity < 25
    auto lo_select_op_3 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::LESS,
            "quantity",
            arrow::compute::Datum((int64_t) 25)
    );

    // Combine select operators.
    auto lo_select_op_composite_1 =
            std::make_shared<hustle::operators::SelectComposite>
                    (lo_select_op_1, lo_select_op_2,
                     hustle::operators::FilterOperator::AND);
    auto lo_select_op_composite_2 =
            std::make_shared<hustle::operators::SelectComposite>
                    (lo_select_op_composite_1, lo_select_op_3,
                     hustle::operators::FilterOperator::AND);

    auto t1 = std::chrono::high_resolution_clock::now();

    arrow::compute::Datum lo_selection =
            lo_select_op_composite_2->select(lineorder);
    arrow::compute::Datum d_selection =
            d_select_op->select(date);

    arrow::compute::Datum empty_selection;
    ColumnReference lo_d_ref = {lineorder, "order date"};

    ColumnReference d_ref = {date, "date key"};

    // Join lineorder.order date == date.date key
    Join join_op(lo_d_ref, lo_selection,
                 d_ref, d_selection);

    auto join_result = join_op.hash_join();

    AggregateUnit agg_unit = {AggregateKernels::SUM,
                              lineorder,
                              "revenue"};

    std::vector<AggregateUnit> units = {agg_unit};
    std::vector<ColumnReference> group_bys = {{date, "year"}};
    std::vector<ColumnReference> order_bys = {};

    auto aggregate_op = std::make_shared<hustle::operators::Aggregate>(
            join_result,
            units,
            group_bys,
            order_bys);

    // Perform aggregate over resulting join table
    auto aggregate = aggregate_op->aggregate();

    // Print the result. The valid bit will be printed as the first column.
    std::cout << "\nAGGREGATE" << std::endl;
    if (aggregate != nullptr) aggregate->print();

}

TEST_F(SSBTestFixture, SSBQ1_2) {

    auto t11 = std::chrono::high_resolution_clock::now();

    lineorder = read_from_file
            ("/Users/corrado/hustle/src/table/tests/lineorder.hsl");

    date = read_from_file
            ("/Users/corrado/hustle/src/table/tests/date.hsl");

    auto t22 = std::chrono::high_resolution_clock::now();

    std::cout << "READ FROM FILE TIME = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t22-t11).count() << " ms" << std::endl;

    auto t1 = std::chrono::high_resolution_clock::now();

    // date.year month num = 199401
    auto d_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "year month num",
            arrow::compute::Datum((int64_t) 199401)
    );

    // lineorder.discount >= 4
    auto lo_select_op_1 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::GREATER_EQUAL,
            "discount",
            arrow::compute::Datum((int64_t) 4)
    );

    // lineorder.discount <= 6
    auto lo_select_op_2 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::LESS_EQUAL,
            "discount",
            arrow::compute::Datum((int64_t) 6)
    );

    // lineorder.quantity >= 26
    auto lo_select_op_3 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::GREATER_EQUAL,
            "quantity",
            arrow::compute::Datum((int64_t) 26)
    );

    // lineorder.quantity <= 35
    auto lo_select_op_4 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::LESS_EQUAL,
            "quantity",
            arrow::compute::Datum((int64_t) 35)
    );

    // Combine select operators
    auto lo_select_op_composite_1 =
            std::make_shared<hustle::operators::SelectComposite>
                    (lo_select_op_1, lo_select_op_2,
                     hustle::operators::FilterOperator::AND);

    auto lo_select_op_composite_2 =
            std::make_shared<hustle::operators::SelectComposite>
                    (lo_select_op_3, lo_select_op_4,
                     hustle::operators::FilterOperator::AND);

    auto lo_select_op_composite_3 =
            std::make_shared<hustle::operators::SelectComposite>(
                    lo_select_op_composite_1,
                    lo_select_op_composite_2,
                    hustle::operators::FilterOperator::AND);

    auto lo_selection = lo_select_op_composite_3->select
            (lineorder);
    auto d_selection = d_select_op->select(date);

    ColumnReference lo_d_ref = {lineorder, "order date"};

    ColumnReference d_ref = {date, "date key"};

    // Join lineorder.order date == date.date key
    Join join_op(lo_d_ref, lo_selection,
                 d_ref, d_selection);

    auto join_result = join_op.hash_join();

    AggregateUnit agg_unit = {AggregateKernels::SUM,
                              lineorder,
                              "revenue"};

    std::vector<AggregateUnit> units = {agg_unit};
    std::vector<ColumnReference> group_bys = {};
    std::vector<ColumnReference> order_bys = {};

    auto aggregate_op = std::make_shared<hustle::operators::Aggregate>(
            join_result,
            units,
            group_bys,
            order_bys);

    // Perform aggregate over resulting join table
    auto aggregate = aggregate_op->aggregate();

    // Print the result. The valid bit will be printed as the first column.
    if (aggregate != nullptr) aggregate->print();

    auto t2 = std::chrono::high_resolution_clock::now();

    std::cout << "QUERY EXECUTION TIME = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t2-t1).count() << " ms" << std::endl;
}

TEST_F(SSBTestFixture, SSBQ1_3) {

    auto t11 = std::chrono::high_resolution_clock::now();

    lineorder = read_from_file
            ("/Users/corrado/hustle/src/table/tests/lineorder.hsl");

    date = read_from_file
            ("/Users/corrado/hustle/src/table/tests/date.hsl");

    auto t22 = std::chrono::high_resolution_clock::now();

    std::cout << "READ FROM FILE TIME = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t22-t11).count() << " ms" << std::endl;

    auto t1 = std::chrono::high_resolution_clock::now();

    // date.week num in year = 6
    auto d_select_op_1 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "week num in year",
            arrow::compute::Datum((int64_t) 6)
    );

    // date.year = 1994
    auto d_select_op_2 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "year",
            arrow::compute::Datum((int64_t) 1994)
    );

    // Combine select operators
    auto d_select_op_composite_1 =
            std::make_shared<hustle::operators::SelectComposite>
                    (d_select_op_1, d_select_op_2,
                     hustle::operators::FilterOperator::AND);

    // lineorder.discount >= 4
    auto lo_select_op_1 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::GREATER_EQUAL,
            "discount",
            arrow::compute::Datum((int64_t) 5)
    );

    // lineorder.discount <= 6
    auto lo_select_op_2 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::LESS_EQUAL,
            "discount",
            arrow::compute::Datum((int64_t) 7)
    );

    // lineorder.quantity >= 26
    auto lo_select_op_3 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::GREATER_EQUAL,
            "quantity",
            arrow::compute::Datum((int64_t) 26)
    );

    // lineorder.quantity <= 35
    auto lo_select_op_4 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::LESS_EQUAL,
            "quantity",
            arrow::compute::Datum((int64_t) 35)
    );

    // Combine select operators
    auto lo_select_op_composite_1 =
            std::make_shared<hustle::operators::SelectComposite>
                    (lo_select_op_1, lo_select_op_2,
                     hustle::operators::FilterOperator::AND);

    auto lo_select_op_composite_2 =
            std::make_shared<hustle::operators::SelectComposite>
                    (lo_select_op_3, lo_select_op_4,
                     hustle::operators::FilterOperator::AND);

    auto lo_select_op_composite_3 =
            std::make_shared<hustle::operators::SelectComposite>(
                    lo_select_op_composite_1,
                    lo_select_op_composite_2,
                    hustle::operators::FilterOperator::AND);

    auto lo_selection = lo_select_op_composite_3->select
            (lineorder);
    auto d_selection = d_select_op_composite_1->select(date);

    ColumnReference lo_d_ref = {lineorder, "order date"};

    ColumnReference d_ref = {date, "date key"};

    // Join lineorder.order date == date.date key
    Join join_op(lo_d_ref, lo_selection,
                 d_ref, d_selection);

    auto join_result = join_op.hash_join();

    AggregateUnit agg_unit = {AggregateKernels::SUM,
                              lineorder,
                              "revenue"};

    std::vector<AggregateUnit> units = {agg_unit};
    std::vector<ColumnReference> group_bys = {};
    std::vector<ColumnReference> order_bys = {};

    auto aggregate_op = std::make_shared<hustle::operators::Aggregate>(
            join_result,
            units,
            group_bys,
            order_bys);

    // Perform aggregate over resulting join table
    auto aggregate = aggregate_op->aggregate();

    // Print the result. The valid bit will be printed as the first column.
    if (aggregate != nullptr) aggregate->print();

    auto t2 = std::chrono::high_resolution_clock::now();

    std::cout << "QUERY EXECUTION TIME = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t2-t1).count() << " ms" << std::endl;
}

TEST_F(SSBTestFixture, SSBQ2_1) {

    auto t11 = std::chrono::high_resolution_clock::now();

    lineorder = read_from_file
            ("/Users/corrado/hustle/src/table/tests/lineorder.hsl");

    date = read_from_file
            ("/Users/corrado/hustle/src/table/tests/date.hsl");

    part = read_from_file
            ("/Users/corrado/hustle/src/table/tests/part.hsl");

    supp = read_from_file
            ("/Users/corrado/hustle/src/table/tests/supplier.hsl");

    auto t22 = std::chrono::high_resolution_clock::now();

    std::cout << "READ FROM FILE TIME = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t22-t11).count() << " ms" << std::endl;

    auto t1 = std::chrono::high_resolution_clock::now();

    // IMPORTANT: There is no Datum constructor that accepts a string as a
    // parameter. You must first create a StringScalar and then pass that in.
    // If you pass in a string to the Datum constructor, it will interpret it
    // as boolean.
    auto p_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "category",
            arrow::compute::Datum(
                    std::make_shared<arrow::StringScalar>("MFGR#12"))
    );

    auto s_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "region",
            arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                                          ("AMERICA"))
    );


    arrow::compute::Datum empty_selection;

    arrow::compute::Datum p_selection =
            p_select_op->select(part);
    arrow::compute::Datum s_selection =
            s_select_op->select(supp);

    ColumnReference lo_s_ref = {lineorder, "supp key"};
    ColumnReference lo_p_ref = {lineorder, "part key"};
    ColumnReference lo_d_ref = {lineorder, "order date"};

    ColumnReference s_ref = {supp, "supp key"};
    ColumnReference p_ref = {part, "part key"};
    ColumnReference d_ref = {date, "date key"};

    auto join_op_1 = std::make_shared<hustle::operators::Join>(
            lo_s_ref, empty_selection,
            s_ref, s_selection);


    auto join_result_1 = join_op_1->hash_join();

    auto join_op_2 = std::make_shared<hustle::operators::Join>(
            join_result_1, lo_p_ref,
            p_ref, p_selection);


    auto join_result_2 = join_op_2->hash_join();

    auto join_op_3 = std::make_shared<hustle::operators::Join>(
            join_result_2, lo_d_ref,
            d_ref, empty_selection);

    auto join_result_3 = join_op_3->hash_join();

    std::vector<ColumnReference> group_bys = {{date, "year"}, {part, "brand1"}};
    std::vector<ColumnReference> order_bys = {{date, "year"}, {part, "brand1"}};

AggregateUnit agg_unit = {AggregateKernels::SUM,
                              lineorder,
                              "revenue"};

    std::vector<AggregateUnit> units = {agg_unit};

    auto aggregate_op = std::make_shared<hustle::operators::Aggregate>(
            join_result_3,
            units,
            group_bys,
            order_bys);

    // Perform aggregate
    auto aggregate = aggregate_op->aggregate();

    // Print the result. The valid bit will be printed as the first column.
    if (aggregate != nullptr) aggregate->print();

    auto t2 = std::chrono::high_resolution_clock::now();

    std::cout << "QUERY EXECUTION TIME = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t2-t1).count() << " ms" << std::endl;

}

TEST_F(SSBTestFixture, SSBQ2_2) {

    auto t11 = std::chrono::high_resolution_clock::now();

    lineorder = read_from_file
            ("/Users/corrado/hustle/src/table/tests/lineorder.hsl");

    date = read_from_file
            ("/Users/corrado/hustle/src/table/tests/date.hsl");

    part = read_from_file
            ("/Users/corrado/hustle/src/table/tests/part.hsl");

    supp = read_from_file
            ("/Users/corrado/hustle/src/table/tests/supplier.hsl");

    auto t22 = std::chrono::high_resolution_clock::now();

    std::cout << "READ FROM FILE TIME = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t22-t11).count() << " ms" << std::endl;

    auto t1 = std::chrono::high_resolution_clock::now();

    // IMPORTANT: There is no Datum constructor that accepts a string as a
    // parameter. You must first create a StringScalar and then pass that in.
    // If you pass in a string to the Datum constructor, it will interpret it
    // as boolean.
    auto p_select_op_1 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::GREATER_EQUAL,
            "brand1",
            arrow::compute::Datum(
                    std::make_shared<arrow::StringScalar>("MFGR#2221"))
    );

    auto p_select_op_2 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::LESS_EQUAL,
            "brand1",
            arrow::compute::Datum(
                    std::make_shared<arrow::StringScalar>("MFGR#2228"))
    );

    auto p_select_op_composite_1 =
            std::make_shared<hustle::operators::SelectComposite>
                    (p_select_op_1, p_select_op_2,
                     hustle::operators::FilterOperator::AND);

    auto s_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "region",
            arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                                          ("ASIA"))
    );


    arrow::compute::Datum empty_selection;

    arrow::compute::Datum p_selection =
            p_select_op_composite_1->select(part);
    arrow::compute::Datum s_selection =
            s_select_op->select(supp);

    ColumnReference lo_s_ref = {lineorder, "supp key"};
    ColumnReference lo_p_ref = {lineorder, "part key"};
    ColumnReference lo_d_ref = {lineorder, "order date"};

    ColumnReference s_ref = {supp, "supp key"};
    ColumnReference p_ref = {part, "part key"};
    ColumnReference d_ref = {date, "date key"};

    auto join_op_1 = std::make_shared<hustle::operators::Join>(
            lo_s_ref, empty_selection,
            s_ref, s_selection);

    auto join_result_1 = join_op_1->hash_join();

    auto join_op_2 = std::make_shared<hustle::operators::Join>(
            join_result_1, lo_p_ref,
            p_ref, p_selection);

    auto join_result_2 = join_op_2->hash_join();

    auto join_op_3 = std::make_shared<hustle::operators::Join>(
            join_result_2, lo_d_ref,
            d_ref, empty_selection);

    auto join_result_3 = join_op_3->hash_join();



    std::vector<ColumnReference> group_bys = {{date, "year"}, {part, "brand1"}};
    std::vector<ColumnReference> order_bys = {{date, "year"}, {part,"brand1"}};
    AggregateUnit agg_unit = {AggregateKernels::SUM,
                              lineorder,
                              "revenue"};

    std::vector<AggregateUnit> units = {agg_unit};

    auto aggregate_op = std::make_shared<hustle::operators::Aggregate>(
            join_result_3,
            units,
            group_bys,
            order_bys);

    // Perform aggregate
    auto aggregate = aggregate_op->aggregate();

    // Print the result. The valid bit will be printed as the first column.
    if (aggregate != nullptr) aggregate->print();

    auto t2 = std::chrono::high_resolution_clock::now();

    std::cout << "QUERY EXECUTION TIME = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t2-t1).count() << " ms" << std::endl;

}

TEST_F(SSBTestFixture, SSBQ2_3) {

    auto t11 = std::chrono::high_resolution_clock::now();

    lineorder = read_from_file
            ("/Users/corrado/hustle/src/table/tests/lineorder.hsl");

    date = read_from_file
            ("/Users/corrado/hustle/src/table/tests/date.hsl");

    part = read_from_file
            ("/Users/corrado/hustle/src/table/tests/part.hsl");

    supp = read_from_file
            ("/Users/corrado/hustle/src/table/tests/supplier.hsl");

    auto t22 = std::chrono::high_resolution_clock::now();

    std::cout << "READ FROM FILE TIME = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t22-t11).count() << " ms" << std::endl;

    auto t1 = std::chrono::high_resolution_clock::now();

    // IMPORTANT: There is no Datum constructor that accepts a string as a
    // parameter. You must first create a StringScalar and then pass that in.
    // If you pass in a string to the Datum constructor, it will interpret it
    // as boolean.
    auto p_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "brand1",
            arrow::compute::Datum(
                    std::make_shared<arrow::StringScalar>("MFGR#2221"))
    );

    auto s_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "region",
            arrow::compute::Datum(std::make_shared<arrow::StringScalar>
                                          ("EUROPE"))
    );


    arrow::compute::Datum empty_selection;

    arrow::compute::Datum p_selection =
            p_select_op->select(part);
    arrow::compute::Datum s_selection =
            s_select_op->select(supp);


    ColumnReference lo_s_ref = {lineorder, "supp key"};
    ColumnReference lo_p_ref = {lineorder, "part key"};
    ColumnReference lo_d_ref = {lineorder, "order date"};

    ColumnReference s_ref = {supp, "supp key"};
    ColumnReference p_ref = {part, "part key"};
    ColumnReference d_ref = {date, "date key"};

    auto join_op_1 = std::make_shared<hustle::operators::Join>(
            lo_s_ref, empty_selection,
            s_ref, s_selection);

    auto join_result_1 = join_op_1->hash_join();

    auto join_op_2 = std::make_shared<hustle::operators::Join>(
            join_result_1, lo_p_ref,
            p_ref, p_selection);

    auto join_result_2 = join_op_2->hash_join();

    auto join_op_3 = std::make_shared<hustle::operators::Join>(
            join_result_2, lo_d_ref,
            d_ref, empty_selection);

    auto join_result_3 = join_op_3->hash_join();




    std::vector<ColumnReference> group_bys = {{date, "year"}, {part, "brand1"}};
    std::vector<ColumnReference> order_bys = {{date, "year"}, {part,"brand1"}};
    AggregateUnit agg_unit = {AggregateKernels::SUM,
                              lineorder,
                              "revenue"};

    std::vector<AggregateUnit> units = {agg_unit};

    auto aggregate_op = std::make_shared<hustle::operators::Aggregate>(
            join_result_3,
            units,
            group_bys,
            order_bys);

    // Perform aggregate
    auto aggregate = aggregate_op->aggregate();

    // Print the result. The valid bit will be printed as the first column.
    if (aggregate != nullptr) aggregate->print();

    auto t2 = std::chrono::high_resolution_clock::now();

    std::cout << "QUERY EXECUTION TIME = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t2-t1).count() << " ms" << std::endl;

}

TEST_F(SSBTestFixture, SSBQ3_1) {

    auto t11 = std::chrono::high_resolution_clock::now();

    lineorder = read_from_file
            ("/Users/corrado/hustle/src/table/tests/lineorder.hsl");

    date = read_from_file
            ("/Users/corrado/hustle/src/table/tests/date.hsl");

    supp = read_from_file
            ("/Users/corrado/hustle/src/table/tests/supplier.hsl");

    cust = read_from_file
            ("/Users/corrado/hustle/src/table/tests/customer.hsl");

    auto t22 = std::chrono::high_resolution_clock::now();

    std::cout << "READ FROM FILE TIME = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t22-t11).count() << " ms" << std::endl;


    auto t1 = std::chrono::high_resolution_clock::now();

    // IMPORTANT: There is no Datum constructor that accepts a string as a
    // parameter. You must first create a StringScalar and then pass that in.
    // If you pass in a string to the Datum constructor, it will interpret it
    // as boolean.
    auto c_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "region",
            arrow::compute::Datum(
                    std::make_shared<arrow::StringScalar>("ASIA"))
    );

    auto s_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "region",
            arrow::compute::Datum(
                    std::make_shared<arrow::StringScalar>("ASIA"))
    );
    auto d_select_op_1 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::GREATER_EQUAL,
            "year",
            arrow::compute::Datum((int64_t) 1992)
    );
    auto d_select_op_2 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::LESS_EQUAL,
            "year",
            arrow::compute::Datum((int64_t) 1997)
    );
    auto d_select_op_composite_1 =
            std::make_shared<hustle::operators::SelectComposite>
                    (d_select_op_1, d_select_op_2,
                     hustle::operators::FilterOperator::AND);

    arrow::compute::Datum empty_selection;

    auto d_selection = d_select_op_composite_1->select(date);
    auto s_selection = s_select_op->select(supp);
    auto c_selection = c_select_op->select(cust);

    ColumnReference lo_s_ref = {lineorder, "supp key"};
    ColumnReference lo_c_ref = {lineorder, "cust key"};
    ColumnReference lo_p_ref = {lineorder, "part key"};
    ColumnReference lo_d_ref = {lineorder, "order date"};

    ColumnReference s_ref = {supp, "supp key"};
    ColumnReference c_ref = {cust, "cust key"};
    ColumnReference p_ref = {part, "part key"};
    ColumnReference d_ref = {date, "date key"};

    auto join_op_1 = std::make_shared<hustle::operators::Join>(
            lo_s_ref, empty_selection,
            s_ref, s_selection);

    auto join_result_1 = join_op_1->hash_join();

    auto join_op_2 = std::make_shared<hustle::operators::Join>(
            join_result_1, lo_c_ref,
            c_ref, c_selection);

    auto join_result_2 = join_op_2->hash_join();

    auto join_op_3 = std::make_shared<hustle::operators::Join>(
            join_result_2, lo_d_ref,
            d_ref, d_selection);

    auto join_result_3 = join_op_3->hash_join();



    std::vector<ColumnReference> group_bys = {{cust, "nation"},
                                              {supp, "nation"},
                                              {date,"year"}};
    //TODO(nicholas): We currently do not support sorting on the aggregate
    // column, so this result will not look as expected.
    std::vector<ColumnReference> order_bys = {{date,"year"}};
    AggregateUnit agg_unit = {AggregateKernels::SUM,
                              lineorder,
                              "revenue"};

    std::vector<AggregateUnit> units = {agg_unit};

    auto aggregate_op = std::make_shared<hustle::operators::Aggregate>(
            join_result_3,
            units,
            group_bys,
            order_bys);

    // Perform aggregate
    auto aggregate = aggregate_op->aggregate();

    // Print the result. The valid bit will be printed as the first column.
    if (aggregate != nullptr) aggregate->print();

    auto t2 = std::chrono::high_resolution_clock::now();

    std::cout << "QUERY EXECUTION TIME = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t2-t1).count() << " ms" << std::endl;

}

TEST_F(SSBTestFixture, SSBQ3_2) {
    auto t11 = std::chrono::high_resolution_clock::now();

    lineorder = read_from_file
            ("/Users/corrado/hustle/src/table/tests/lineorder.hsl");

    date = read_from_file
            ("/Users/corrado/hustle/src/table/tests/date.hsl");

    supp = read_from_file
            ("/Users/corrado/hustle/src/table/tests/supplier.hsl");

    cust = read_from_file
            ("/Users/corrado/hustle/src/table/tests/customer.hsl");

    auto t22 = std::chrono::high_resolution_clock::now();

    std::cout << "READ FROM FILE TIME = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t22-t11).count() << " ms" << std::endl;

    auto t1 = std::chrono::high_resolution_clock::now();

    // IMPORTANT: There is no Datum constructor that accepts a string as a
    // parameter. You must first create a StringScalar and then pass that in.
    // If you pass in a string to the Datum constructor, it will interpret it
    // as boolean.
    auto c_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "nation",
            arrow::compute::Datum(
                    std::make_shared<arrow::StringScalar>("UNITED STATES"))
    );

    auto s_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "nation",
            arrow::compute::Datum(
                    std::make_shared<arrow::StringScalar>("UNITED STATES"))
    );
    auto d_select_op_1 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::GREATER_EQUAL,
            "year",
            arrow::compute::Datum((int64_t) 1992)
    );
    auto d_select_op_2 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::LESS_EQUAL,
            "year",
            arrow::compute::Datum((int64_t) 1997)
    );
    auto d_select_op_composite_1 =
            std::make_shared<hustle::operators::SelectComposite>
                    (d_select_op_1, d_select_op_2,
                     hustle::operators::FilterOperator::AND);

    arrow::compute::Datum empty_selection;

    auto d_selection = d_select_op_composite_1->select(date);
    auto s_selection = s_select_op->select(supp);
    auto c_selection = c_select_op->select(cust);


    ColumnReference lo_s_ref = {lineorder, "supp key"};
    ColumnReference lo_c_ref = {lineorder, "cust key"};
    ColumnReference lo_p_ref = {lineorder, "part key"};
    ColumnReference lo_d_ref = {lineorder, "order date"};

    ColumnReference s_ref = {supp, "supp key"};
    ColumnReference c_ref = {cust, "cust key"};
    ColumnReference p_ref = {part, "part key"};
    ColumnReference d_ref = {date, "date key"};

    auto join_op_1 = std::make_shared<hustle::operators::Join>(
            lo_s_ref, empty_selection,
            s_ref, s_selection);

    auto join_result_1 = join_op_1->hash_join();

    auto join_op_2 = std::make_shared<hustle::operators::Join>(
            join_result_1, lo_c_ref,
            c_ref, c_selection);

    auto join_result_2 = join_op_2->hash_join();

    auto join_op_3 = std::make_shared<hustle::operators::Join>(
            join_result_2, lo_d_ref,
            d_ref, d_selection);

    auto join_result_3 = join_op_3->hash_join();


    //TODO(nicholas): Result is incorrectly grouped when two column
    // references share the same name.
    std::vector<ColumnReference> group_bys = {{cust, "city"},
                                              {supp, "city"},
                                              {date,"year"}};
    //TODO(nicholas): We currently do not support sorting on the aggregate
    // column, so this result will not look as expected.
    //TODO(nicholas): The strings in order_bys must correspond to the
    // ColumnReferences in group_bys. Since we group_by year last, we must
    // put two placeholder empty string before it. Need to fix this.
    std::vector<ColumnReference> order_bys = {{date,"year"}};
    AggregateUnit agg_unit = {AggregateKernels::SUM,
                              lineorder,
                              "revenue"};

    std::vector<AggregateUnit> units = {agg_unit};

    auto aggregate_op = std::make_shared<hustle::operators::Aggregate>(
            join_result_3,
            units,
            group_bys,
            order_bys);

    // Perform aggregate
    auto aggregate = aggregate_op->aggregate();

    // Print the result. The valid bit will be printed as the first column.
    if (aggregate != nullptr) aggregate->print();

    auto t2 = std::chrono::high_resolution_clock::now();

    std::cout << "QUERY EXECUTION TIME = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t2-t1).count() << " ms" << std::endl;

}

TEST_F(SSBTestFixture, SSBQ3_3) {
    auto t11 = std::chrono::high_resolution_clock::now();

    lineorder = read_from_file
            ("/Users/corrado/hustle/src/table/tests/lineorder.hsl");

    date = read_from_file
            ("/Users/corrado/hustle/src/table/tests/date.hsl");

    supp = read_from_file
            ("/Users/corrado/hustle/src/table/tests/supplier.hsl");

    cust = read_from_file
            ("/Users/corrado/hustle/src/table/tests/customer.hsl");

    auto t22 = std::chrono::high_resolution_clock::now();

    std::cout << "READ FROM FILE TIME = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t22-t11).count() << " ms" << std::endl;

    auto t1 = std::chrono::high_resolution_clock::now();

    // IMPORTANT: There is no Datum constructor that accepts a string as a
    // parameter. You must first create a StringScalar and then pass that in.
    // If you pass in a string to the Datum constructor, it will interpret it
    // as boolean.
    auto c_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "nation",
            arrow::compute::Datum(
                    std::make_shared<arrow::StringScalar>("UNITED STATES"))
    );

    auto s_select_op = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::EQUAL,
            "nation",
            arrow::compute::Datum(
                    std::make_shared<arrow::StringScalar>("UNITED STATES"))
    );
    auto d_select_op_1 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::GREATER_EQUAL,
            "year",
            arrow::compute::Datum((int64_t) 1992)
    );
    auto d_select_op_2 = std::make_shared<hustle::operators::Select>(
            arrow::compute::CompareOperator::LESS_EQUAL,
            "year",
            arrow::compute::Datum((int64_t) 1997)
    );
    auto d_select_op_composite_1 =
            std::make_shared<hustle::operators::SelectComposite>
                    (d_select_op_1, d_select_op_2,
                     hustle::operators::FilterOperator::AND);

    arrow::compute::Datum empty_selection;

    auto d_selection = d_select_op_composite_1->select(date);
    auto s_selection = s_select_op->select(supp);
    auto c_selection = c_select_op->select(cust);

    ColumnReference lo_s_ref = {lineorder, "supp key"};
    ColumnReference lo_c_ref = {lineorder, "cust key"};
    ColumnReference lo_p_ref = {lineorder, "part key"};
    ColumnReference lo_d_ref = {lineorder, "order date"};

    ColumnReference s_ref = {supp, "supp key"};
    ColumnReference c_ref = {cust, "cust key"};
    ColumnReference p_ref = {part, "part key"};
    ColumnReference d_ref = {date, "date key"};

    auto join_op_1 = std::make_shared<hustle::operators::Join>(
            lo_s_ref, empty_selection,
            s_ref, s_selection);

    auto join_result_1 = join_op_1->hash_join();

    auto join_op_2 = std::make_shared<hustle::operators::Join>(
            join_result_1, lo_c_ref,
            c_ref, c_selection);

    auto join_result_2 = join_op_2->hash_join();

    auto join_op_3 = std::make_shared<hustle::operators::Join>(
            join_result_2, lo_d_ref,
            d_ref, d_selection);

    auto join_result_3 = join_op_3->hash_join();

    std::vector<ColumnReference> group_bys = {{date,"year"},
                                              {cust, "city"},
                                              {supp, "city"},
                                              };
    //TODO(nicholas): We currently do not support sorting on the aggregate
    // column, so this result will not look as expected.
    std::vector<ColumnReference> order_bys = {{date,"year"}};
    AggregateUnit agg_unit = {AggregateKernels::SUM,
                              lineorder,
                              "revenue"};

    std::vector<AggregateUnit> units = {agg_unit};

    auto aggregate_op = std::make_shared<hustle::operators::Aggregate>(
            join_result_3,
            units,
            group_bys,
            order_bys);

    // Perform aggregate
    auto aggregate = aggregate_op->aggregate();

    // Print the result. The valid bit will be printed as the first column.
    if (aggregate != nullptr) aggregate->print();

    auto t2 = std::chrono::high_resolution_clock::now();

    std::cout << "QUERY EXECUTION TIME = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t2-t1).count() << " ms" << std::endl;

}
*/
TEST_F(SSBTestFixture, SSBQ4_1) {

    auto t11 = std::chrono::high_resolution_clock::now();

    lineorder = read_from_file
            ("/Users/corrado/hustle/src/table/tests/lineorder.hsl");

    date = read_from_file
            ("/Users/corrado/hustle/src/table/tests/date.hsl");

    part = read_from_file
            ("/Users/corrado/hustle/src/table/tests/part.hsl");

    supp = read_from_file
            ("/Users/corrado/hustle/src/table/tests/supplier.hsl");

    cust = read_from_file
            ("/Users/corrado/hustle/src/table/tests/customer.hsl");

    auto t22 = std::chrono::high_resolution_clock::now();

    std::cout << "READ FROM FILE TIME = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t22-t11).count() << " ms" << std::endl;

    auto t1 = std::chrono::high_resolution_clock::now();

    // IMPORTANT: There is no Datum constructor that accepts a string as a
    // parameter. You must first create a StringScalar and then pass that in.
    // If you pass in a string to the Datum constructor, it will interpet it
    // as boolean.
    auto p_pred_1 = Predicate{
            {part, "mfgr"},
            arrow::compute::CompareOperator::EQUAL,
            arrow::compute::Datum(
                    std::make_shared<arrow::StringScalar>("MFGR#1"))
    };
    auto p_pred_2 = Predicate {
            {part, "mfgr"},
            arrow::compute::CompareOperator::EQUAL,
            arrow::compute::Datum(
                    std::make_shared<arrow::StringScalar>("MFGR#2"))
    };

    auto p_pred_node_1 =
            std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(p_pred_1));

    auto p_pred_node_2 =
            std::make_shared<PredicateNode>(
                    std::make_shared<Predicate>(p_pred_2));

    auto p_connective_node_1 = std::make_shared<ConnectiveNode>(
            p_pred_node_1,
                    p_pred_node_2,
            hustle::operators::FilterOperator::OR
    );

    auto p_tree = std::make_shared<PredicateTree>(
            p_connective_node_1);

    auto c_pred_1 = Predicate {
            {cust, "region"},
            arrow::compute::CompareOperator::EQUAL,
            arrow::compute::Datum(
                    std::make_shared<arrow::StringScalar>("AMERICA"))};

    PredicateNode c_pred_node_1 = PredicateNode(
            std::make_shared<Predicate>(c_pred_1));

    auto c_tree = std::make_shared<PredicateTree>(
            std::make_shared<Node>(c_pred_node_1));

    auto s_pred_1 = Predicate {
            {supp, "region"},
            arrow::compute::CompareOperator::EQUAL,
            arrow::compute::Datum(
                    std::make_shared<arrow::StringScalar>("AMERICA"))};

    PredicateNode s_pred_node_1 = PredicateNode(
            std::make_shared<Predicate>(s_pred_1));

    auto s_tree = std::make_shared<PredicateTree>(
            std::make_shared<Node>(s_pred_node_1));

    auto p_select_op = Select(part, p_tree);
    auto c_select_op = Select(cust, c_tree);
    auto s_select_op = Select(supp, s_tree);


    ColumnReference lo_s_ref = {lineorder, "supp key"};
    ColumnReference lo_c_ref = {lineorder, "cust key"};
    ColumnReference lo_p_ref = {lineorder, "part key"};
    ColumnReference lo_d_ref = {lineorder, "order date"};

    ColumnReference s_ref = {supp, "supp key"};
    ColumnReference c_ref = {cust, "cust key"};
    ColumnReference p_ref = {part, "part key"};
    ColumnReference d_ref = {date, "date key"};

    auto p_selection = p_select_op.run();
    auto c_selection = c_select_op.run();
    auto s_selection = s_select_op.run();

    JoinPredicate p_jpred = {lo_p_ref, arrow::compute::EQUAL, p_ref};
    JoinPredicate c_jpred = {lo_c_ref, arrow::compute::EQUAL, c_ref};
    JoinPredicate s_jpred = {lo_s_ref, arrow::compute::EQUAL, s_ref};
    JoinPredicate d_jpred = {lo_d_ref, arrow::compute::EQUAL, d_ref};

//    JoinGraph graph({{p_jpred}, {c_jpred}, {s_jpred}, {d_jpred}});
    JoinGraph graph1({{p_jpred}});
    JoinGraph graph2({{c_jpred}});
    JoinGraph graph3({{s_jpred}});
    JoinGraph graph4({{d_jpred}});

    auto result_0 = std::make_shared<hustle::operators::OperatorResult>();

    result_0->add_table(lineorder);
    result_0->append(p_selection);

    Join j_op_1(result_0, graph1);
    auto result_1 = j_op_1.run();
    result_1->append(c_selection);

    for (auto &t : result_1->lazy_tables_) {
        std::cout << t.table->get_num_rows() << " " << t.indices.length() <<
                  std::endl;

    }
    std::cout << std::endl;

    Join j_op_2(result_1, graph2);
    auto result_2 = j_op_2.run();
    result_2->append(s_selection);

    for (auto &t : result_2->lazy_tables_) {
        std::cout << t.table->get_num_rows() << " " << t.indices.length() <<
                  std::endl;
    }
    std::cout << std::endl;

    Join j_op_3(result_2, graph3);
    auto result_3 = j_op_3.run();
    result_3->add_table(date);

    for (auto &t : result_3->lazy_tables_) {
        std::cout << t.table->get_num_rows() << " " << t.indices.length() <<
                  std::endl;
    }
    std::cout << std::endl;

    Join j_op_4(result_3, graph4);
    auto result_4 = j_op_4.run();





    for (auto &t : result_3->lazy_tables_) {
        std::cout << t.table->get_num_rows() << " " << t.indices.length() <<
                  std::endl;
    }
    std::cout << std::endl;

    for (auto &t : result_4->lazy_tables_) {
        std::cout << t.table->get_num_rows() << " " << t.indices.length() <<
                  std::endl;

    }

    std::vector<ColumnReference> group_bys = {{date,"year"},
                                              {cust, "nation"}};
    //TODO(nicholas): We currently do not support sorting on the aggregate
    // column, so this result will not look as expected.
    std::vector<ColumnReference> order_bys = {{date,"year"},
                                              {cust, "nation"}};
    AggregateUnit agg_unit = {AggregateKernels::SUM,
                              lineorder,
                              "revenue"};

    std::vector<AggregateUnit> units = {agg_unit};

    auto aggregate_op = std::make_shared<hustle::operators::Aggregate>(
            result_4,
            units,
            group_bys,
            order_bys);

    // Perform aggregate
    auto aggregate = aggregate_op->aggregate();

    // Print the result. The valid bit will be printed as the first column.
    if (aggregate != nullptr) aggregate->print();

    auto t2 = std::chrono::high_resolution_clock::now();

    std::cout << "QUERY EXECUTION TIME = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t2-t1).count() << " ms" << std::endl;

}

TEST_F(SSBTestFixture, testest){

    date = read_from_file
            ("/Users/corrado/hustle/src/table/tests/date.hsl");

    arrow::Int64Builder indices_builder;
    std::shared_ptr<arrow::Int64Array> indices;
    for(int i=0; i<date->get_num_rows(); i++) {
        indices_builder.Append(i);
    }
    indices_builder.Finish(&indices);

    std::shared_ptr<arrow::Int64Array> indices2;
    for(int i=0; i<date->get_num_rows(); i+=2) {
        indices_builder.Append(i);
    }
    indices_builder.Finish(&indices2);

    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());
    arrow::compute::TakeOptions take_options;
    arrow::compute::Datum out_indices;
    std::shared_ptr<arrow::ChunkedArray> out_ref;

    arrow::compute::Match(&function_context,
                          indices2, indices, &out_indices);

        std::cout << out_indices.make_array()->ToString() <<
    std::endl;


}