//
// Created by Sandhya Kannan on 3/10/20.
//

#include <arrow/compute/test_util.h>
#include "arrow/compute/kernels/compare.h"
#include <arrow/compute/api.h>
#include <arrow/api.h>

#include <gtest/gtest.h>
#include <chrono>

#include <bitweaving/types.h>
#include <bitweaving/options.h>
#include <bitweaving/column.h>
#include <bitweaving/bitvector.h>
#include <bitweaving/iterator.h>
#include <bitweaving/bitweaving_compare.h>
#include <table/table.h>
#include <table/util.h>
#include <operators/Select.h>
#include <operators/Join.h>
#include <operators/JoinGraph.h>
#include <bitweaving/rdtsc.h>
#include <operators/Aggregate.h>
#include <execution/ExecutionPlan.hpp>
#include "scheduler/Scheduler.hpp"

#include "bitweaving/bitweaving_util.h"
#include "bitweaving_test_base.h"

using namespace hustle::operators;

namespace hustle::bitweaving {

TEST(BitweavingTest, simpleTest) {

    size_t num_codes = 1000;
    uint8_t bit_width = 6;
    auto *codes1 = new Code[num_codes];
    auto *codes2 = new Code[num_codes];

    // Generate data
    Code mask = (1ULL << bit_width) - 1;
    for (size_t i = 0; i < num_codes; i++) {
        codes1[i] = rand() & mask;
        codes2[i] = rand() & mask;
    }

    auto *results = new Code[num_codes];
    size_t num_results = 0;
    for (size_t i = 0; i < num_codes; i++) {
        // Store matching values
        if ((codes1[i] > 9 && codes1[i] < 20) && (codes2[i] < 10))
            results[num_results++] = codes1[i];
    }
    // Data generation done.

    // Create a table
    Options options = Options();
    options.delete_exist_files = true;
    options.in_memory = true;
    auto *table = new BWTable("./abc", options);
    table->Open();


    // Add two columns into the table
    table->AddColumn("first", kBitWeavingH, bit_width);
    table->AddColumn("second", kBitWeavingV, bit_width);

    Column *column1, *column2;
    column1 = table->GetColumn("first");
    column2 = table->GetColumn("second");
    assert(column1 != NULL);
    assert(column2 != NULL);

    // Insert values into both columns
    column1->Append(codes1, num_codes);
    column2->Append(codes2, num_codes);

    // Create bitvector
    BitVector *bitvector = table->CreateBitVector();

    // Perform the scan with the predicate (9 < first < 20 && second < 10)
    column1->Scan(kGreater, 9, *bitvector);
    column1->Scan(kLess, 20, *bitvector, kAnd);
    column2->Scan(kLess, 10, *bitvector, kAnd);

    // Create an iterator to get matching values
    Iterator *iter = table->CreateIterator(*bitvector);

    Code code;
    size_t i = 0;
    while (iter->Next()) {
        // Get the next matching value
        iter->GetCode(*column1, code);
        if (code != results[i])
            std::cout << "Wrong Result: " << code << " " << results[i] << std::endl;
        i++;
    }
    if (num_results != i)
        std::cout << "Number of results does not match." << std::endl;

    if (table != NULLPTR)
        table->Close();

    delete iter;
    delete bitvector;
    delete table;

    std::cout << "Pass the test." << std::endl;

}

TEST(BitweavingTest, bwCompareWithFilterTest) {

    arrow::Status status;

    std::shared_ptr<arrow::Array> testArray = nullptr;
    //std::shared_ptr<arrow::Field> testInt32Field = arrow::field("int_val", arrow::int32());
    arrow::UInt64Builder testArrayBuilder;
    std::vector<uint64_t> values = {10, 5, 4, 8, 20, 30};
    status = testArrayBuilder.AppendValues(values);
    status = testArrayBuilder.Finish(&testArray);

    int num_codes = testArray->length();
    uint8_t bit_width = 32;

    // Create a table
    Options options = Options();
    options.delete_exist_files = true;
    options.in_memory = true;
    auto *table = new BWTable("./abc", options);
    //part_table = new Table(options);
    table->Open();

    // Add columns into the table
    table->AddColumn("first", kBitWeavingV, bit_width);

    Column *column1;
    column1 = table->GetColumn("first");
    assert(column1 != NULL);

    // Insert values into columns
    auto *codes1 = new Code[num_codes];
    //Code mask = (1ULL << bit_width) - 1;
    for (size_t i = 0; i < values.size(); i++) {
        codes1[i] = values[values.size() - 1 - i];
        //codes1[i] = input[i] & mask;
        std::cout << "Codes[" << i << "] " << codes1[i] << std::endl;
    }

    column1->Append(codes1, num_codes);

    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    auto *resultBitVector = new arrow::compute::Datum();
    arrow::compute::Datum value((uint64_t) 5);
    std::shared_ptr<bitweaving::Code> scalar = std::make_shared<bitweaving::Code>
        (((arrow::UInt64Scalar *) value.scalar().get())->value);

    status = BitweavingCompare(
        &function_context,
        table,
        column1,
        scalar,
        std::make_shared<Comparator>(kGreaterEqual),
        std::make_shared<BitVectorOpt>(kSet),
        resultBitVector
    );

    std::shared_ptr<arrow::Array> array = resultBitVector->make_array();
    std::cout << array->ToString() << std::endl;

    std::shared_ptr<arrow::Array> col_1 = nullptr;
    std::shared_ptr<arrow::Field> col_1_field = arrow::field("string_id_alt", arrow::utf8());
    arrow::StringBuilder col_1_builder;
    status = col_1_builder.AppendValues({"str_A", "str_B", "str_C", "str_D", "str_E", "str_F"});
    status = col_1_builder.Finish(&col_1);

    auto *filter_result = new arrow::compute::Datum();
    arrow::compute::FilterOptions filter_options;
    status = arrow::compute::Filter(&function_context,
                                    arrow::compute::Datum(col_1),
                                    *resultBitVector,
                                    filter_options,
                                    filter_result);
    array = filter_result->make_array();
    std::cout << array->ToString() << std::endl;

    delete table;
}

TEST(BitweavingTest, bwCompareWithInputLargerThanAWord) {

    arrow::Status status;

    int num_codes = 100;
    uint8_t bit_width = 4;

    // Create a table
    Options options = Options();
    options.delete_exist_files = true;
    options.in_memory = true;
    auto *table = new BWTable("./abc", options);
    table->Open();

    // Add columns into the table
    table->AddColumn("first", kBitWeavingV, bit_width);

    Column *column1;

    // Insert values into columns
    auto *codes1 = new Code[num_codes];
    //Code mask = (1ULL << bit_width) - 1;
    for (int i = num_codes - 1; i >= 0; i--) {
        codes1[i] = i;
        //std::cout << "Codes[" << (num_codes - 1 - i) << "] " << codes1[(num_codes - 1 - i)] << std::endl;
        std::cout << "Codes[" << i << "] " << codes1[i] << std::endl;

    }

    table->AppendToColumn("first", codes1, num_codes);
    column1 = table->GetColumn("first");
    assert(column1 != NULL);

    //column1->Append(codes1, num_codes);

    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    auto *resultBitVector = new arrow::compute::Datum();
    arrow::compute::Datum value((uint64_t) 56);
    std::shared_ptr<bitweaving::Code> scalar = std::make_shared<bitweaving::Code>
        (((arrow::UInt64Scalar *) value.scalar().get())->value);
    status = BitweavingCompare(
        &function_context,
        table,
        column1,
        scalar,
        std::make_shared<Comparator>(kGreaterEqual),
        std::make_shared<BitVectorOpt>(kSet),
        resultBitVector
    );
    if (!status.ok()) {
        std::cout << "Invalid status " << std::endl;
    }
//            std::shared_ptr<arrow::Array> array = resultBitVector->make_array();
//            std::cout << array->ToString() << std::endl;

    std::shared_ptr<arrow::Array> col_1 = nullptr;
    arrow::UInt64Builder col_1_builder;
    for (int i = 0; i < num_codes; i++) {
        status = col_1_builder.Append(i);
    }
    status = col_1_builder.Finish(&col_1);

    auto *filter_result = new arrow::compute::Datum();
    arrow::compute::FilterOptions filter_options;
    ASSERT_OK(arrow::compute::Filter(&function_context,
                                     arrow::compute::Datum(col_1),
                                     *resultBitVector,
                                     filter_options,
                                     filter_result));
    auto uint64_array = std::static_pointer_cast<arrow::UInt64Array>(filter_result->make_array());
    const uint64_t *data = uint64_array->raw_values();
    assert(uint64_array->length() == 44);
    for (int i = 0; i < uint64_array->length(); i++) {
        if (data[i] != (uint64_t) 56 + i) {
            GTEST_FAIL();
        }
        std::cout << data[i] << std::endl;
    }
    delete table;
}

TEST(BitweavingTest, ArrowComparePerfTest) {
    std::vector<std::string> schema = {"discount"};

    std::shared_ptr<Table> single_col_table =
        read_from_file("/Users/sandhyakannan/Masters/RA/Project/Arrow-Bitweaving/cpp/data/large_single_column.hsl");

//            std::shared_ptr<Table> single_col_table = build_table("./data/test.csv",
//                                                                  arrow::default_memory_pool(), schema);
//
//            write_to_file("./data/single_column.hsl", single_col_table);

    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    arrow::Status status;
    std::shared_ptr<Table> out_table;
    ColumnReference discount_ref = {single_col_table, "discount"};

    auto start = std::chrono::high_resolution_clock::now();

    auto select_pred = Predicate{
        {single_col_table,
         "discount"},
        arrow::compute::CompareOperator::GREATER_EQUAL,
        arrow::compute::Datum((int64_t) 8)
    };
    auto select_pred_node =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(select_pred));

    auto select_pred_tree = std::make_shared<PredicateTree>(select_pred_node);

    auto select_result = std::make_shared<OperatorResult>();
    select_result->append(single_col_table);

    auto result = std::make_shared<OperatorResult>();

    Select select_op(0, select_result, result, select_pred_tree);

    Scheduler &scheduler = Scheduler::GlobalInstance();
    scheduler.addTask(select_op.createTask());

    typedef unsigned long long cycle;
    cycle sum = 0;
    int num_iterations = 1;
    for (int i = 0; i < num_iterations; i++) {
        cycle c;
        startTimer(&c);

        scheduler.start();
        scheduler.join();

        stopTimer(&c);
        sum += c;
    }

    std::cout << "Avg cycles per value: " << double(sum) / num_iterations / single_col_table->get_num_rows()
              << std::endl;

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Arrow compute compare predicate execution time = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                  (end - start).count() << "ms" << std::endl;

    out_table = result->materialize({discount_ref});

    std::cout << "Total selected rows " << out_table->get_column(0)->length() << std::endl;

}

TEST(BitweavingTest, BitweavingPerfTest) {
    std::vector<std::string> schema = {"discount"};

    std::shared_ptr<Table> single_col_table =
        read_from_file("/Users/sandhyakannan/Masters/RA/Project/Arrow-Bitweaving/cpp/data/large_single_column.hsl");

    Options options;
    options = Options();
    options.delete_exist_files = true;
    options.in_memory = true;

    auto *bw_single_col_table = createBitweavingIndex(single_col_table,
        {BitweavingColumnIndexUnit("discount", 4)}, false);

    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());

    arrow::Status status;
    //Predicate: discount >= 2
    Column *column = bw_single_col_table->GetColumn("discount"); //year
    std::shared_ptr<Code> min_discount = std::make_shared<uint64_t>(8);
    std::shared_ptr<Comparator> op = std::make_shared<Comparator>(
        kGreaterEqual);
    std::shared_ptr<BitVectorOpt> opt = std::make_shared<BitVectorOpt>(
        kSet);
    BitweavingCompareOptionsUnit optUnit(min_discount, op, opt);
    BitweavingCompareOptions option(column);
    option.addOpt(optUnit);
    auto *bw_resultBitVector = new arrow::compute::Datum();

    auto start = std::chrono::high_resolution_clock::now();

    typedef unsigned long long cycle;
    cycle sum = 0;
    //cycle sum_builtin = 0;
    int num_iterations = 1;
    for (int i = 0; i < num_iterations; i++) {
        cycle c;
        startTimer(&c);
        //assert(__has_builtin(__builtin_readcyclecounter));
        //unsigned long long t0 = __builtin_readcyclecounter();
        status = BitweavingCompare(
            &function_context,
            bw_single_col_table,
            std::vector<BitweavingCompareOptions>{option},
            bw_resultBitVector
        );
        evaluate_status(status, __FUNCTION__, __LINE__);
        //unsigned long long t1 = __builtin_readcyclecounter();
        //sum_builtin += t1-t0;
        stopTimer(&c);
        sum += c;
    }
    //std::cout << "Cycles per value by builtin counter " << double(sum_builtin)/ 3 /single_col_table->num_rows() << std::endl;
    std::cout << "Avg cycles per value: " << double(sum) / num_iterations / single_col_table->get_num_rows()
              << std::endl;

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "bitweaving predicate execution time = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                  (end - start).count() << "ms" << std::endl;

    auto *filter_result = new arrow::compute::Datum();
    std::shared_ptr<arrow::ChunkedArray> select_col = single_col_table->get_column_by_name("discount");
    arrow::compute::FilterOptions filter_options;

    status = arrow::compute::Filter(&function_context,
                                    arrow::compute::Datum(select_col),
                                    *bw_resultBitVector,
                                    filter_options,
                                    filter_result);
    evaluate_status(status, __FUNCTION__, __LINE__);

    std::cout << "Total selected rows " << filter_result->chunked_array()->length() << std::endl;

    ASSERT_EQ(filter_result->chunked_array()->length(), 78524496);
}

TEST_F(BitweavingTestBase, SSBQ1_1Arrow) {

    std::shared_ptr<Table> out_table;
    ColumnReference discount_ref = {lineorder, "discount"};
    ColumnReference quantity_ref = {lineorder, "quantity"};

    auto t_start = std::chrono::high_resolution_clock::now();
    //discount >= 1
    auto discount_pred_1 = Predicate{
        {lineorder,
         "discount"},
        arrow::compute::CompareOperator::GREATER_EQUAL,
        arrow::compute::Datum((int64_t) 1)
    };
    auto discount_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(discount_pred_1));

    //discount <= 3
    auto discount_pred_2 = Predicate{
        {lineorder,
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
        {lineorder,
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

    auto lineorder_pred_tree = std::make_shared<PredicateTree>(lo_root_node);

    // date.year = 1993
    ColumnReference year_ref = {date, "year"};
    auto year_pred_1 = Predicate{
        {date,
         "year"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum((int64_t) 1993)
    };
    auto year_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(year_pred_1));
    auto date_pred_tree = std::make_shared<PredicateTree>(year_pred_node_1);

    auto lo_select_result = std::make_shared<OperatorResult>();
    lo_select_result->append(lineorder);

    auto date_select_result = std::make_shared<OperatorResult>();
    date_select_result->append(date);

    auto select_result_out = std::make_shared<OperatorResult>();

    Select lo_select_op(0, lo_select_result, select_result_out, lineorder_pred_tree);
    Select date_select_op(0, date_select_result, select_result_out, date_pred_tree);

    auto t1 = std::chrono::high_resolution_clock::now();

    // Join date and lineorder tables
    ColumnReference lo_d_ref = {lineorder, "order date"};
    ColumnReference d_ref = {date, "date key"};
    ColumnReference revenue_ref = {lineorder, "revenue"};

    auto join_result = std::make_shared<OperatorResult>();
    join_result->append(lo_select_result);
    join_result->append(date_select_result);

    JoinPredicate join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
    JoinGraph graph({{join_pred}});
    Join join_op(0, select_result_out, join_result, graph);

    auto agg_result = std::make_shared<OperatorResult>();
    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", {lineorder, "revenue"}};
    Aggregate agg_op(0, join_result, agg_result, {agg_ref}, {}, {});

    Scheduler &scheduler = Scheduler::GlobalInstance();

    ExecutionPlan plan(0);
    auto lo_select_id = plan.addOperator(&lo_select_op);
    auto d_select_id = plan.addOperator(&date_select_op);
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

    out_table = agg_result->materialize({{nullptr, "revenue"}});
    out_table->print();
    hustle::simple_profiler.summarizeToStream(std::cout);
    //ASSERT_EQ(out_table->num_rows(), 118598);

    auto t4 = std::chrono::high_resolution_clock::now();
    std::cout << "Query execution time = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                  (t4 - t_start).count() << std::endl;
}

TEST_F(BitweavingTestBase, SSBQ1_2Arrow) {

    std::shared_ptr<Table> out_table;
    ColumnReference discount_ref = {lineorder, "discount"};
    ColumnReference quantity_ref = {lineorder, "quantity"};

    auto t_start = std::chrono::high_resolution_clock::now();
    //discount >= 4
    auto discount_pred_1 = Predicate{
        {lineorder,
         "discount"},
        arrow::compute::CompareOperator::GREATER_EQUAL,
        arrow::compute::Datum((int64_t) 4)
    };
    auto discount_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(discount_pred_1));

    //discount <= 6
    auto discount_pred_2 = Predicate{
        {lineorder,
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
        {lineorder,
         "quantity"},
        arrow::compute::CompareOperator::GREATER_EQUAL,
        arrow::compute::Datum((int64_t) 26)
    };
    auto quantity_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(quantity_pred_1));

    //quantity <= 35
    auto quantity_pred_2 = Predicate{
        {lineorder,
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

    auto lineorder_pred_tree = std::make_shared<PredicateTree>(lo_root_node);

    // date.year month num = 199401
    ColumnReference year_ref = {date, "year month num"};
    auto year_pred_1 = Predicate{
        {date,
         "year month num"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum((int64_t) 199401)
    };
    auto year_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(year_pred_1));
    auto date_pred_tree = std::make_shared<PredicateTree>(year_pred_node_1);

    auto lo_select_result = std::make_shared<OperatorResult>();
    lo_select_result->append(lineorder);

    auto date_select_result = std::make_shared<OperatorResult>();
    date_select_result->append(date);

    auto select_result_out = std::make_shared<OperatorResult>();

    Select lo_select_op(0, lo_select_result, select_result_out, lineorder_pred_tree);
    Select date_select_op(0, date_select_result, select_result_out, date_pred_tree);

    // Join date and lineorder tables
    ColumnReference lo_d_ref = {lineorder, "order date"};
    ColumnReference d_ref = {date, "date key"};
    ColumnReference revenue_ref = {lineorder, "revenue"};

    auto join_result = std::make_shared<OperatorResult>();
    join_result->append(lo_select_result);
    join_result->append(date_select_result);

    JoinPredicate join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
    JoinGraph graph({{join_pred}});
    Join join_op(0, select_result_out, join_result, graph);

    auto agg_result = std::make_shared<OperatorResult>();
    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", {lineorder, "revenue"}};
    Aggregate agg_op(0, join_result, agg_result, {agg_ref}, {}, {});

    Scheduler &scheduler = Scheduler::GlobalInstance();

    ExecutionPlan plan(0);
    auto lo_select_id = plan.addOperator(&lo_select_op);
    auto d_select_id = plan.addOperator(&date_select_op);
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

    out_table = agg_result->materialize({{nullptr, "revenue"}});
    out_table->print();
    hustle::simple_profiler.summarizeToStream(std::cout);

    auto t4 = std::chrono::high_resolution_clock::now();
    std::cout << "Query execution time = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                  (t4 - t_start).count() << std::endl;
}

TEST_F(BitweavingTestBase, SSBQ1_3Arrow) {

    std::shared_ptr<Table> out_table;
    ColumnReference discount_ref = {lineorder, "discount"};
    ColumnReference quantity_ref = {lineorder, "quantity"};

    auto t_start = std::chrono::high_resolution_clock::now();
    //discount >= 5
    auto discount_pred_1 = Predicate{
        {lineorder,
         "discount"},
        arrow::compute::CompareOperator::GREATER_EQUAL,
        arrow::compute::Datum((int64_t) 5)
    };
    auto discount_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(discount_pred_1));

    //discount <= 7
    auto discount_pred_2 = Predicate{
        {lineorder,
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
        {lineorder,
         "quantity"},
        arrow::compute::CompareOperator::GREATER_EQUAL,
        arrow::compute::Datum((int64_t) 26)
    };
    auto quantity_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(quantity_pred_1));

    //quantity <= 35
    auto quantity_pred_2 = Predicate{
        {lineorder,
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

    auto lineorder_pred_tree = std::make_shared<PredicateTree>(lo_root_node);

    // date.year = 1994
    auto year_pred_1 = Predicate{
        {date,
         "year"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum((int64_t) 1994)
    };
    auto year_pred_node_1 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(year_pred_1));

    // date.week num in year = 6
    auto year_pred_2 = Predicate{
        {date,
         "week num in year"},
        arrow::compute::CompareOperator::EQUAL,
        arrow::compute::Datum((int64_t) 6)
    };
    auto year_pred_node_2 =
        std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(year_pred_2));

    auto date_root_node = std::make_shared<ConnectiveNode>(
        year_pred_node_1,
        year_pred_node_2,
        FilterOperator::AND
    );

    auto date_pred_tree = std::make_shared<PredicateTree>(date_root_node);

    auto lo_select_result = std::make_shared<OperatorResult>();
    lo_select_result->append(lineorder);

    auto date_select_result = std::make_shared<OperatorResult>();
    date_select_result->append(date);

    auto select_result_out = std::make_shared<OperatorResult>();

    Select lo_select_op(0, lo_select_result, select_result_out, lineorder_pred_tree);
    Select date_select_op(0, date_select_result, select_result_out, date_pred_tree);

    // Join date and lineorder tables
    ColumnReference lo_d_ref = {lineorder, "order date"};
    ColumnReference d_ref = {date, "date key"};
    ColumnReference revenue_ref = {lineorder, "revenue"};

    auto join_result = std::make_shared<OperatorResult>();
    join_result->append(lo_select_result);
    join_result->append(date_select_result);

    JoinPredicate join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
    JoinGraph graph({{join_pred}});
    Join join_op(0, select_result_out, join_result, graph);

    auto agg_result = std::make_shared<OperatorResult>();
    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", {lineorder, "revenue"}};
    Aggregate agg_op(0, join_result, agg_result, {agg_ref}, {}, {});

    Scheduler &scheduler = Scheduler::GlobalInstance();

    ExecutionPlan plan(0);
    auto lo_select_id = plan.addOperator(&lo_select_op);
    auto d_select_id = plan.addOperator(&date_select_op);
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

    out_table = agg_result->materialize({{nullptr, "revenue"}});
    out_table->print();
    hustle::simple_profiler.summarizeToStream(std::cout);

    auto t4 = std::chrono::high_resolution_clock::now();
    std::cout << "Query execution time = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                  (t4 - t_start).count() << std::endl;
}

/*TEST_F(BitweavingTestBase, SSBQ1_1) {
    arrow::Status status;

    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    auto *lo_resultBitVector = new arrow::compute::Datum();
    std::vector<BitweavingCompareOptions> options;

    // Predicate : 1 <= lo_discount <= 3
    Column *discount_column = lineorder_table->GetColumn("discount");
    BitweavingCompareOptions discount_options(discount_column);
    BitweavingCompareOptionsUnit min_discount_opt(std::make_shared<uint64_t>(1),
            std::make_shared<Comparator>(kGreaterEqual),
                    std::make_shared<BitVectorOpt>(kSet));
    BitweavingCompareOptionsUnit max_discount_opt(std::make_shared<uint64_t>(3),
            std::make_shared<Comparator>(kLessEqual),
                    std::make_shared<BitVectorOpt>(kAnd));
    discount_options.addOpt(min_discount_opt);
    discount_options.addOpt(max_discount_opt);

    options.push_back(discount_options);

    // Predicate : lo_quantity < 25
    Column *quantity_column = lineorder_table->GetColumn("quantity"); //QUANTITY
    BitweavingCompareOptions quantity_options(quantity_column);
    quantity_options.addOpt(*(new BitweavingCompareOptionsUnit(std::make_shared<uint64_t>(25),
            std::make_shared<Comparator>(kLess),
                    std::make_shared<BitVectorOpt>(kAnd))));

    options.push_back(quantity_options);

    auto t_start = std::chrono::high_resolution_clock::now();
    status = BitweavingCompare(
            &function_context,
            lineorder_table,
            options,
            lo_resultBitVector
    );
    evaluate_status(status, __FUNCTION__, __LINE__);


    //Predicate: d_year = 1993
    Column *d_column = date_table->GetColumn("year"); //year
    std::shared_ptr<Code> year = std::make_shared<uint64_t>(1993);
    std::shared_ptr<Comparator> d_op = std::make_shared<Comparator>(kEqual);
    std::shared_ptr<BitVectorOpt> d_opt = std::make_shared<BitVectorOpt>(
            kSet);
    BitweavingCompareOptionsUnit year_optUnit(year, d_op, d_opt);
    BitweavingCompareOptions date_options(d_column);
    date_options.addOpt(year_optUnit);
    auto *d_resultBitVector = new arrow::compute::Datum();

    status = BitweavingCompare(
            &function_context,
            date_table,
            std::vector<BitweavingCompareOptions> {date_options},
            d_resultBitVector
    );
    evaluate_status(status, __FUNCTION__, __LINE__);

    auto t2 = std::chrono::high_resolution_clock::now();
    std::cout << "predicate execution time = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t2 - t_start).count() << std::endl;

    // Join date and lineorder tables
    ColumnReference lo_d_ref = {lineorder, "order date"};
    ColumnReference d_ref = {date, "date key"};
    ColumnReference revenue_ref = {lineorder, "revenue"};

    auto join_result = std::make_shared<OperatorResult>();
    join_result->append(lineorder, *lo_resultBitVector);
    join_result->append(date, *d_resultBitVector);

    JoinPredicate join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
    JoinGraph graph({{join_pred}});
    Join join_op(0, select_result_out, join_result, graph);

    auto agg_result = std::make_shared<OperatorResult>();
    AggregateReference agg_ref = {AggregateKernels::SUM, "revenue", {lineorder, "revenue"}};
    Aggregate agg_op(0, join_result, agg_result, {agg_ref}, {}, {});

    Scheduler &scheduler = Scheduler::GlobalInstance();

    ExecutionPlan plan(0);
    auto lo_select_id = plan.addOperator(&lo_select_op);
    auto d_select_id = plan.addOperator(&date_select_op);
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

    out_table = agg_result->materialize({{nullptr, "revenue"}});
    out_table->print();
    hustle::simple_profiler.summarizeToStream(std::cout);

    auto t_end = std::chrono::high_resolution_clock::now();

    std::cout << "query execution time = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t_end - t_start).count() << std::endl;

}

 TEST_F(BitweavingTestBase, SSBQ1_2) {
    arrow::Status status;

    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    auto *lo_resultBitVector = new arrow::compute::Datum();

    std::vector<BitweavingCompareOptions> compare_options;

    // Predicate : 4 <= lo_discount <= 6
    Column *discount_column = lineorder_table->GetColumn("discount");
    BitweavingCompareOptions discount_options(discount_column);
    BitweavingCompareOptionsUnit min_discount_opt(std::make_shared<uint64_t>(4),
            std::make_shared<Comparator>(kGreaterEqual),
            std::make_shared<BitVectorOpt>(kSet));
    BitweavingCompareOptionsUnit max_discount_opt(std::make_shared<uint64_t>(6),
            std::make_shared<Comparator>(kLessEqual),
            std::make_shared<BitVectorOpt>(kAnd));
    discount_options.addOpt(min_discount_opt);
    discount_options.addOpt(max_discount_opt);

    compare_options.push_back(discount_options);

    // Predicate : 26 <= lo_quantity <= 35
    Column *quantity_column = lineorder_table->GetColumn("quantity"); //QUANTITY
    BitweavingCompareOptions quantity_options(quantity_column);
    quantity_options.addOpt(BitweavingCompareOptionsUnit(std::make_shared<uint64_t>(26),
            std::make_shared<Comparator>(kGreaterEqual),
            std::make_shared<BitVectorOpt>(kAnd)));
    quantity_options.addOpt( BitweavingCompareOptionsUnit(std::make_shared<uint64_t>(35),
            std::make_shared<Comparator>(kLessEqual),
            std::make_shared<BitVectorOpt>(kAnd)));

    compare_options.push_back(quantity_options);

    auto t_start = std::chrono::high_resolution_clock::now();
    status = BitweavingCompare(
            &function_context,
            lineorder_table,
            compare_options,
            lo_resultBitVector
    );
    evaluate_status(status, __FUNCTION__, __LINE__);


    //Predicate: d_yearmonthnum = 199401
    Column *d_column = date_table->GetColumn("year month num");
    std::shared_ptr<Code> year_month_num = std::make_shared<uint64_t>(199401);
    std::shared_ptr<Comparator> d_op = std::make_shared<Comparator>(kEqual);
    std::shared_ptr<BitVectorOpt> d_opt = std::make_shared<BitVectorOpt>(
            kSet);
    BitweavingCompareOptions date_options(d_column);
    date_options.addOpt(BitweavingCompareOptionsUnit(year_month_num, d_op, d_opt));
    auto *d_resultBitVector = new arrow::compute::Datum();

    status = BitweavingCompare(
            &function_context,
            date_table,
            std::vector<BitweavingCompareOptions> {date_options},
            d_resultBitVector
    );
    evaluate_status(status, __FUNCTION__, __LINE__);

    auto t2 = std::chrono::high_resolution_clock::now();
    std::cout << "predicate execution time = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t2 - t_start).count() << std::endl;

    // Join date and lineorder tables
    ColumnReference lo_d_ref = {lineorder, "order date"};
    ColumnReference d_ref = {date, "date key"};
    ColumnReference revenue_ref = {lineorder, "revenue"};

    auto join_result = std::make_shared<OperatorResult>();
    join_result->append(lineorder, *lo_resultBitVector);
    join_result->append(date, *d_resultBitVector);

    JoinPredicate join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
    JoinGraph graph({{join_pred}});
    Join join_op(join_result, graph);

    join_result = join_op.run();

    auto out_table = join_result->materialize({lo_d_ref, d_ref, revenue_ref});

    std::cout << "Total selected rows " << out_table->num_rows() << std::endl;

    //ASSERT_EQ(out_table->num_rows(), 4301);

    auto t3 = std::chrono::high_resolution_clock::now();
    std::cout << "Join execution time = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t3 - t2).count() << std::endl;

    std::shared_ptr<arrow::ChunkedArray> revenue = out_table->GetColumnByName("revenue"); //REVENUE
    arrow::compute::Datum sum_result;
    status = arrow::compute::Sum(
            &function_context,
            revenue,
            &sum_result
    );
    evaluate_status(status, __FUNCTION__, __LINE__);

    //std::cout << "Sum(revenue) is " << sum_result. <<std::endl;

    auto t_end = std::chrono::high_resolution_clock::now();

    std::cout << "query execution time = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t_end - t_start).count() << std::endl;

}


TEST_F(BitweavingTestBase, SSBQ1_3) {
    arrow::Status status;

    arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
    auto *lo_resultBitVector = new arrow::compute::Datum();

    std::vector<BitweavingCompareOptions> compare_options;

    // Predicate : 5 <= lo_discount <= 7
    Column *discount_column = lineorder_table->GetColumn("discount");
    BitweavingCompareOptions discount_options(discount_column);
    BitweavingCompareOptionsUnit min_discount_opt(std::make_shared<uint64_t>(5),
            std::make_shared<Comparator>(kGreaterEqual),
            std::make_shared<BitVectorOpt>(kSet));
    BitweavingCompareOptionsUnit max_discount_opt(std::make_shared<uint64_t>(7),
            std::make_shared<Comparator>(kLessEqual),
            std::make_shared<BitVectorOpt>(kAnd));
    discount_options.addOpt(min_discount_opt);
    discount_options.addOpt(max_discount_opt);

    compare_options.push_back(discount_options);

    // Predicate : 26 <= lo_quantity <= 35
    Column *quantity_column = lineorder_table->GetColumn("quantity"); //QUANTITY
    BitweavingCompareOptions quantity_options(quantity_column);
    quantity_options.addOpt(BitweavingCompareOptionsUnit(std::make_shared<uint64_t>(26),
            std::make_shared<Comparator>(kGreaterEqual),
            std::make_shared<BitVectorOpt>(kAnd)));
    quantity_options.addOpt( BitweavingCompareOptionsUnit(std::make_shared<uint64_t>(35),
            std::make_shared<Comparator>(kLessEqual),
            std::make_shared<BitVectorOpt>(kAnd)));

    compare_options.push_back(quantity_options);

    auto t_start = std::chrono::high_resolution_clock::now();
    status = BitweavingCompare(
            &function_context,
            lineorder_table,
            compare_options,
            lo_resultBitVector
    );
    evaluate_status(status, __FUNCTION__, __LINE__);

    // Predicate d_year = 1994
    Column *d_year = date_table->GetColumn("year");
    BitweavingCompareOptions year_options(d_year);
    year_options.addOpt(BitweavingCompareOptionsUnit(std::make_shared<uint64_t>(1994),
            std::make_shared<Comparator>(kEqual),
            std::make_shared<BitVectorOpt>(
                    kSet)));

    //Predicate:  d_weeknuminyear = 6
    Column *d_weeknuminyear = date_table->GetColumn("week num in year");
    BitweavingCompareOptions week_options(d_weeknuminyear);
    week_options.addOpt(BitweavingCompareOptionsUnit(std::make_shared<uint64_t>(6),
            std::make_shared<Comparator>(kEqual),
            std::make_shared<BitVectorOpt>(
                    kAnd)));
    auto *d_resultBitVector = new arrow::compute::Datum();

    status = BitweavingCompare(
            &function_context,
            date_table,
            std::vector<BitweavingCompareOptions> {year_options, week_options},
            d_resultBitVector
    );
    evaluate_status(status, __FUNCTION__, __LINE__);

    auto t2 = std::chrono::high_resolution_clock::now();
    std::cout << "predicate execution time = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t2 - t_start).count() << std::endl;

    // Join date and lineorder tables
    ColumnReference lo_d_ref = {lineorder, "order date"};
    ColumnReference d_ref = {date, "date key"};
    ColumnReference revenue_ref = {lineorder, "revenue"};

    auto join_result = std::make_shared<OperatorResult>();
    join_result->append(lineorder, *lo_resultBitVector);
    join_result->append(date, *d_resultBitVector);

    JoinPredicate join_pred = {lo_d_ref, arrow::compute::EQUAL, d_ref};
    JoinGraph graph({{join_pred}});
    Join join_op(join_result, graph);

    join_result = join_op.run();

    auto out_table = join_result->materialize({lo_d_ref, d_ref, revenue_ref});

    std::cout << "Total selected rows " << out_table->num_rows() << std::endl;

    //ASSERT_EQ(out_table->num_rows(), 955);

    auto t3 = std::chrono::high_resolution_clock::now();
    std::cout << "Join execution time = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t3 - t2).count() << std::endl;

    std::shared_ptr<arrow::ChunkedArray> revenue = out_table->GetColumnByName("revenue"); //REVENUE
    arrow::compute::Datum sum_result;
    status = arrow::compute::Sum(
            &function_context,
            revenue,
            &sum_result
    );
    evaluate_status(status, __FUNCTION__, __LINE__);

    //std::cout << "Sum(revenue) is " << sum_result. <<std::endl;

    auto t_end = std::chrono::high_resolution_clock::now();

    std::cout << "query execution time = " <<
              std::chrono::duration_cast<std::chrono::milliseconds>
                      (t_end - t_start).count() << std::endl;

}*/
}
