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

    std::shared_ptr<Table> T;
    std::shared_ptr<arrow::Schema> T_schema;

    void SetUp() override {

        std::shared_ptr<arrow::Field> T_field = arrow::field("col",
                                                              arrow::int64());

        T_schema = arrow::schema({T_field});

        T = read_from_file
                ("/Users/corrado/hustle/src/table/tests/T.hsl");
    }
};


TEST_F(SSBTestFixture, SSBQ41) {

    auto pred_1 = Predicate{
            {T, "col"},
            arrow::compute::CompareOperator::LESS_EQUAL,
            arrow::compute::Datum((int64_t) 50000)
    };

    auto pred_node_1 =
            std::make_shared<PredicateNode>(
                    std::make_shared<Predicate>(pred_1));

    auto pred_tree = std::make_shared<PredicateTree>
            (pred_node_1);

    auto result = std::make_shared<OperatorResult>();
    result->append(T);

    Select select_op0(0, result, pred_tree);
    Select select_op1(0, result, pred_tree);
    Select select_op2(0, result, pred_tree);
    Select select_op3(0, result, pred_tree);
    Select select_op4(0, result, pred_tree);
    Select select_op5(0, result, pred_tree);
    Select select_op6(0, result, pred_tree);
    Select select_op7(0, result, pred_tree);
    Select select_op8(0, result, pred_tree);
    Select select_op9(0, result, pred_tree);

    for (int num_workers=1; num_workers<= 8; num_workers++) {

        Scheduler scheduler = Scheduler(num_workers);
        scheduler.addTask(select_op0.createTask());
        scheduler.addTask(select_op1.createTask());
        scheduler.addTask(select_op2.createTask());
        scheduler.addTask(select_op3.createTask());
        scheduler.addTask(select_op4.createTask());
        scheduler.addTask(select_op5.createTask());
        scheduler.addTask(select_op6.createTask());
        scheduler.addTask(select_op7.createTask());
        scheduler.addTask(select_op8.createTask());
        scheduler.addTask(select_op9.createTask());

        auto t1 = std::chrono::high_resolution_clock::now();

        scheduler.start();
        scheduler.join();

        auto t2 = std::chrono::high_resolution_clock::now();

        auto delta = std::chrono::duration_cast<std::chrono::milliseconds>(t2-t1);
        std::cout << "NUM WORKERS = " << num_workers << std::endl;
        std::cout << "EXECUTION TIME = " << delta.count() << " ms" <<
                  std::endl;
    }


}