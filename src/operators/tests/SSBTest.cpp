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
#include <utility/EventProfiler.hpp>
#include <scheduler/SchedulerFlags.hpp>

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


    }
};

void dummy_function(Task *ctx) {

    int num_blocks = 10;


        ctx->spawnLambdaTask([]() {
            auto *container = simple_profiler.getContainer();
            container->startEvent("calc");
            int x = 0;
            for (int j = 0; j < 10000000; j++) {
                x += std::log(j);
            }
            container->endEvent("calc");
        });


}


TEST_F(SSBTestFixture, SSBQ41) {

    T = read_from_file
            ("/Users/corrado/hustle/src/table/tests/T.hsl", false);

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
    Select select_op10(0, result, pred_tree);
    Select select_op11(0, result, pred_tree);
    Select select_op12(0, result, pred_tree);
    Select select_op13(0, result, pred_tree);
    Select select_op14(0, result, pred_tree);
    Select select_op15(0, result, pred_tree);
    Select select_op16(0, result, pred_tree);
    Select select_op17(0, result, pred_tree);
    Select select_op18(0, result, pred_tree);
    Select select_op19(0, result, pred_tree);


    std::cout << std::endl;
    FLAGS_num_threads = 8;
    std::cout << "num threads = " << FLAGS_num_threads << std::endl;
    std::cout << "num blocks = " << T->get_num_blocks() << std::endl;

    Scheduler scheduler = Scheduler(FLAGS_num_threads);

//        Task *ctx = CreateLambdaTask([](Task *ctx) {
//            for (int j = 0; j < 100; j++) {
//                dummy_function(ctx);
//            }
//        });

//        scheduler.addTask(ctx);

    auto *container = simple_profiler.getContainer();
    container->startEvent("overall");


    scheduler.start();

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
    scheduler.addTask(select_op10.createTask());
    scheduler.addTask(select_op11.createTask());
    scheduler.addTask(select_op12.createTask());
    scheduler.addTask(select_op13.createTask());
    scheduler.addTask(select_op14.createTask());
    scheduler.addTask(select_op15.createTask());
    scheduler.addTask(select_op16.createTask());
    scheduler.addTask(select_op17.createTask());
    scheduler.addTask(select_op18.createTask());
    scheduler.addTask(select_op19.createTask());
    scheduler.join();

    container->endEvent("overall");
    simple_profiler.summarizeToStream(std::cout);

}


void calcSubLogSum() {
    auto *container = simple_profiler.getContainer();
    container->startEvent("calc");
    double sum = 0;
//  for (std::size_t i = range.begin(); i < range.end(); ++i) {
    for (int i=0; i< 1000000; i++) {
        sum += std::log(i);
    }
    container->endEvent("calc");
//  return sum;
}

static void calcLogSum(const std::uint64_t N, Scheduler *scheduler) {
//  std::shared_ptr<std::atomic<double>> sum =
//      std::make_shared<std::atomic<double>>(0);


        auto calc_task = [N](Task *ctx) {
            for (int i=0; i<100; i++) {
                ctx->spawnLambdaTask([] {
                    calcSubLogSum();
                });
            }
        };

    auto print_task = [](Task *ctx) {
        std::cout << "[" << (1 / 100000000) << "] -- "
                  << "okay" << std::endl;
    };

    scheduler->addTask(CreateTaskChain(CreateLambdaTask(calc_task),
                                       CreateLambdaTask(print_task)));
}

//int main(int argc, char *argv[]) {
////    google::InitGoogleLogging(argv[0]);
////    gflags::ParseCommandLineFlags(&argc, &argv, true);
//
//    std::cout << std::endl;
//    FLAGS_num_threads = 8;
//    std::cout << "num threads = " << FLAGS_num_threads << std::endl;
//
//    for (int i=0; i<1; i++) {
//
//        auto *container = simple_profiler.getContainer();
//        container->startEvent("overall");
//
//        Scheduler &scheduler = Scheduler::GlobalInstance();
//
//        scheduler.start();
//
//        for (int k = 0; k < 10; ++k) {
//            calcLogSum(10000000 * k, &scheduler);
//        }
//
//        scheduler.join();
//
//        container->endEvent("overall");
//
//        simple_profiler.summarizeToStream(std::cout);
//
//    }
//}