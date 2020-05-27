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

        auto field1 = arrow::field("order key", arrow::int64());
        auto field2 = arrow::field("line number", arrow::int64());
        auto field3 = arrow::field("cust key", arrow::int64());
        auto field4 = arrow::field("part key", arrow::int64());
        auto field5 = arrow::field("supp key", arrow::int64());
        auto field6 = arrow::field("order date", arrow::int64());
        auto field7 = arrow::field("ord priority", arrow::utf8());
        auto field8 = arrow::field("ship priority", arrow::int64());
        auto field9 = arrow::field("quantity", arrow::int64());
        auto field10 = arrow::field("extended price", arrow::int64());
        auto field11 = arrow::field("ord total price", arrow::int64());
        auto field12 = arrow::field("discount", arrow::int64());
        auto field13 = arrow::field("revenue", arrow::int64());
        auto field14 = arrow::field("supply cost", arrow::int64());
        auto field15 = arrow::field("tax", arrow::int64());
        auto field16 = arrow::field("commit date", arrow::int64());
        auto field17 = arrow::field("ship mode", arrow::utf8());
        lo_schema=arrow::schema({field1,field2,field3,field4,field5,
                                 field6,field7,field8,field9,field10,
                                 field11,field12,field13,field14,field15,
                                 field16,field17});


        std::shared_ptr<arrow::Field>s_field1=arrow::field("s supp key",
                                                           arrow::int64());
        std::shared_ptr<arrow::Field>s_field2=arrow::field("s name",
                                                           arrow::utf8());
        std::shared_ptr<arrow::Field>s_field3=arrow::field("s address",
                                                           arrow::utf8());
        std::shared_ptr<arrow::Field>s_field4=arrow::field("s city",
                                                           arrow::utf8());
        std::shared_ptr<arrow::Field>s_field5=arrow::field("s nation",
                                                           arrow::utf8());
        std::shared_ptr<arrow::Field>s_field6=arrow::field("s region",
                                                           arrow::utf8());
        std::shared_ptr<arrow::Field>s_field7=arrow::field("s phone",
                                                           arrow::utf8());

        s_schema=arrow::schema({s_field1,s_field2,s_field3,s_field4,
                                s_field5,
                                s_field6,s_field7});


        std::shared_ptr<arrow::Field>c_field1=arrow::field("c cust key",
                                                           arrow::int64());
        std::shared_ptr<arrow::Field>c_field2=arrow::field("c name",
                                                           arrow::utf8());
        std::shared_ptr<arrow::Field>c_field3=arrow::field("c address",
                                                           arrow::utf8());
        std::shared_ptr<arrow::Field>c_field4=arrow::field("c city",
                                                           arrow::utf8());
        std::shared_ptr<arrow::Field>c_field5=arrow::field("c nation",
                                                           arrow::utf8());
        std::shared_ptr<arrow::Field>c_field6=arrow::field("c region",
                                                           arrow::utf8());
        std::shared_ptr<arrow::Field>c_field7=arrow::field("c phone",
                                                           arrow::utf8());
        std::shared_ptr<arrow::Field>c_field8=arrow::field("c mkt segment",
                                                           arrow::utf8());

        c_schema=arrow::schema({c_field1,c_field2,c_field3,c_field4,
                                   c_field5,c_field6,c_field7,c_field8});


        auto t = read_from_csv_file("/Users/corrado/hustle/data/ssb-1/customer.tbl", c_schema, BLOCK_SIZE);
        write_to_file("/Users/corrado/hustle/data/ssb-1/customer.hsl", *t);
        t = read_from_csv_file("/Users/corrado/hustle/data/ssb-1/supplier.tbl", s_schema, BLOCK_SIZE);
        write_to_file("/Users/corrado/hustle/data/ssb-1/supplier.hsl", *t);

//        lo = read_from_file("/Users/corrado/hustle/data/ssb-1/lineorder.hsl");
//        d  = read_from_file("/Users/corrado/hustle/data/ssb-1/date.hsl");
//        p  = read_from_file("/Users/corrado/hustle/data/ssb-1/part.hsl");
//        c  = read_from_file("/Users/corrado/hustle/data/ssb-1/customer.hsl");
//        s  = read_from_file("/Users/corrado/hustle/data/ssb-1/supplier.hsl");

    }
};