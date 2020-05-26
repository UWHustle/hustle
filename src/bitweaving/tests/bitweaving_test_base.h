//
// Created by Sandhya Kannan on 3/18/20.
//

#ifndef HUSTLE_BITWEAVINGTESTBASE_H
#define HUSTLE_BITWEAVINGTESTBASE_H

#include <gtest/gtest.h>
#include <bitweaving/table.h>
#include <table/table.h>

namespace hustle::bitweaving {
        class BitweavingTestBase : public ::testing::Test
        {
        public:
            /**
             * The constructor of the BitweavingTestBase
             */
            BitweavingTestBase();

            /**
             * The destructor of the BitweavingTestBase to free any dynamically
             * allocated objects
             */
            virtual ~BitweavingTestBase();

            /**
             * Override the SetUpTestCase method of the base class
             * This method is executed once before any test is run.
             * Perfect place to load the tables and build the bitweaving indices
             */
            static void SetUpTestCase() ;

            /**
             * Override the TearDownTestCase method of the base class.
             * This method is executed once after all tests in the test class are run.
             * Could be used to cleanup the bitweaving index objects
             */
            static void TearDownTestCase();

            /**
             * Override the SetUp method of the base class.
             * This method is run once, before each test case
             */
            virtual void SetUp() override;

            /**
            * Override the TearDown method of the base class.
            * This method is run once, after the execution of each test case
            */
            virtual void TearDown() override;


        public:

            static BWTable *customer_table;
            static BWTable *date_table;
            static BWTable *lineorder_table;
            static BWTable *part_table;
            static BWTable *supplier_table;

            Options options;

            static std::shared_ptr<Table> customer;
            static std::shared_ptr<Table> date;
            static std::shared_ptr<Table> lineorder;
            static std::shared_ptr<Table> part;
            static std::shared_ptr<Table> supplier;
        };
}




#endif //HUSTLE_BITWEAVINGTESTBASE_H
