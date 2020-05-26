//
// Created by Sandhya Kannan on 3/10/20.
//

#ifndef HUSTLE_BITWEAVING_TEST_UTIL_H
#define HUSTLE_BITWEAVING_TEST_UTIL_H

#include <iostream>
#include <arrow/memory_pool.h>
#include <bitweaving/table.h>
#include <table/table.h>

namespace hustle::bitweaving {

        /**
        * If a status outcome is an error, print its error message and throw an
        * expection. Otherwise, do nothing.
         *
        * The function_name and line_no parameters allow us to find exactly where
        * the error occured, since the same error can be thrown from different places.
        * @param status Status outcome object
        * @param function_name name of the function that called evaluate_status
        * @param line_no Line number of the call to evaluate_status
        *
        * TODO: Do we want to throw an expection on error?
        */
        void evaluate_status(const arrow::Status &status, const char *function_name,
                             int line_no);

        /**
         * This method builds an Arrow table from the given csv file
         * @param file_path path of the csv/tbl file input
         * @param pool the memory pool to allocate space in
         * @param schema The schema/ names of columns to use for the table
         * @return  A shared pointer to the newly created table
         */
        std::shared_ptr<arrow::Table>
        build_table(const std::string &file_path, arrow::MemoryPool *pool, std::vector<std::string> &schema);

        /**
         * This method copies the columns from arrow table to the bit weaving table. The bitweaving table is actually empty before
         * this method call.
         * @param arrow_table - The arrow table with data
         * @param bw_table - The bitweaving table to which the data is to be copied to
         */
        void addColumnsToBitweavingTable(std::shared_ptr<Table> arrow_table, BWTable *bw_table);
}


#endif //HUSTLE_BITWEAVING_TEST_UTIL_H
