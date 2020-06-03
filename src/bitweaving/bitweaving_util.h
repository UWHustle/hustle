//
// Created by Sandhya Kannan on 3/10/20.
//

#ifndef HUSTLE_BITWEAVING_UTIL_H
#define HUSTLE_BITWEAVING_UTIL_H

#include <iostream>
#include <arrow/memory_pool.h>
#include <bitweaving/table.h>
#include <table/table.h>
#include <map>
#include <operators/LazyTable.h>
#include <arrow/util/memory.h>
#include <table/Index.h>

namespace hustle::bitweaving {

const int default_bit_width = 16;

struct BitweavingColumnIndexUnit {
  BitweavingColumnIndexUnit(std::string name) : col_name(std::move(name)), bit_width(default_bit_width) {}

  BitweavingColumnIndexUnit(std::string name, int bitwidth) : col_name(std::move(name)),
                                                              bit_width(bitwidth) {}

  std::string col_name;
  int bit_width;
};

/**
 * This method create and copies the columns from hustle table to the bit weaving table. T
 * @param hustle_table - The hustle table with data
 * @param cols - The names and suggested bit width of columns for which indices are to be created
 * @param auto_tune_bitwidth - bool that indicates if the optimum bit width tuning is turned on or off
 * @return BWTable* -  Returns the pointer to the Bitweaving index table created for the hustle table
 */
BWTable *createBitweavingIndex(const std::shared_ptr<Table> &hustle_table,
                               std::vector<BitweavingColumnIndexUnit> cols,
                               bool auto_tune_bitwidth = true);
}

#endif //HUSTLE_BITWEAVING_UTIL_H
