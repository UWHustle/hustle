//
// Created by Sandhya Kannan on 6/2/20.
//

#ifndef HUSTLE_SRC_BITWEAVING_BITWEAVING_INDEX_H_
#define HUSTLE_SRC_BITWEAVING_BITWEAVING_INDEX_H_

#include <table/Index.h>
#include "table.h"

namespace hustle::bitweaving {
  class BitweavingIndex : Index {
    arrow::compute::Datum scan(std::shared_ptr<Predicate>& p) override;

    void createIndex(std::shared_ptr<Table> table, std::vector<std::string> cols) override;

    bool isColumnIndexed(std::string col_name) override;

   private:
    BWTable* bwTable{};
  };
}

#endif //HUSTLE_SRC_BITWEAVING_BITWEAVING_INDEX_H_
