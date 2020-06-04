//
// Created by Sandhya Kannan on 6/2/20.
//

#ifndef HUSTLE_SRC_BITWEAVING_BITWEAVING_INDEX_H_
#define HUSTLE_SRC_BITWEAVING_BITWEAVING_INDEX_H_

#include <table/Index.h>
#include "table.h"
#include "bitweaving_compare.h"

namespace hustle::bitweaving {
  class BitweavingIndex : public Index {

    arrow::compute::Datum scan(std::shared_ptr<Predicate>& p) override;

    arrow::compute::Datum scan(std::shared_ptr<PredicateTree> &root) override;

    void createIndex(std::shared_ptr<Table> table, std::vector<std::string> cols) override;

    bool isColumnIndexed(std::string col_name) override;

   public:
    explicit BitweavingIndex(IndexType type);

    ~BitweavingIndex() override;

   private:
    BWTable* bwTable{};

    void addPredicate(std::shared_ptr<Predicate>& p, FilterOperator op, std::vector<BitweavingCompareOptions> &options);

    void traversePredicateTree(std::shared_ptr<Node> &node, FilterOperator op,
        std::vector<BitweavingCompareOptions> &options);
  };
}

#endif //HUSTLE_SRC_BITWEAVING_BITWEAVING_INDEX_H_
