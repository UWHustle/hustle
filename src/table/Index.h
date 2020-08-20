//
// Created by Sandhya Kannan on 6/1/20.
//
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <map>
#include "table.h"
#include "operators/Predicate.h"

#ifndef HUSTLE_SRC_TABLE_INDEX_H_
#define HUSTLE_SRC_TABLE_INDEX_H_

enum IndexType{
  BitweavingIndex,
  None
};

using namespace hustle::operators;
class Index{
 public:
  virtual ~Index() {}

  virtual arrow::compute::Datum scan(std::shared_ptr<Predicate>& p) = 0;

  virtual arrow::compute::Datum scan(std::shared_ptr<PredicateTree>& root) = 0;

  virtual void createIndex(std::shared_ptr<Table> table, std::vector<std::string> cols) = 0;

  virtual bool isColumnIndexed(std::string col_name) = 0;

  IndexType get_index_type(){
    return index_type_;
  }

 protected:
  Index(IndexType type) : index_type_(type){}

  IndexType index_type_;
};

struct TableCompare {
  bool operator()(const std::shared_ptr<Table> &lhs, const std::shared_ptr<Table> &rhs) const {
    return lhs.get() < rhs.get();
  }
};
typedef std::map<std::shared_ptr<Table>, Index*, TableCompare> TableIndexMap;
extern TableIndexMap table_index_map;


#endif //HUSTLE_SRC_TABLE_INDEX_H_
