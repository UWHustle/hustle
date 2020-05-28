//
// Created by Sandhya Kannan on 3/18/20.
//

#include "bitweaving_test_base.h"
#include "bitweaving/bitweaving_util.h"
#include <iostream>
#include <gtest/gtest.h>
#include <bitweaving/table.h>
#include <table/util.h>

namespace hustle::bitweaving {

BWTable *BitweavingTestBase::customer_table = nullptr;
BWTable *BitweavingTestBase::date_table = nullptr;
BWTable *BitweavingTestBase::lineorder_table = nullptr;
BWTable *BitweavingTestBase::part_table = nullptr;
BWTable *BitweavingTestBase::supplier_table = nullptr;

std::shared_ptr<Table> BitweavingTestBase::customer = nullptr;
std::shared_ptr<Table> BitweavingTestBase::date = nullptr;
std::shared_ptr<Table> BitweavingTestBase::lineorder = nullptr;
std::shared_ptr<Table> BitweavingTestBase::part = nullptr;
std::shared_ptr<Table> BitweavingTestBase::supplier = nullptr;

BitweavingTestBase::BitweavingTestBase() {
  options = Options();
  options.delete_exist_files = true;
  options.in_memory = true;
}

BitweavingTestBase::~BitweavingTestBase() {

}

void BitweavingTestBase::SetUp() {
  std::cout << "Inside SetUp of BitweavingTest" << std::endl;
}

void BitweavingTestBase::TearDown() {
  std::cout << "Inside TearDown of BitweavingTest" << std::endl;
}

void BitweavingTestBase::SetUpTestCase() {

  std::cout << "Inside SetUpTestCase of BitweavingTest" << std::endl;

  std::vector<std::string> customer_schema = {"cust key", "name", "address", "city", "nation", "region", "phone",
                                              "mkt segment"};
  std::vector<std::string> date_schema = {"date key", "date", "day of week", "month", "year", "year month num",
                                          "year month", "day num in week", "day num in month", "day num year",
                                          "month num in year", "week num in year", "selling season",
                                          "last day in week fl", "last day in month fl", "holiday fl",
                                          "weekday fl"};
  std::vector<std::string> lineorder_schema = {"order key", "line number", "cust key", "part key", "supp key",
                                               "order date", "ord priority", "ship priority", "quantity",
                                               "extended price", "ord total price", "discount", "revenue",
                                               "supply cost", "tax", "commit date", "ship mode"};
  std::vector<std::string> part_schema = {"part key", "name", "mfgr", "category", "brand", "color",
                                          "type", "size", "container"};
  std::vector<std::string> supplier_schema = {"supp key", "name", "address", "city", "nation",
                                              "region", "phone"};

  arrow::MemoryPool *pool = arrow::default_memory_pool();

  //I have set the working directory for this target to be the cpp folder. Thus entering a path relative to it
  std::string folder_path = "/Users/sandhyakannan/Masters/RA/Project/Arrow-Bitweaving/cpp/data/sf1/";

  //customer    = build_table(folder_path + "customer.tbl",  pool, customer_schema);
  //date        = build_table(folder_path + "date.tbl", pool, date_schema);
  //write_to_file("/Users/sandhyakannan/Masters/RA/Project/Arrow-Bitweaving/cpp/data/sf1/date.hsl", *date);
  date =
      read_from_file("/Users/sandhyakannan/Masters/RA/Project/Arrow-Bitweaving/cpp/data/sf1/date.hsl",
                     false);
  //lineorder   = build_table(folder_path + "lineorder.tbl", pool, lineorder_schema);
  //write_to_file("/Users/sandhyakannan/Masters/RA/Project/Arrow-Bitweaving/cpp/data/sf1/lineorder.hsl", *lineorder);
  lineorder =
      read_from_file("/Users/sandhyakannan/Masters/RA/Project/Arrow-Bitweaving/cpp/data/sf1/lineorder.hsl",
                     false);
  //part        = build_table(folder_path + "part.tbl",      pool, part_schema);
  //supplier    = build_table(folder_path + "supplier.tbl",  pool, supplier_schema);

  Options options = Options();
  options.delete_exist_files = true;
  options.in_memory = true;

  customer_table = new BWTable("./abc", options);
  date_table = new BWTable("./abc", options);
  lineorder_table = new BWTable("./abc", options);
  part_table = new BWTable("./abc", options);
  supplier_table = new BWTable("./abc", options);

  lineorder_table = createBitweavingIndex(lineorder,{BitweavingColumnIndexUnit("quantity", 6),
                         BitweavingColumnIndexUnit("discount", 4)}, false);

  date_table = createBitweavingIndex(date,{BitweavingColumnIndexUnit("year", 11),
                         BitweavingColumnIndexUnit("year month num", 16),
                         BitweavingColumnIndexUnit("week num in year", 16)},
                        true);
  //createBitweavingIndex(customer, customer_table);
  //createBitweavingIndex(part, part_table);
  //createBitweavingIndex(supplier, supplier_table);
}

void BitweavingTestBase::TearDownTestCase() {
  delete customer_table;
  delete part_table;
  delete lineorder_table;
  delete date_table;
  delete supplier_table;
}

}