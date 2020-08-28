// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "skew.h"
#include "ssb_workload.h"
#include "storage/util.h"
using namespace hustle::operators;
using namespace std::chrono;

void read_from_csv() {
  std::shared_ptr<Table> lo, c, s, p, d;
  std::shared_ptr<arrow::Schema> lo_schema, c_schema, s_schema, p_schema,
      d_schema;

  auto field1 = arrow::field("order key", arrow::uint32());
  auto field2 = arrow::field("line number", arrow::int64());
  auto field3 = arrow::field("cust key", arrow::int64());
  auto field4 = arrow::field("part key", arrow::int64());
  auto field5 = arrow::field("supp key", arrow::int64());
  auto field6 = arrow::field("order date", arrow::int64());
  auto field7 = arrow::field("ord priority", arrow::utf8());
  auto field8 = arrow::field("ship priority", arrow::int64());
  auto field9 = arrow::field("quantity", arrow::uint8());
  auto field10 = arrow::field("extended price", arrow::int64());
  auto field11 = arrow::field("ord total price", arrow::int64());
  auto field12 = arrow::field("discount", arrow::uint8());
  auto field13 = arrow::field("revenue", arrow::int64());
  auto field14 = arrow::field("supply cost", arrow::int64());
  auto field15 = arrow::field("tax", arrow::int64());
  auto field16 = arrow::field("commit date", arrow::int64());
  auto field17 = arrow::field("ship mode", arrow::utf8());
  lo_schema = arrow::schema({field1, field2, field3, field4, field5, field6,
                             field7, field8, field9, field10, field11, field12,
                             field13, field14, field15, field16, field17});

  auto s_field1 = arrow::field("s supp key", arrow::int64());
  auto s_field2 = arrow::field("s name", arrow::utf8());
  auto s_field3 = arrow::field("s address", arrow::utf8());
  auto s_field4 = arrow::field("s city", arrow::utf8());
  auto s_field5 = arrow::field("s nation", arrow::utf8());
  auto s_field6 = arrow::field("s region", arrow::utf8());
  auto s_field7 = arrow::field("s phone", arrow::utf8());

  s_schema = arrow::schema(
      {s_field1, s_field2, s_field3, s_field4, s_field5, s_field6, s_field7});

  auto c_field1 = arrow::field("c cust key", arrow::int64());
  auto c_field2 = arrow::field("c name", arrow::utf8());
  auto c_field3 = arrow::field("c address", arrow::utf8());
  auto c_field4 = arrow::field("c city", arrow::utf8());
  auto c_field5 = arrow::field("c nation", arrow::utf8());
  auto c_field6 = arrow::field("c region", arrow::utf8());
  auto c_field7 = arrow::field("c phone", arrow::utf8());
  auto c_field8 = arrow::field("c mkt segment", arrow::utf8());

  c_schema = arrow::schema({c_field1, c_field2, c_field3, c_field4, c_field5,
                            c_field6, c_field7, c_field8});

  auto d_field1 = arrow::field("date key", arrow::int64());
  auto d_field2 = arrow::field("date", arrow::utf8());
  auto d_field3 = arrow::field("day of week", arrow::utf8());
  auto d_field4 = arrow::field("month", arrow::utf8());
  auto d_field5 = arrow::field("year", arrow::int64());
  auto d_field6 = arrow::field("year month num", arrow::int64());
  auto d_field7 = arrow::field("year month", arrow::utf8());
  auto d_field8 = arrow::field("day num in week", arrow::int64());
  auto d_field9 = arrow::field("day num in month", arrow::int64());
  auto d_field10 = arrow::field("day num in year", arrow::int64());
  auto d_field11 = arrow::field("month num in year", arrow::int64());
  auto d_field12 = arrow::field("week num in year", arrow::int64());
  auto d_field13 = arrow::field("selling season", arrow::utf8());
  auto d_field14 = arrow::field("last day in week fl", arrow::int64());
  auto d_field15 = arrow::field("last day in month fl", arrow::int64());
  auto d_field16 = arrow::field("holiday fl", arrow::int64());
  auto d_field17 = arrow::field("weekday fl", arrow::int64());

  d_schema = arrow::schema({d_field1, d_field2, d_field3, d_field4, d_field5,
                            d_field6, d_field7, d_field8, d_field9, d_field10,
                            d_field11, d_field12, d_field13, d_field14,
                            d_field15, d_field16, d_field17});

  auto p_field1 = arrow::field("part key", arrow::int64());
  auto p_field2 = arrow::field("name", arrow::utf8());
  auto p_field3 = arrow::field("mfgr", arrow::utf8());
  auto p_field4 = arrow::field("category", arrow::utf8());
  auto p_field5 = arrow::field("brand1", arrow::utf8());
  auto p_field6 = arrow::field("color", arrow::utf8());
  auto p_field7 = arrow::field("type", arrow::utf8());
  auto p_field8 = arrow::field("size", arrow::int64());
  auto p_field9 = arrow::field("container", arrow::utf8());

  p_schema = arrow::schema({p_field1, p_field2, p_field3, p_field4, p_field5,
                            p_field6, p_field7, p_field8, p_field9});

  std::shared_ptr<Table> t;

  t = read_from_csv_file(
      "/mydata/SQL-benchmark-data-generator/ssbgen/customer.tbl", c_schema,
      20 * BLOCK_SIZE);
  write_to_file("/mydata/SQL-benchmark-data-generator/ssbgen/customer.hsl", *t);
  std::cout << "c" << std::endl;

  t = read_from_csv_file(
      "/mydata/SQL-benchmark-data-generator/ssbgen/supplier.tbl", s_schema,
      20 * BLOCK_SIZE);
  write_to_file("/mydata/SQL-benchmark-data-generator/ssbgen/supplier.hsl", *t);
  std::cout << "s" << std::endl;

  t = read_from_csv_file("/mydata/SQL-benchmark-data-generator/ssbgen/date.tbl",
                         d_schema, 20 * BLOCK_SIZE);
  write_to_file("/mydata/SQL-benchmark-data-generator/ssbgen/date.hsl", *t);
  std::cout << "d" << std::endl;

  t = read_from_csv_file("/mydata/SQL-benchmark-data-generator/ssbgen/part.tbl",
                         p_schema, 20 * BLOCK_SIZE);
  write_to_file("/mydata/SQL-benchmark-data-generator/ssbgen/part.hsl", *t);
  std::cout << "p" << std::endl;

  t = read_from_csv_file(
      "/mydata/SQL-benchmark-data-generator/ssbgen/lineorder.tbl", lo_schema,
      20 * BLOCK_SIZE);
  write_to_file("/mydata/SQL-benchmark-data-generator/ssbgen/lineorder.hsl",
                *t);
  std::cout << "lo" << std::endl;
  //
  //    t =
  //    read_from_csv_file("/Users/corrado/hustle/src/ssb/data/ssb-01/customer.tbl",
  //    c_schema, 20*BLOCK_SIZE);
  //    write_to_file("../../../src/ssb/data/ssb-01/customer.hsl", *t);
  //    std::cout << "c" << std::endl;
  //
  //    t =
  //    read_from_csv_file("/Users/corrado/hustle/src/ssb/data/ssb-01/supplier.tbl",
  //    s_schema, 20*BLOCK_SIZE);
  //    write_to_file("../../../src/ssb/data/ssb-01/supplier.hsl", *t);
  //    std::cout << "s" << std::endl;
  //
  //    t =
  //    read_from_csv_file("/Users/corrado/hustle/src/ssb/data/ssb-01/date.tbl",
  //    d_schema, 20*BLOCK_SIZE);
  //    write_to_file("../../../src/ssb/data/ssb-01/date.hsl", *t);
  //    std::cout << "d" << std::endl;
  //
  //    t =
  //    read_from_csv_file("/Users/corrado/hustle/src/ssb/data/ssb-01/part.tbl",
  //    p_schema, 20*BLOCK_SIZE);
  //    write_to_file("../../../src/ssb/data/ssb-01/part.hsl", *t);
  //    std::cout << "p" << std::endl;
  //
  //    t =
  //    read_from_csv_file("/Users/corrado/hustle/src/ssb/data/ssb-01/lineorder.tbl",
  //    lo_schema, 20*BLOCK_SIZE);
  //    write_to_file("../../../src/ssb/data/ssb-01/lineorder.hsl", *t);
  //    std::cout << "lo" << std::endl;
}

void run_experiment(int sf, int num_trials = 1, bool load = false,
                    bool print = false) {
  if (load) read_from_csv();
  SSB workload(sf, print);
  std::cout << workload.lo->get_num_blocks() << std::endl;

  std::cout << "skewing column..." << std::endl;
  //    skew_column(workload.lo->get_column(4), true);
  //    std::cout << workload.lo->get_column(4)->ToString() << std::endl;
  std::cout << "sleeping after loading tables..." << std::endl;
  sleep(0);
  for (int i = 0; i < num_trials; i++) {
    std::cout << "batch start" << std::endl;
    //
    //        workload.q11();

    workload.q11();
    workload.q12();
    workload.q13();

    //        workload.q21();
    //        workload.q22();
    //        workload.q23();
    //
    //        workload.q31();
    //        workload.q32();
    //        workload.q33();
    //        workload.q34();

    //        workload.q41();
    //        workload.q42();
    //        workload.q43();
    //
    workload.q21_lip();
    workload.q22_lip();
    workload.q23_lip();

    workload.q31_lip();
    workload.q32_lip();
    workload.q33_lip();
    workload.q34_lip();

    workload.q41_lip();
    workload.q42_lip();
    workload.q43_lip();

    std::cout << "sleeping..." << std::endl;
    sleep(1);
  }
}

int main(int argc, char *argv[]) {
  //    read_from_csv();
  //    return 0;
  if (argc == 5) {
    run_experiment(std::stoi(argv[1]), std::stoi(argv[2]), std::stoi(argv[3]),
                   std::stoi(argv[4]));
  } else if (argc == 4) {
    run_experiment(std::stoi(argv[1]), std::stoi(argv[2]), std::stoi(argv[3]));
  } else if (argc == 3) {
    run_experiment(std::stoi(argv[1]), std::stoi(argv[2]));
  } else if (argc == 2) {
    run_experiment(std::stoi(argv[1]));
  } else {
    run_experiment(1, 1, 0, 1);
  }
}