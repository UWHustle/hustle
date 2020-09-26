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

#include <benchmark/benchmark.h>

#include "skew.h"
#include "ssb_workload.h"
#include "storage/util.h"

using namespace hustle::operators;
using namespace std::chrono;

#define DEBUG false

SSB* workload;

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
  t = read_from_csv_file("../../../ssb/data/customer.tbl", c_schema,
                         20 * BLOCK_SIZE);
  write_to_file("../../../ssb/data/customer.hsl", *t);

  t = read_from_csv_file("../../../ssb/data/supplier.tbl", s_schema,
                         20 * BLOCK_SIZE);
  write_to_file("../../../ssb/data/supplier.hsl", *t);

  t = read_from_csv_file("../../../ssb/data/date.tbl", d_schema,
                         20 * BLOCK_SIZE);
  write_to_file("../../../ssb/data/date.hsl", *t);

  t = read_from_csv_file("../../../ssb/data/part.tbl", p_schema,
                         20 * BLOCK_SIZE);
  write_to_file("../../../ssb/data/part.hsl", *t);

  t = read_from_csv_file("../../../ssb/data/lineorder.tbl", lo_schema,
                         20 * BLOCK_SIZE);
  write_to_file("../../../ssb/data/lineorder.hsl", *t);
  std::cout << "read the table files..." << std::endl;
}

static void query11(benchmark::State& state) {
  for (auto _ : state) {
    workload->q11();
  }
}

static void query12(benchmark::State& state) {
  for (auto _ : state) {
    workload->q12();
  }
}

static void query13(benchmark::State& state) {
  for (auto _ : state) {
    workload->q13();
  }
}

static void query21(benchmark::State& state) {
  for (auto _ : state) {
    workload->q21_lip();
  }
}

static void query22(benchmark::State& state) {
  for (auto _ : state) {
    workload->q22_lip();
  }
}

static void query23(benchmark::State& state) {
  for (auto _ : state) {
    workload->q23_lip();
  }
}

static void query31(benchmark::State& state) {
  for (auto _ : state) {
    workload->q31_lip();
  }
}

static void query32(benchmark::State& state) {
  for (auto _ : state) {
    workload->q32_lip();
  }
}

static void query33(benchmark::State& state) {
  for (auto _ : state) {
    workload->q33_lip();
  }
}

static void query34(benchmark::State& state) {
  for (auto _ : state) {
    workload->q34_lip();
  }
}

static void query41(benchmark::State& state) {
  for (auto _ : state) {
    workload->q41_lip();
  }
}

static void query42(benchmark::State& state) {
  for (auto _ : state) {
    workload->q42_lip();
  }
}

static void query43(benchmark::State& state) {
  for (auto _ : state) {
    workload->q43_lip();
  }
}

BENCHMARK(query11);
BENCHMARK(query12);
BENCHMARK(query13);
BENCHMARK(query21);
BENCHMARK(query22);
BENCHMARK(query23);
BENCHMARK(query31);
BENCHMARK(query32);
BENCHMARK(query33);
BENCHMARK(query34);
BENCHMARK(query41);
BENCHMARK(query42);
BENCHMARK(query43);

int main(int argc, char* argv[]) {
  std::cout << "Started initializing with the required data ..." << std::endl;
  read_from_csv();

  if (DEBUG) {
    workload = new SSB(0, 1);
    workload->q11();
    workload->q12();
    workload->q13();
    workload->q21_lip();
    workload->q22_lip();
    workload->q23_lip();
    workload->q31_lip();
    workload->q32_lip();
    workload->q33_lip();
    workload->q34_lip();
    workload->q41_lip();
    workload->q42_lip();
    workload->q43_lip();
  } else {
    workload = new SSB();
    ::benchmark::Initialize(&argc, argv);

    std::cout << "Stated running benchmarks ..." << std::endl;
    ::benchmark::RunSpecifiedBenchmarks();
  }
}
