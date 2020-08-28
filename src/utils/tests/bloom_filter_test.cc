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

#include "operators/bloom_filter.h"

#include <fstream>

#include "gtest/gtest.h"
#include "storage/util.h"

using namespace testing;

class BloomFilterTestFixture : public testing::Test {
 protected:
  void SetUp() override {}
};

TEST_F(BloomFilterTestFixture, Test1) {
  auto d = read_from_file("/Users/corrado/hustle/data/ssb-1/date.hsl");

  auto col = d->get_column(0);
  BloomFilter b(col->length());
  b.set_memory(10);

  // Build
  for (int i = 0; i < col->num_chunks(); i++) {
    auto chunk = std::static_pointer_cast<arrow::Int64Array>(col->chunk(i));

    for (int j = 0; j < chunk->length(); j++) {
      auto val = chunk->Value(j);
      b.insert(val);
    }
  }

  // Probe
  for (int i = 0; i < col->num_chunks(); i++) {
    auto chunk = std::static_pointer_cast<arrow::Int64Array>(col->chunk(i));

    for (int j = 0; j < chunk->length(); j++) {
      auto val = chunk->Value(j);
      b.probe(j);
    }

    b.update();
  }

  std::cout << std::endl;
  std::cout << "hit rate = " << b.get_hit_rate() << std::endl;
}