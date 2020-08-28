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

#include "storage/util.h"

#include <arrow/io/api.h>

#include <filesystem>
#include <iostream>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "storage/table.h"

using namespace testing;

class HustleUtilTest : public testing::Test {
 protected:
  void SetUp() override {}
};

TEST_F(HustleUtilTest, CatalogToArrowSchema) {
  hustle::catalog::ColumnType int_type(hustle::catalog::HustleType::INTEGER,
                                       sizeof(int64_t));
  hustle::catalog::ColumnSchema int_col("int col", int_type, true, true);

  hustle::catalog::ColumnType str_type(hustle::catalog::HustleType::CHAR);
  hustle::catalog::ColumnSchema str_col("str col", str_type, true, true);

  hustle::catalog::TableSchema catalog_schema("schema");
  catalog_schema.addColumn(int_col);
  catalog_schema.addColumn(str_col);

  auto schema = make_schema(catalog_schema);

  auto test_schema = arrow::schema({arrow::field("int col", arrow::int64()),
                                    arrow::field("str col", arrow::utf8())});

  EXPECT_TRUE(schema->Equals(test_schema));
}