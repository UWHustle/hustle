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

#include <filesystem>
#include <iostream>

#include "gtest/gtest.h"
#include "storage/cmemlog.h"

using namespace testing;

class HustleMemLogTest : public testing::Test {
 protected:
  void SetUp() override { hustle_memlog_initialize(&hustle_memlog, 100); }

  HustleMemLog* hustle_memlog;
};

TEST_F(HustleMemLogTest, MemlogTestForInsertion) {
  DBRecord* record = hustle_memlog_create_record("test", 4);
  hustle_memlog_insert_record(hustle_memlog, record, 0);
  record = hustle_memlog_create_record("sample", 6);
  hustle_memlog_insert_record(hustle_memlog, record, 0);
  EXPECT_EQ(hustle_memlog->record_list[0].curr_size, 2);
  hustle_memlog_free(hustle_memlog);
}

TEST_F(HustleMemLogTest, MemlogTestForMultipleInsertion) {
  DBRecord* record = hustle_memlog_create_record("test", 4);
  hustle_memlog_insert_record(hustle_memlog, record, 0);
  record = hustle_memlog_create_record("sample", 6);
  hustle_memlog_insert_record(hustle_memlog, record, 0);

  record = hustle_memlog_create_record("test2", 5);
  hustle_memlog_insert_record(hustle_memlog, record, 1);
  record = hustle_memlog_create_record("sample2", 7);
  hustle_memlog_insert_record(hustle_memlog, record, 1);

  EXPECT_EQ(hustle_memlog->record_list[0].curr_size, 2);
  EXPECT_EQ(hustle_memlog->record_list[1].curr_size, 2);
  hustle_memlog_free(hustle_memlog);
}

TEST_F(HustleMemLogTest, MemlogTestForClear) {
  DBRecord* record = hustle_memlog_create_record("test", 4);
  hustle_memlog_insert_record(hustle_memlog, record, 0);
  record = hustle_memlog_create_record("sample", 6);
  hustle_memlog_insert_record(hustle_memlog, record, 0);
  EXPECT_EQ(hustle_memlog->record_list[0].curr_size, 2);

  hustle_memlog_clear(hustle_memlog);
  EXPECT_EQ(hustle_memlog->record_list[0].curr_size, 0);
  hustle_memlog_free(hustle_memlog);
}

TEST_F(HustleMemLogTest, MemlogTestStatus) {
  DBRecord* record = hustle_memlog_create_record("test", 4);
  int status = hustle_memlog_insert_record(hustle_memlog, record, 0);
  EXPECT_EQ(status, MEMLOG_OK);

  record = hustle_memlog_create_record("sample", 6);
  status = hustle_memlog_insert_record(hustle_memlog, record, 0);
  EXPECT_EQ(status, MEMLOG_OK);

  status = hustle_memlog_free(hustle_memlog);
  EXPECT_EQ(status, MEMLOG_OK);

  status = hustle_memlog_insert_record(NULL, NULL, 0);
  EXPECT_EQ(status, MEMLOG_ERROR);
}

