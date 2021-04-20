//// Licensed to the Apache Software Foundation (ASF) under one
//// or more contributor license agreements.  See the NOTICE file
//// distributed with this work for additional information
//// regarding copyright ownership.  The ASF licenses this file
//// to you under the Apache License, Version 2.0 (the
//// "License"); you may not use this file except in compliance
//// with the License.  You may obtain a copy of the License at
////
////   http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing,
//// software distributed under the License is distributed on an
//// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//// KIND, either express or implied.  See the License for the
//// specific language governing permissions and limitations
//// under the License.
//
#include <filesystem>
#include <iostream>

#include "gtest/gtest.h"
#include "storage/cmemlog.h"

using namespace testing;

class HustleMemLogTest : public testing::Test {
 protected:
  void SetUp() override {
    hustle_memlog_initialize(&hustle_memlog, "test", MEMLOG_INIT_SIZE);
  }

  HustleMemLog* hustle_memlog;
};

TEST_F(HustleMemLogTest, MemlogTestForInsertion) {
  DBRecord* record =
      hustle_memlog_create_record(MEMLOG_HUSTLE_INSERT, 0, "test", 4);
  int status = hustle_memlog_insert_record(hustle_memlog, record, 0);
  EXPECT_EQ(status, MEMLOG_OK);
  record = hustle_memlog_create_record(MEMLOG_HUSTLE_INSERT, 1, "sample", 6);
  status = hustle_memlog_insert_record(hustle_memlog, record, 0);

  DBRecordList* recordList = hustle_memlog_get_records(hustle_memlog, 0);
  record = recordList->head;
  EXPECT_TRUE(memcmp(record->data, "test", 4) == 0);
  record = record->next_record;
  EXPECT_TRUE(memcmp(record->data, "sample", 6) == 0);

  EXPECT_EQ(status, MEMLOG_OK);
  EXPECT_EQ(hustle_memlog->record_list[0].curr_size, 2);
  status = hustle_memlog_free(hustle_memlog);
  EXPECT_EQ(status, MEMLOG_OK);
}
//
//TEST_F(HustleMemLogTest, MemlogTestForMultipleInsertion) {
//  DBRecord* record =
//      hustle_memlog_create_record(MEMLOG_HUSTLE_INSERT, 0, "test", 4);
//  int status = hustle_memlog_insert_record(hustle_memlog, record, 0);
//  EXPECT_EQ(status, MEMLOG_OK);
//  record = hustle_memlog_create_record(MEMLOG_HUSTLE_INSERT, 1, "sample", 6);
//  status = hustle_memlog_insert_record(hustle_memlog, record, 0);
//  EXPECT_EQ(status, MEMLOG_OK);
//
//  record = hustle_memlog_create_record(MEMLOG_HUSTLE_INSERT, 2, "test2", 5);
//  status = hustle_memlog_insert_record(hustle_memlog, record, 1);
//  EXPECT_EQ(status, MEMLOG_OK);
//  record = hustle_memlog_create_record(MEMLOG_HUSTLE_INSERT, 3, "sample2", 7);
//  status = hustle_memlog_insert_record(hustle_memlog, record, 1);
//  EXPECT_EQ(status, MEMLOG_OK);
//
//  DBRecordList* recordList = hustle_memlog_get_records(hustle_memlog, 0);
//  record = recordList->head;
//  EXPECT_TRUE(memcmp(record->data, "test", 4) == 0);
//  record = record->next_record;
//  EXPECT_TRUE(memcmp(record->data, "sample", 6) == 0);
//
//  recordList = hustle_memlog_get_records(hustle_memlog, 1);
//  record = recordList->head;
//  EXPECT_TRUE(memcmp(record->data, "test2", 5) == 0);
//  record = record->next_record;
//  EXPECT_TRUE(memcmp(record->data, "sample2", 7) == 0);
//
//  EXPECT_EQ(hustle_memlog->record_list[0].curr_size, 2);
//  EXPECT_EQ(hustle_memlog->record_list[1].curr_size, 2);
//  status = hustle_memlog_free(hustle_memlog);
//  EXPECT_EQ(status, MEMLOG_OK);
//}
//
//TEST_F(HustleMemLogTest, MemlogTestForClear) {
//  DBRecord* record =
//      hustle_memlog_create_record(MEMLOG_HUSTLE_INSERT, 0, "test", 4);
//  int status = hustle_memlog_insert_record(hustle_memlog, record, 0);
//  EXPECT_EQ(status, MEMLOG_OK);
//  record = hustle_memlog_create_record(MEMLOG_HUSTLE_INSERT, 1, "sample", 6);
//  status = hustle_memlog_insert_record(hustle_memlog, record, 0);
//  EXPECT_EQ(status, MEMLOG_OK);
//  EXPECT_EQ(hustle_memlog->record_list[0].curr_size, 2);
//
//  status = hustle_memlog_clear(hustle_memlog);
//  EXPECT_EQ(status, MEMLOG_OK);
//  EXPECT_EQ(hustle_memlog->record_list[0].curr_size, 0);
//  status = hustle_memlog_free(hustle_memlog);
//  EXPECT_EQ(status, MEMLOG_OK);
//}
//
//TEST_F(HustleMemLogTest, MemlogTestExpansion) {
//  DBRecord* record =
//      hustle_memlog_create_record(MEMLOG_HUSTLE_INSERT, 0, "test", 4);
//  EXPECT_EQ(hustle_memlog->total_size, MEMLOG_INIT_SIZE);
//  int status =
//      hustle_memlog_insert_record(hustle_memlog, record, MEMLOG_INIT_SIZE + 1);
//  EXPECT_EQ(status, MEMLOG_OK);
//  EXPECT_EQ(hustle_memlog->total_size, 2 * (MEMLOG_INIT_SIZE + 1));
//
//  record = hustle_memlog_create_record(MEMLOG_HUSTLE_INSERT, 1, "sample", 6);
//  status =
//      hustle_memlog_insert_record(hustle_memlog, record, MEMLOG_INIT_SIZE + 1);
//  EXPECT_EQ(status, MEMLOG_OK);
//  EXPECT_EQ(hustle_memlog->record_list[MEMLOG_INIT_SIZE + 1].curr_size, 2);
//  EXPECT_EQ(hustle_memlog->total_size, 2 * (MEMLOG_INIT_SIZE + 1));
//
//  status = hustle_memlog_free(hustle_memlog);
//  EXPECT_EQ(status, MEMLOG_OK);
//}
//
//TEST_F(HustleMemLogTest, MemlogTestStatus) {
//  DBRecord* record =
//      hustle_memlog_create_record(MEMLOG_HUSTLE_INSERT, 0, "test", 4);
//  int status = hustle_memlog_insert_record(hustle_memlog, record, 0);
//  EXPECT_EQ(status, MEMLOG_OK);
//
//  record = hustle_memlog_create_record(MEMLOG_HUSTLE_INSERT, 1, "sample", 6);
//  status = hustle_memlog_insert_record(hustle_memlog, record, 0);
//  EXPECT_EQ(status, MEMLOG_OK);
//
//  status = hustle_memlog_free(hustle_memlog);
//  EXPECT_EQ(status, MEMLOG_OK);
//
//  status = hustle_memlog_insert_record(NULL, NULL, 0);
//  EXPECT_EQ(status, MEMLOG_ERROR);
//
//  status = hustle_memlog_free(NULL);
//  EXPECT_EQ(status, MEMLOG_ERROR);
//
//  status = hustle_memlog_clear(NULL);
//  EXPECT_EQ(status, MEMLOG_ERROR);
//
//  hustle_memlog_initialize(&hustle_memlog, "test", MEMLOG_INIT_SIZE);
//  DBRecordList* recordList = hustle_memlog_get_records(hustle_memlog, 200);
//  EXPECT_TRUE(recordList == NULL);
//
//  recordList = hustle_memlog_get_records(NULL, 0);
//  EXPECT_TRUE(recordList == NULL);
//}
