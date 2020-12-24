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

#ifndef HUSTLE_TATP_WORKLOAD_H
#define HUSTLE_TATP_WORKLOAD_H

#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>

#include <iostream>

#include "api/hustle_db.h"
#include "catalog/catalog.h"
#include "catalog/column_schema.h"
#include "catalog/table_schema.h"
#include "parser/parser.h"
#include "sqlite3/sqlite3.h"
#include "storage/util.h"

namespace hustle::operators {
class TATP {
 public:
  TATP();

  void RunBenchmark();

  ~TATP();

 private:
  std::shared_ptr<HustleDB> hustle_db;

  void CreateTable();

  void RunQuery1();

  void RunQuery2();

  void RunQuery3();

  void RunQuery4();

  void RunQuery5();

  void RunQuery6();

  void RunQuery7();
};
}  // namespace hustle::operators

#endif  // HUSTLE_TATP_WORKLOAD_H
