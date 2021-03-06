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

#ifndef HUSTLE_SSB_QUERIES_H
#define HUSTLE_SSB_QUERIES_H

#include "api/hustle_db.h"
#include "catalog/catalog.h"
#include "catalog/column_schema.h"
#include "catalog/table_schema.h"
#include "storage/util.h"

namespace hustle::operators {
    class SSBQueries {
    public:
        SSBQueries(bool is_output_enabled = false);

        void LoadTables();
        char** getfields (char* line, int num);

        void q11();
        void q12();
        void q13();

        void q21();
        void q22();
        void q23();

        void q31();
        void q32();
        void q33();
        void q34();

        void q41();
        void q42();
        void q43();

        ~SSBQueries();

    private:
        bool is_output_enabled;
        std::shared_ptr<HustleDB> hustle_db;
        std::shared_ptr<arrow::Schema> lo_schema, c_schema, s_schema, p_schema,
                d_schema;

        void CreateTable();
    };
}  // namespace hustle::operators
#endif //HUSTLE_SSB_QUERIES_H
