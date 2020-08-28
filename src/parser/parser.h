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

#ifndef HUSTLE_SRC_PARSER_PARSER_H_
#define HUSTLE_SRC_PARSER_PARSER_H_

#include "parser/parse_tree.h"
#include "absl/strings/match.h"
#include "api/hustle_db.h"

namespace hustle {
namespace parser {

class Parser {
 public:
  /**
   * This function takes as input an "EXPLAIN" sql query as well as the
   * hustleDB, calls the SQLite3's library function through sqlite3_exec,
   * extracts the optimized parse tree which maintains the nested loop
   * order as well as the predicates. Project, Aggregate and OrderBy are
   * also perserved in the parse tree.
   *
   * @param sql: input sql query
   * @param hustleDB: input hustle database
   */
  void parse(const std::string &sql, hustle::HustleDB &hustleDB);

  /**
   * Function to return the parse tree
   * @return the parse tree
   */
  std::shared_ptr<ParseTree> getParseTree();

  /**
   * Move select predicates from loop_pred to other_pred
   */
  void preprocessing();

  /**
   * Function to serialize the parse tree
   * @param indent: indentation
   * @return serialized json string
   */
  std::string toString(int indent);

 private:
  /**
   * check if the sql query starts with an "EXPLAIN"
   * @param sql: input sql query string
   */
  static void checkExplain(const std::string &sql);

  std::shared_ptr<ParseTree> parse_tree_;
};

}  // namespace parser
}  // namespace hustle
#endif  // HUSTLE_SRC_PARSER_PARSER_H_
