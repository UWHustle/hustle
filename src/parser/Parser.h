#ifndef HUSTLE_SRC_PARSER_PARSER_H_
#define HUSTLE_SRC_PARSER_PARSER_H_

#include "absl/strings/match.h"

#include "api/HustleDB.h"
#include "ParseTree.h"

extern const int SERIAL_BLOCK_SIZE = 4096;
char project[SERIAL_BLOCK_SIZE];
char loopPred[SERIAL_BLOCK_SIZE];
char otherPred[SERIAL_BLOCK_SIZE];
char groupBy[SERIAL_BLOCK_SIZE];
char orderBy[SERIAL_BLOCK_SIZE];
char *currPos = nullptr;

namespace hustle {
namespace parser {

class Parser {
 public:
  /**
   * This function takes as input an "EXPLAIN" sql query as well as the
   * hustleDB, calls the SQLite3's library function through sqlite3_exec,
   * extracts the optimized parse tree which maintains the nested loop
   * order as well as the predicates. Project, GroupBy and OrderBy are also
   * perserved in the parse tree.
   *
   * @param sql: input sql query
   * @param hustleDB: input hustle database
   */
  void parse(const std::string &sql, hustle::HustleDB &hustleDB) {
    check_explain(sql);
    hustleDB.getPlan(sql);

    std::string text =
        "{\"project\": [" + std::string(project) +
        "], \"loop_pred\": [" + std::string(loopPred) +
        "], \"other_pred\": [" + std::string(otherPred) +
        "], \"group_by\": [" + std::string(groupBy) +
        "], \"order_by\": [" + std::string(orderBy) +
        "]}";

    json j = json::parse(text);
    parse_tree_ = j;
    preprocessing();
  }

  /**
   * Function to return the parse tree
   * @return the parse tree
   */
  std::shared_ptr<ParseTree> get_parse_tree() {
    return parse_tree_;
  }

  /**
   * Move select predicates from loop_pred to other_pred
   */
  void preprocessing() {
    for (auto &loop_pred : parse_tree_->loop_pred) {
      for (auto it = loop_pred->predicates.begin();
           it != loop_pred->predicates.end();) {
        if ((*it)->plan_type == "SELECT_Pred") {
          parse_tree_->other_pred.push_back(std::move(*it));
          loop_pred->predicates.erase(it);
        } else {
          it += 1;
        }
      }
    }
  }

  /**
   * Function to serialize the parse tree
   * @param indent: indentation
   * @return serialized json string
   */
  std::string to_string(int indent) {
    json j = parse_tree_;
    return j.dump(indent);
  }

 private:
  /**
   * check if the sql query starts with an "EXPLAIN"
   * @param sql: input sql query string
   */
  static void check_explain(const std::string &sql) {
    if (!absl::StartsWithIgnoreCase(sql, "EXPLAIN")) {
      std::cerr << "Not starting with EXPLAIN keyword" << std::endl;
      exit(-1);
    }
  }

  std::shared_ptr<ParseTree> parse_tree_;
};

}
}
#endif //HUSTLE_SRC_PARSER_PARSER_H_
