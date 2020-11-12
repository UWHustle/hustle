#include "resolver/cresolver.h"

#include <iostream>

#include "api/hustle_db.h"
#include "catalog/catalog.h"
#include "execution/execution_plan.h"
#include "operators/select.h"
#include "operators/utils/operator_result.h"
#include "resolver/select_resolver.h"

void execute(hustle::resolver::SelectResolver* select_resolver,
             Catalog* catalog);

int resolveSelect(char* dbName, Sqlite3Select* queryTree) {
  ExprList* pEList = queryTree->pEList;
  Expr* pWhere = queryTree->pWhere;
  ExprList* pGroupBy = queryTree->pGroupBy;
  Expr* pHaving = queryTree->pHaving;
  ExprList* pOrderBy = queryTree->pOrderBy;

  // TODO: (@srsuryadev) resolve the select query
  // return 0 if query is supported in column store else return 1
  using hustle::resolver::SelectResolver;
  Catalog* catalog = hustle::HustleDB::getCatalog(dbName).get();
  if (dbName == NULL || catalog == nullptr) return 0;

  SelectResolver* select_resolver = new SelectResolver(catalog);
  bool is_resolvable = select_resolver->ResolveSelectTree(queryTree);
  if (is_resolvable) {
    execute(select_resolver, catalog);
  }
  delete select_resolver;
  return is_resolvable ? 1 : 0;
}

void execute(hustle::resolver::SelectResolver* select_resolver,
             Catalog* catalog) {
  using namespace hustle::operators;
  std::unordered_map<std::string, std::shared_ptr<PredicateTree>>
      select_predicates = select_resolver->get_select_predicates();

  std::vector<std::shared_ptr<OperatorResult>> select_result;
  std::vector<std::shared_ptr<hustle::operators::Select>> select_operators;

  for (auto const& [table_name, predicate_tree] : select_predicates) {
    std::shared_ptr<OperatorResult> input_result =
        std::make_shared<OperatorResult>();
    std::shared_ptr<OperatorResult> output_result =
        std::make_shared<OperatorResult>();
    auto table_ptr = catalog->getTable(table_name);
    if (table_ptr == nullptr) return;
    if (predicate_tree == nullptr) {
      output_result->append(table_ptr);
    } else {
      input_result->append(table_ptr);
      std::shared_ptr<hustle::operators::Select> select =
          std::make_shared<hustle::operators::Select>(
              0, table_ptr, input_result, output_result, predicate_tree);
      select_operators.emplace_back(select);
    }
    select_result.emplace_back(output_result);
  }
  JoinGraph join_graph({*(select_resolver->get_join_predicates())});

  std::shared_ptr<OperatorResult> join_result_out;
  Join join_op(0, select_result, join_result_out, join_graph);

  std::shared_ptr<std::vector<AggregateReference>> agg_refs =
      (select_resolver->get_agg_references());

  assert(agg_refs.size() == 1);  // currently supports one agg op
  std::shared_ptr<OperatorResult> agg_result_out;

  auto group_by_ref_ptrs = *(select_resolver->get_groupby_references());
  std::vector<ColumnReference> group_by_refs(group_by_ref_ptrs.size());
  std::transform(
      group_by_ref_ptrs.begin(), group_by_ref_ptrs.end(), group_by_refs.begin(),
      [](std::shared_ptr<hustle::operators::ColumnReference> x) { return *x; });

  auto order_by_ref_ptrs = *(select_resolver->get_orderby_references());
  std::vector<ColumnReference> order_by_refs(order_by_ref_ptrs.size());
  std::transform(
      order_by_ref_ptrs.begin(), order_by_ref_ptrs.end(), order_by_refs.begin(),
      [](std::shared_ptr<hustle::operators::ColumnReference> x) { return *x; });

  Aggregate agg_op(0, join_result_out, agg_result_out, *agg_refs, group_by_refs,
                   order_by_refs);

  hustle::ExecutionPlan plan(0);
  auto join_id = plan.addOperator(&join_op);
  auto agg_id = plan.addOperator(&agg_op);

  for (auto select_operator : select_operators) {
    auto select_id = plan.addOperator(select_operator.get());
    plan.createLink(select_id, join_id);
  }
  // Declare aggregate dependency on join operator
  plan.createLink(join_id, agg_id);

  hustle::Scheduler& scheduler = hustle::HustleDB::getScheduler();
  scheduler.addTask(&plan);
  scheduler.start();
  scheduler.join();

  std::vector<std::shared_ptr<hustle::resolver::ProjectReference>>
      project_references = *(select_resolver->get_project_references());
  std::vector<ColumnReference> agg_project_cols;
  for (auto project_ref : project_references) {
    if (!project_ref->alias.empty()) {
      agg_project_cols.emplace_back(
          ColumnReference{nullptr, project_ref->alias});
    } else {
      agg_project_cols.emplace_back(
          ColumnReference{nullptr, project_ref->colRef.col_name});
    }
  }

  std::shared_ptr<hustle::storage::DBTable> out_table =
      agg_result_out->materialize(agg_project_cols);
  out_table->print();
}
