#add_subdirectory(cost)
#add_subdirectory(rules)

# Declare micro-libs:
#add_library(project_optimizer_Aggregate Aggregate.cpp Aggregate.hpp)
#add_library(project_optimizer_AggregateFunction AggregateFunction.cpp AggregateFunction.hpp)
#add_library(project_optimizer_Alias Alias.cpp Alias.hpp)
#add_library(project_optimizer_AttributeReference AttributeReference.cpp AttributeReference.hpp)
#add_library(project_optimizer_ChainedJoinAggregate ChainedJoinAggregate.cpp ChainedJoinAggregate.hpp)
#add_library(project_optimizer_Comparison Comparison.cpp Comparison.hpp)
#add_library(project_optimizer_Conjunction Conjunction.cpp Conjunction.hpp)
#add_library(project_optimizer_EquiJoin EquiJoin.cpp EquiJoin.hpp)
#add_library(project_optimizer_EquiJoinAggregate EquiJoinAggregate.cpp EquiJoinAggregate.hpp)
#add_library(project_optimizer_ExecutionGenerator ExecutionGenerator.cpp ExecutionGenerator.hpp)
add_library(project_optimizer_ExecutionPlan ExecutionPlan.cpp ExecutionPlan.hpp)
#add_library(project_optimizer_ExprId ../empty_src.cpp ExprId.hpp)
#add_library(project_optimizer_Expression ../empty_src.cpp Expression.hpp)
#add_library(project_optimizer_ExpressionUtil ExpressionUtil.cpp ExpressionUtil.hpp)
#add_library(project_optimizer_Filter Filter.cpp Filter.hpp)
#add_library(project_optimizer_MultiwayCartesianJoin MultiwayCartesianJoin.cpp MultiwayCartesianJoin.hpp)
#add_library(project_optimizer_MultiwayEquiJoinAggregate MultiwayEquiJoinAggregate.cpp MultiwayEquiJoinAggregate.hpp)
#add_library(project_optimizer_Optimizer Optimizer.cpp Optimizer.hpp)
#add_library(project_optimizer_OptimizerContext ../empty_src.cpp OptimizerContext.hpp)
#add_library(project_optimizer_OptimizerTree ../empty_src.cpp OptimizerTree.hpp)
#add_library(project_optimizer_OptimizerTypedefs ../empty_src.cpp OptimizerTypedefs.hpp)
#add_library(project_optimizer_PatternMatcher ../empty_src.cpp PatternMatcher.hpp)
#add_library(project_optimizer_Plan ../empty_src.cpp Plan.hpp)
#add_library(project_optimizer_Predicate ../empty_src.cpp Predicate.hpp)
#add_library(project_optimizer_PredicateLiteral PredicateLiteral.cpp PredicateLiteral.hpp)
#add_library(project_optimizer_Project Project.cpp Project.hpp)
#add_library(project_optimizer_QueryHandle ../empty_src.cpp QueryHandle.hpp)
#add_library(project_optimizer_Scalar ../empty_src.cpp Scalar.hpp)
#add_library(project_optimizer_ScalarLiteral ScalarLiteral.cpp ScalarLiteral.hpp)
#add_library(project_optimizer_Selection Selection.cpp Selection.hpp)
#add_library(project_optimizer_TableReference TableReference.cpp TableReference.hpp)
#add_library(project_optimizer_TableView TableView.cpp TableView.hpp)
#add_library(project_optimizer_TopLevelPlan TopLevelPlan.cpp TopLevelPlan.hpp)

# Link dependencies:
#target_link_libraries(project_optimizer_Aggregate
#                      glog
#                      project_optimizer_AttributeReference
#                      project_optimizer_ExpressionUtil
#                      project_optimizer_Plan
#                      project_optimizer_Predicate
#                      project_optimizer_Scalar
#                      project_utility_ContainerUtil
#                      project_utility_Macros
#                      project_utility_MemoryUtil)
#target_link_libraries(project_optimizer_AggregateFunction
#                      glog
#                      project_optimizer_AttributeReference
#                      project_optimizer_Expression
#                      project_optimizer_PatternMatcher
#                      project_optimizer_Scalar
#                      project_types_Type
#                      project_utility_Macros)
#target_link_libraries(project_optimizer_Alias
#                      glog
#                      project_optimizer_AttributeReference
#                      project_optimizer_Expression
#                      project_optimizer_PatternMatcher
#                      project_optimizer_Plan
#                      project_optimizer_Scalar
#                      project_types_Type
#                      project_utility_Macros)
#target_link_libraries(project_optimizer_AttributeReference
#                      glog
#                      project_operators_expressions_ScalarAttribute
#                      project_optimizer_ExprId
#                      project_optimizer_Expression
#                      project_optimizer_PatternMatcher
#                      project_optimizer_Plan
#                      project_optimizer_Scalar
#                      project_types_Type
#                      project_utility_Macros)
#target_link_libraries(project_optimizer_ChainedJoinAggregate
#                      glog
#                      project_optimizer_AttributeReference
#                      project_optimizer_ExpressionUtil
#                      project_optimizer_Plan
#                      project_optimizer_Predicate
#                      project_optimizer_Scalar
#                      project_utility_ContainerUtil
#                      project_utility_Macros
#                      project_utility_MemoryUtil)
#target_link_libraries(project_optimizer_Comparison
#                      glog
#                      project_operators_expressions_Comparison
#                      project_operators_expressions_ComparisonType
#                      project_optimizer_AttributeReference
#                      project_optimizer_PatternMatcher
#                      project_optimizer_Predicate
#                      project_optimizer_Scalar
#                      project_optimizer_ScalarLiteral
#                      project_utility_ContainerUtil
#                      project_utility_Macros
#                      project_utility_Range)
#target_link_libraries(project_optimizer_Conjunction
#                      glog
#                      project_operators_expressions_Conjunction
#                      project_optimizer_AttributeReference
#                      project_optimizer_Expression
#                      project_optimizer_PatternMatcher
#                      project_optimizer_Predicate
#                      project_utility_ContainerUtil
#                      project_utility_Macros
#                      project_utility_MemoryUtil
#                      project_utility_Range
#                      project_utility_StringUtil)
#target_link_libraries(project_optimizer_EquiJoin
#                      glog
#                      project_optimizer_AttributeReference
#                      project_optimizer_ExpressionUtil
#                      project_optimizer_Plan
#                      project_optimizer_Predicate
#                      project_optimizer_Scalar
#                      project_utility_ContainerUtil
#                      project_utility_Macros
#                      project_utility_MemoryUtil)
#target_link_libraries(project_optimizer_EquiJoinAggregate
#                      glog
#                      project_optimizer_AttributeReference
#                      project_optimizer_ExpressionUtil
#                      project_optimizer_Plan
#                      project_optimizer_Predicate
#                      project_optimizer_Scalar
#                      project_utility_ContainerUtil
#                      project_utility_Macros
#                      project_utility_MemoryUtil)
#target_link_libraries(project_optimizer_ExecutionGenerator
#                      glog
#                      project_operators_AggregateOperator
#                      project_operators_ChainedJoinAggregateOperator
#                      project_operators_CreateTableViewOperator
#                      project_operators_DropTableOperator
#                      project_operators_HashJoinOperator
#                      project_operators_IndexJoinAggregateOperator
#                      project_operators_IndexJoinOperator
#                      project_operators_MultiwayJoinAggregateOperator
#                      project_operators_OperatorTypedefs
#                      project_operators_PrintOperator
#                      project_operators_RelationalOperator
#                      project_operators_ScanJoinAggregateOperator
#                      project_operators_SelectOperator
#                      project_operators_SortMergeJoinOperator
#                      project_operators_expressions_Comparison
#                      project_operators_expressions_Predicate
#                      project_operators_expressions_Scalar
#                      project_operators_expressions_ScalarAttribute
#                      project_optimizer_Aggregate
#                      project_optimizer_AggregateFunction
#                      project_optimizer_Alias
#                      project_optimizer_AttributeReference
#                      project_optimizer_ChainedJoinAggregate
#                      project_optimizer_Comparison
#                      project_optimizer_Conjunction
#                      project_optimizer_EquiJoin
#                      project_optimizer_EquiJoinAggregate
#                      project_optimizer_ExecutionPlan
#                      project_optimizer_ExprId
#                      project_optimizer_ExpressionUtil
#                      project_optimizer_MultiwayEquiJoinAggregate
#                      project_optimizer_PatternMatcher
#                      project_optimizer_Plan
#                      project_optimizer_QueryHandle
#                      project_optimizer_ScalarLiteral
#                      project_optimizer_Selection
#                      project_optimizer_TableReference
#                      project_optimizer_TableView
#                      project_optimizer_TopLevelPlan
#                      project_optimizer_cost_SimpleCostModel
#                      project_storage_Attribute
#                      project_storage_Database
#                      project_storage_Relation
#                      project_utility_Macros)
target_link_libraries(project_optimizer_ExecutionPlan
                      ${GFLAGS_LIB_NAME}
                      hustle_operators_Operator
                      glog
                      project_scheduler_SchedulerInterface
                      project_scheduler_SchedulerTypedefs
                      project_scheduler_Task
                      project_utility_EventProfiler
                      project_utility_Macros
                      project_utility_SyncStream)
#target_link_libraries(project_optimizer_Expression
#                      project_optimizer_OptimizerTree
#                      project_types_Type
#                      project_utility_Macros)
#target_link_libraries(project_optimizer_ExpressionUtil
#                      project_optimizer_AttributeReference
#                      project_optimizer_Conjunction
#                      project_optimizer_ExprId
#                      project_optimizer_Expression
#                      project_optimizer_PatternMatcher
#                      project_optimizer_Predicate
#                      project_optimizer_PredicateLiteral
#                      project_optimizer_Scalar
#                      project_optimizer_rules_UpdateExpression
#                      project_utility_ContainerUtil
#                      project_utility_Macros
#                      project_utility_MemoryUtil)
#target_link_libraries(project_optimizer_Filter
#                      glog
#                      project_optimizer_AttributeReference
#                      project_optimizer_Plan
#                      project_optimizer_Predicate
#                      project_utility_Macros)
#target_link_libraries(project_optimizer_MultiwayCartesianJoin
#                      glog
#                      project_optimizer_AttributeReference
#                      project_optimizer_Plan
#                      project_utility_ContainerUtil
#                      project_utility_Macros
#                      project_utility_MemoryUtil)
#target_link_libraries(project_optimizer_MultiwayEquiJoinAggregate
#                      glog
#                      project_optimizer_AttributeReference
#                      project_optimizer_ExpressionUtil
#                      project_optimizer_Plan
#                      project_optimizer_Predicate
#                      project_optimizer_Scalar
#                      project_utility_ContainerUtil
#                      project_utility_Macros
#                      project_utility_MemoryUtil)
#target_link_libraries(project_optimizer_Optimizer
#                      ${GFLAGS_LIB_NAME}
#                      project_optimizer_Aggregate
#                      project_optimizer_AggregateFunction
#                      project_optimizer_Alias
#                      project_optimizer_AttributeReference
#                      project_optimizer_Comparison
#                      project_optimizer_Conjunction
#                      project_optimizer_ExecutionGenerator
#                      project_optimizer_ExpressionUtil
#                      project_optimizer_Filter
#                      project_optimizer_MultiwayCartesianJoin
#                      project_optimizer_OptimizerContext
#                      project_optimizer_Plan
#                      project_optimizer_Predicate
#                      project_optimizer_Project
#                      project_optimizer_QueryHandle
#                      project_optimizer_Scalar
#                      project_optimizer_ScalarLiteral
#                      project_optimizer_TableReference
#                      project_optimizer_TopLevelPlan
#                      project_optimizer_rules_CollapseSelection
#                      project_optimizer_rules_ConstantPropagation
#                      project_optimizer_rules_CreateTableViews
#                      project_optimizer_rules_EliminatePredicateLiteral
#                      project_optimizer_rules_FuseOperators
#                      project_optimizer_rules_GenerateJoins
#                      project_optimizer_rules_GenerateSelection
#                      project_optimizer_rules_LiftUpExtraEquiJoinPredicate
#                      project_optimizer_rules_PopulateFilterOnJoinAttributes
#                      project_optimizer_rules_PruneColumns
#                      project_optimizer_rules_PushDownFilter
#                      project_optimizer_rules_ReducePredicate
#                      project_optimizer_rules_ReduceSemiJoin
#                      project_optimizer_rules_ReorderJoins
#                      project_optimizer_rules_ResolveAlias
#                      project_optimizer_rules_ReuseAggregateExpressions
#                      project_optimizer_rules_Rule
#                      project_optimizer_rules_SpecializeChainedJoinAggregate
#                      project_optimizer_rules_SpecializeForeignKeyIndexJoinAggregate
#                      project_optimizer_rules_SpecializeMultiwayJoinAggregate
#                      project_optimizer_rules_SpecializePrimaryKeyIndexJoin
#                      project_optimizer_rules_SpecializePrimaryKeyIndexJoinAggregate
#                      project_optimizer_rules_SpecializePrimaryKeyScanJoinAggregate
#                      project_optimizer_rules_SpecializeSelection
#                      project_storage_Database
#                      project_utility_ExecutionDAGVisualizer
#                      project_utility_Macros
#                      project_utility_MemoryUtil
#                      project_utility_PlanVisualizer
#                      project_utility_StringUtil
#                      project_utility_SyncStream)
#target_link_libraries(project_optimizer_OptimizerContext
#                      project_optimizer_ExprId
#                      project_utility_Macros)
#target_link_libraries(project_optimizer_OptimizerTree
#                      project_utility_Macros
#                      project_utility_TreeStringSerializable)
#target_link_libraries(project_optimizer_PatternMatcher
#                      project_optimizer_Expression
#                      project_optimizer_Plan)
#target_link_libraries(project_optimizer_Plan
#                      project_optimizer_AttributeReference
#                      project_optimizer_ExprId
#                      project_optimizer_OptimizerTree
#                      project_utility_Macros
#                      project_utility_TreeStringSerializable)
#target_link_libraries(project_optimizer_Predicate
#                      glog
#                      project_optimizer_ExprId
#                      project_optimizer_Expression
#                      project_types_Type
#                      project_utility_Macros
#                      project_utility_Range)
#target_link_libraries(project_optimizer_PredicateLiteral
#                      glog
#                      project_operators_expressions_TruePredicate
#                      project_optimizer_AttributeReference
#                      project_optimizer_Predicate
#                      project_utility_Macros
#                      project_utility_Range)
#target_link_libraries(project_optimizer_Project
#                      glog
#                      project_optimizer_AttributeReference
#                      project_optimizer_Plan
#                      project_utility_Macros
#                      project_utility_MemoryUtil)
#target_link_libraries(project_optimizer_QueryHandle
#                      project_optimizer_ExecutionPlan
#                      project_utility_Macros)
#target_link_libraries(project_optimizer_Scalar
#                      glog
#                      project_optimizer_ExprId
#                      project_optimizer_Expression
#                      project_storage_StorageTypedefs
#                      project_utility_Macros)
#target_link_libraries(project_optimizer_ScalarLiteral
#                      glog
#                      project_operators_expressions_ScalarLiteral
#                      project_optimizer_AttributeReference
#                      project_optimizer_Scalar
#                      project_utility_Macros)
#target_link_libraries(project_optimizer_Selection
#                      glog
#                      project_optimizer_AttributeReference
#                      project_optimizer_ExpressionUtil
#                      project_optimizer_Plan
#                      project_optimizer_Predicate
#                      project_optimizer_Scalar
#                      project_utility_ContainerUtil
#                      project_utility_Macros
#                      project_utility_MemoryUtil)
#target_link_libraries(project_optimizer_TableReference
#                      glog
#                      project_optimizer_AttributeReference
#                      project_optimizer_OptimizerContext
#                      project_optimizer_Plan
#                      project_storage_Relation
#                      project_utility_Macros
#                      project_utility_MemoryUtil
#                      project_utility_StringUtil)
#target_link_libraries(project_optimizer_TableView
#                      glog
#                      project_optimizer_AttributeReference
#                      project_optimizer_Comparison
#                      project_optimizer_ExpressionUtil
#                      project_optimizer_Plan
#                      project_storage_AttributeStatistics
#                      project_utility_ContainerUtil
#                      project_utility_Macros
#                      project_utility_MemoryUtil)
#target_link_libraries(project_optimizer_TopLevelPlan
#                      glog
#                      project_optimizer_AttributeReference
#                      project_optimizer_Plan
#                      project_utility_Macros)
