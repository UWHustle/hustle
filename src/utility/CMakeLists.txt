add_subdirectory(meta)
#add_subdirectory(specializations)

# Declare micro-libs:
#add_library(project_utility_BitManipulation ../empty_src.cpp BitManipulation.hpp)
#add_library(project_utility_BitVector ../empty_src.cpp BitVector.hpp)
#add_library(project_utility_ConcurrentBitVector ../empty_src.cpp ConcurrentBitVector.hpp)
add_library(project_utility_ContainerUtil ../empty_src.cpp ContainerUtil.hpp)
#add_library(project_utility_DisjointSetForest ../empty_src.cpp DisjointSetForest.hpp)
#add_library(project_utility_EventProfiler EventProfiler.cpp EventProfiler.hpp)
#add_library(project_utility_ExecutionDAGVisualizer ExecutionDAGVisualizer.cpp ExecutionDAGVisualizer.hpp)
#add_library(project_utility_HashFilter ../empty_src.cpp HashFilter.hpp)
#add_library(project_utility_HashPair ../empty_src.cpp HashPair.hpp)
#add_library(project_utility_HashTable ../empty_src.cpp HashTable.hpp)
add_library(project_utility_Macros ../empty_src.cpp Macros.hpp)
#add_library(project_utility_MemoryUtil ../empty_src.cpp MemoryUtil.hpp)
#add_library(project_utility_MultiwayParallelMergeSort ../empty_src.cpp MultiwayParallelMergeSort.hpp)
#add_library(project_utility_NumberUtil ../empty_src.cpp NumberUtil.hpp)
#add_library(project_utility_NumericGenerator ../empty_src.cpp NumericGenerator.hpp)
#add_library(project_utility_PlanVisualizer PlanVisualizer.cpp PlanVisualizer.hpp)
#add_library(project_utility_Range ../empty_src.cpp Range.hpp)
#add_library(project_utility_RowStoreBlockSorter RowStoreBlockSorter.cpp RowStoreBlockSorter.hpp)
#add_library(project_utility_ScopedArray ../empty_src.cpp ScopedArray.hpp)
#add_library(project_utility_ScopedBuffer ../empty_src.cpp ScopedBuffer.hpp)
#add_library(project_utility_StringUtil ../empty_src.cpp StringUtil.hpp)
#add_library(project_utility_SyncStream ../empty_src.cpp SyncStream.hpp)
#add_library(project_utility_ThreadSafePool ../empty_src.cpp ThreadSafePool.hpp)
add_library(project_utility_ThreadSafeQueue ../empty_src.cpp ThreadSafeQueue.hpp)
#add_library(project_utility_TreeStringSerializable ../empty_src.cpp TreeStringSerializable.hpp)

# Link dependencies:
#target_link_libraries(project_utility_BitVector
#                      glog
#                      project_utility_BitManipulation
#                      project_utility_Macros)
#target_link_libraries(project_utility_ConcurrentBitVector
#                      glog
#                      project_utility_BitManipulation
#                      project_utility_Macros)
#target_link_libraries(project_utility_DisjointSetForest
#                      glog
#                      project_utility_Macros)
#target_link_libraries(project_utility_EventProfiler
#                      glog
#                      project_threading_Mutex
#                      project_utility_Macros)
#target_link_libraries(project_utility_ExecutionDAGVisualizer
#                      project_operators_AggregateOperator
#                      project_operators_DropTableOperator
#                      project_operators_PrintOperator
#                      project_operators_SelectOperator
#                      project_optimizer_ExecutionPlan
#                      project_utility_Macros
#                      project_utility_StringUtil)
#target_link_libraries(project_utility_HashFilter
#                      glog
#                      project_utility_BitVector
#                      project_utility_ConcurrentBitVector
#                      project_utility_Macros)
#target_link_libraries(project_utility_HashTable
#                      project_utility_Macros)
#target_link_libraries(project_utility_MultiwayParallelMergeSort
#                      glog
#                      project_scheduler_Task
#                      project_utility_BitManipulation
#                      project_utility_Macros
#                      project_utility_Range)
#target_link_libraries(project_utility_NumberUtil
#                      glog
#                      project_utility_BitManipulation)
#target_link_libraries(project_utility_PlanVisualizer
#                      glog
#                      project_optimizer_Aggregate
#                      project_optimizer_AttributeReference
#                      project_optimizer_ChainedJoinAggregate
#                      project_optimizer_EquiJoin
#                      project_optimizer_EquiJoinAggregate
#                      project_optimizer_ExprId
#                      project_optimizer_MultiwayEquiJoinAggregate
#                      project_optimizer_Plan
#                      project_optimizer_Scalar
#                      project_optimizer_Selection
#                      project_optimizer_TableReference
#                      project_optimizer_TableView
#                      project_optimizer_cost_SimpleCostModel
#                      project_storage_Relation
#                      project_utility_Macros
#                      project_utility_Range
#                      project_utility_StringUtil)
#target_link_libraries(project_utility_Range
#                      glog
#                      project_utility_Macros
#                      project_utility_NumericGenerator)
#target_link_libraries(project_utility_RowStoreBlockSorter
#                      glog
#                      project_storage_RowStoreBlock
#                      project_types_Type
#                      project_utility_specializations_RowStoreBlockSorter01
#                      project_utility_specializations_RowStoreBlockSorter02
#                      project_utility_specializations_RowStoreBlockSorter03)
#target_link_libraries(project_utility_ScopedArray
#                      project_utility_Macros
#                      project_utility_ScopedBuffer)
#target_link_libraries(project_utility_ScopedBuffer
#                      glog
#                      project_utility_Macros)
#target_link_libraries(project_utility_SyncStream
#                      project_utility_Macros)
#target_link_libraries(project_utility_ThreadSafePool
#                      project_utility_Macros)
target_link_libraries(project_utility_ThreadSafeQueue
                      project_utility_Macros)
#target_link_libraries(project_utility_TreeStringSerializable
#                      glog
#                      project_utility_Macros)
