# Declare micro-libs:
add_library(project_utility_meta_Common ../../empty_src.cpp Common.hpp)
add_library(project_utility_meta_FunctionTraits ../../empty_src.cpp FunctionTraits.hpp)
add_library(project_utility_meta_MultipleDispatcher ../../empty_src.cpp MultipleDispatcher.hpp)
add_library(project_utility_meta_TMP ../../empty_src.cpp TMP.hpp)
add_library(project_utility_meta_TypeList ../../empty_src.cpp TypeList.hpp)
add_library(project_utility_meta_TypeListMetaFunctions ../../empty_src.cpp TypeListMetaFunctions.hpp)

# Link dependencies:
target_link_libraries(project_utility_meta_MultipleDispatcher
                      glog
                      project_utility_meta_Common
                      project_utility_meta_TypeList)
target_link_libraries(project_utility_meta_TMP
                      project_utility_meta_Common
                      project_utility_meta_FunctionTraits
                      project_utility_meta_TypeList)
target_link_libraries(project_utility_meta_TypeList
                      project_utility_meta_Common
                      project_utility_meta_TypeListMetaFunctions)
target_link_libraries(project_utility_meta_TypeListMetaFunctions
                      project_utility_meta_Common)
