# Declare micro-libs:
add_library(hustle_src_utils_meta_Common ../../empty_src.cc common.h)
add_library(hustle_src_utils_meta_FunctionTraits ../../empty_src.cc function_traits.h)
add_library(hustle_src_utils_meta_MultipleDispatcher ../../empty_src.cc multiple_dispatcher.h)
add_library(hustle_src_utils_meta_TMP ../../empty_src.cc tmp.h)
add_library(hustle_src_utils_meta_TypeList ../../empty_src.cc type_list.h)
add_library(hustle_src_utils_meta_TypeListMetaFunctions ../../empty_src.cc type_list_meta_functions.h)

# Link dependencies:
target_link_libraries(hustle_src_utils_meta_MultipleDispatcher
                      glog
                      hustle_src_utils_meta_Common
                      hustle_src_utils_meta_TypeList)
target_link_libraries(hustle_src_utils_meta_TMP
                      hustle_src_utils_meta_Common
                      hustle_src_utils_meta_FunctionTraits
                      hustle_src_utils_meta_TypeList)
target_link_libraries(hustle_src_utils_meta_TypeList
                      hustle_src_utils_meta_Common
                      hustle_src_utils_meta_TypeListMetaFunctions)
target_link_libraries(hustle_src_utils_meta_TypeListMetaFunctions
                      hustle_src_utils_meta_Common)
