# Declare micro-libs:
add_library(hustle_src_utils_meta_Common ../../empty_src.cpp Common.hpp)
add_library(hustle_src_utils_meta_FunctionTraits ../../empty_src.cpp FunctionTraits.hpp)
add_library(hustle_src_utils_meta_MultipleDispatcher ../../empty_src.cpp MultipleDispatcher.hpp)
add_library(hustle_src_utils_meta_TMP ../../empty_src.cpp TMP.hpp)
add_library(hustle_src_utils_meta_TypeList ../../empty_src.cpp TypeList.hpp)
add_library(hustle_src_utils_meta_TypeListMetaFunctions ../../empty_src.cpp TypeListMetaFunctions.hpp)

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
