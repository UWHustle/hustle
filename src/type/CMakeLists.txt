
add_library(
        hustle_src_type
        type_helper.h type_helper.cc)
target_include_directories(hustle_src_type PUBLIC ${ARROW_INCLUDE_DIR})
target_link_libraries(hustle_src_type ${ARROW_SHARED_LIB})
