# Declare micro-libs:
add_library(hustle_src_utils_arrow_hacks vector_selection.cc)

# Include Arrow
target_include_directories(hustle_src_utils_arrow_hacks PUBLIC ${ARROW_INCLUDE_DIR})

# Link dependencies:
target_link_libraries(hustle_src_utils_arrow_hacks ${ARROW_SHARED_LIB})
