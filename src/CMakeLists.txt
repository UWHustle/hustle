macro (set_gflags_lib_name)
    if (BUILD_SHARED_LIBS)
        set(GFLAGS_LIB_NAME gflags_nothreads-shared)
    else()
        set(GFLAGS_LIB_NAME gflags_nothreads-static)
    endif()
endmacro (set_gflags_lib_name)

set_gflags_lib_name ()

set(THIRD_PARTY_SOURCE_DIR "${PROJECT_SOURCE_DIR}/third_party")

add_subdirectory ("${THIRD_PARTY_SOURCE_DIR}/gflags" "${CMAKE_CURRENT_BINARY_DIR}/third_party/gflags")
include_directories("${CMAKE_CURRENT_BINARY_DIR}/third_party/gflags/include")

add_subdirectory("${THIRD_PARTY_SOURCE_DIR}/glog" "${CMAKE_CURRENT_BINARY_DIR}/third_party/glog")
include_directories(${THIRD_PARTY_SOURCE_DIR}/glog/src)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/third_party)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}")
find_package(Arrow REQUIRED)

add_executable(
        hustle_main
        main.cpp
)

set_target_properties(hustle_main
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

target_link_libraries(hustle_main
        hustleDB
        hustle_src_catalog
        hustle_src_table
        hustle_src_parser
        hustle_src_resolver
        hustle_src_operators
        hustle_src_scheduler_Scheduler
        hustle_src_optimizer_ExecutionPlan
        ${ARROW_SHARED_LIB}
        ${GFLAGS_LIB_NAME}
        glog
        )


add_subdirectory(utils)
add_subdirectory(catalog)
add_subdirectory(api)
add_subdirectory(parser)
add_subdirectory(resolver)
add_subdirectory(table)
add_subdirectory(operators)
add_subdirectory(scheduler)
add_subdirectory(threading)
add_subdirectory(execution)
if(INCLUDE_BITWEAVING)
    add_subdirectory(bitweaving)
    add_definitions(-DUSE_BITWEAVING=1)
else()
    add_definitions(-DUSE_BITWEAVING=0)
endif()
