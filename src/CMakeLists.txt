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
        catalog
        table
        parser
        resolver
        project_scheduler_Scheduler
        ${GFLAGS_LIB_NAME}
        glog
        )

macro (set_gflags_lib_name)
    if (BUILD_SHARED_LIBS)
        set(GFLAGS_LIB_NAME gflags_nothreads-shared)
    else()
        set(GFLAGS_LIB_NAME gflags_nothreads-static)
    endif()
endmacro (set_gflags_lib_name)

set_gflags_lib_name ()

add_subdirectory(utils)
add_subdirectory(catalog)
add_subdirectory(api)
add_subdirectory(parser)
add_subdirectory(resolver)
add_subdirectory(table)
add_subdirectory(operators)
add_subdirectory(scheduler)
add_subdirectory(utility)
add_subdirectory(threading)
add_subdirectory(execution)

