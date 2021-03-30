set(CMAKE_OSX_DEPLOYMENT_TARGET "10.15" CACHE STRING "Minimum OS X deployment version")
cmake_minimum_required(VERSION 3.10)
project(hustle)

# Check CXX Version.
# CXX Version can only be 17 or 20
if (NOT DEFINED CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD "17")
    message(STATUS "Set default CMAKE_CXX_STANDARD to 17")
endif ()


if (${CMAKE_CXX_STANDARD} STREQUAL "17")
    message(STATUS "Compile with C++17 standard")
elseif (${CMAKE_CXX_STANDARD} STREQUAL "20")
    message(STATUS "Compile with C++20 standard.")
    message(WARNING "Notice: Unit test `hustle_src_util` does not compile under g++ -std=c++2a. \
     We temporarily disabled this test in order to pass the target build.")
else ()
    error("CMAKE_CXX_STANDARD must be either 17 or 20.")
endif ()

# Import Subdirectories.
find_package(Threads REQUIRED)
execute_process(COMMAND python ${CMAKE_CURRENT_LIST_DIR}/test/data/test_generate_data.py
        WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/test/data/)

include_directories(third_party)
include_directories(src)

enable_testing()
add_subdirectory(third_party)
add_subdirectory(src)