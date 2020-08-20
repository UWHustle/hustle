set(CMAKE_OSX_DEPLOYMENT_TARGET "10.15" CACHE STRING "Minimum OS X deployment version")
cmake_minimum_required(VERSION 3.10)
project(hustle)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

OPTION(INCLUDE_BITWEAVING "Option to build and use bitweaving for predicate evaluation" ON)

find_package(Threads REQUIRED)

include_directories(third_party)
include_directories(src)

enable_testing()
add_subdirectory(third_party)
add_subdirectory(src)