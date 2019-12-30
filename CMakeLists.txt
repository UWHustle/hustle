cmake_minimum_required(VERSION 3.10)
project(hustle)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

include_directories(third_party)
include_directories(src)

add_subdirectory(third_party)

#enable_testing()
add_subdirectory(src)
