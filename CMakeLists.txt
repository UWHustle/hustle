cmake_minimum_required(VERSION 3.15)
project(hustle)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
#list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
#find_package(Arrow REQUIRED)



#configure_file(experiments.csv ${CMAKE_CURRENT_BINARY_DIR} COPYONLY)


add_subdirectory(src)
add_subdirectory(third_party)