set(CMAKE_OSX_DEPLOYMENT_TARGET "10.15" CACHE STRING "Minimum OS X deployment version")
#set(CMAKE_OSX_DEPLOYMENT_TARGET "11.1" CACHE STRING "Minimum OS X deployment version") # Pass in Big Sur
cmake_minimum_required(VERSION 3.10)
project(hustle)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

include_directories(third_party)
include_directories(src)

enable_testing()
add_subdirectory(third_party)
add_subdirectory(src)