add_library(
        tatp
        tatp_connector.h
        tatp_worker.h tatp_worker.cpp
        tatp_util.h tatp_util.cpp
)

target_include_directories(
        tatp
        PRIVATE
        ${CMAKE_SOURCE_DIR}
)

target_link_libraries(
        tatp
        txbench
)
