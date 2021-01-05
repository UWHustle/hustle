add_executable(
        tatp_mysql
        tatp_mysql.cpp
        tatp_mysql_benchmark.h tatp_mysql_benchmark.cpp
        tatp_mysql_loader.h tatp_mysql_loader.cpp
        tatp_mysql_connector.h tatp_mysql_connector.cpp
)

target_include_directories(
        tatp_mysql
        PRIVATE
        ${CMAKE_SOURCE_DIR}
        /usr/include/mysql-cppconn-8
)

target_link_libraries(
        tatp_mysql
        txbench
        tatp
        /usr/lib/x86_64-linux-gnu/libmysqlcppconn8.so.2.8.0.19
)
