add_executable(
        tatp_hustle
        tatp_hustle.cc
        tatp_hustle_benchmark.h tatp_hustle_benchmark.cc
        tatp_hustle_loader.h tatp_hustle_loader.cc
        tatp_hustle_connector.h tatp_hustle_connector.cc
)

target_include_directories(
        tatp_hustle
        PRIVATE
        ${CMAKE_SOURCE_DIR}
        hustle_src_catalog
        hustle_src_storage
        hustle_src_operators
        hustle_src_scheduler_Scheduler
        hustle_src_optimizer_ExecutionPlan
        hustle_src_utils_EventProfiler
        hustle_src_utils_skew
        hustle_src_utils_Config
)

target_link_libraries(
        tatp_hustle
        txbench
        tatp
	hustleDB
        hustle_src_catalog
        hustle_src_storage
        hustle_src_resolver
        hustle_src_operators
        hustle_src_scheduler_Scheduler
        hustle_src_optimizer_ExecutionPlan
        ${ARROW_SHARED_LIB}
        ${GFLAGS_LIB_NAME}
        glog
)
