#include "tatp_mysql_benchmark.h"
#include "benchmarks/tatp/tatp_worker.h"
#include "tatp_mysql_connector.h"
#include "tatp_mysql_loader.h"

txbench::TATPMySQLBenchmark::TATPMySQLBenchmark(int n_workers,
                                                int warmup_duration,
                                                int measurement_duration,
                                                int n_rows)
    : Benchmark(n_workers, warmup_duration, measurement_duration),
      n_rows_(n_rows) {}

std::unique_ptr<txbench::Loader> txbench::TATPMySQLBenchmark::make_loader() {
  return std::make_unique<TATPMySQLLoader>(n_rows_, "localhost", 33060,
                                           "txbench");
}

std::unique_ptr<txbench::Worker> txbench::TATPMySQLBenchmark::make_worker() {
  auto connector =
      std::make_unique<TATPMySQLConnector>("localhost", 33060, "txbench");
  auto worker = std::make_unique<TATPWorker>(n_rows_, std::move(connector));
  return worker;
}
