#include "tatp_hustle_benchmark.h"
#include "txbench/benchmarks/tatp/tatp_worker.h"
#include "tatp_hustle_connector.h"
#include "tatp_hustle_loader.h"

std::shared_ptr<hustle::HustleDB> txbench::TATPHustleBenchmark::hustle_db {nullptr};
txbench::TATPHustleBenchmark::TATPHustleBenchmark(int n_workers,
                                                int warmup_duration,
                                                int measurement_duration,
                                                int n_rows)
    : Benchmark(n_workers, warmup_duration, measurement_duration),
      n_rows_(n_rows) {
         std::cout << "Start scheduler" << std::endl;
    hustle::HustleDB::init();
      }


std::unique_ptr<txbench::Loader> txbench::TATPHustleBenchmark::make_loader() {
  return std::make_unique<TATPHustleLoader>(n_rows_, "localhost", 33060,
                                           "txbench");
}

std::unique_ptr<txbench::Worker> txbench::TATPHustleBenchmark::make_worker() {
  auto connector =
      std::make_unique<TATPHustleConnector>("localhost", 33060, "txbench");
  auto worker = std::make_unique<TATPWorker>(n_rows_, std::move(connector));
  return worker;
}
