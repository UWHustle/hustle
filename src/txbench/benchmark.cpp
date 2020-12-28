#include "benchmark.h"
#include "loader.h"
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

txbench::Benchmark::Benchmark(int n_workers, int warmup_duration,
                              int measurement_duration)
    : n_workers_(n_workers), warmup_duration_(warmup_duration),
      measurement_duration_(measurement_duration) {}

double txbench::Benchmark::run() {
  std::unique_ptr<txbench::Loader> loader = make_loader();
  loader->load();

  std::vector<std::unique_ptr<txbench::Worker>> workers;
  workers.reserve(n_workers_);

  for (int i = 0; i < n_workers_; ++i) {
    std::unique_ptr<txbench::Worker> worker = make_worker();
    workers.emplace_back(std::move(worker));
  }

  std::vector<std::thread> threads;
  threads.reserve(n_workers_);

  std::atomic_bool terminate = false;
  std::atomic_int transaction_count = 0;

  for (const auto &worker : workers) {
    threads.emplace_back([&] { worker->run(terminate, transaction_count); });
  }

  std::this_thread::sleep_for(std::chrono::seconds(warmup_duration_));

  int warmup_transaction_count = transaction_count;

  std::this_thread::sleep_for(std::chrono::seconds(measurement_duration_));

  int total_transaction_count = transaction_count;

  terminate = true;
  for (auto &thread : threads) {
    thread.join();
  }

  int measurement_transaction_count =
      total_transaction_count - warmup_transaction_count;
  double tps = (double)measurement_transaction_count / measurement_duration_;
  return tps;
}
