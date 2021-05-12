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

  std::vector<std::unique_ptr<std::atomic<double>>> query_time;
  std::vector<std::unique_ptr<std::atomic<int>>> query_count;
  query_time.resize(NUM_QUERIES);
  query_count.resize(NUM_QUERIES);
  for (auto& query_time_ptr : query_time) {
    query_time_ptr = std::make_unique<std::atomic<double>>(0.0);   // init atomic ints to 0.0
  }
  for (auto& query_count_ptr : query_count) {
      query_count_ptr = std::make_unique<std::atomic<int>>(0);   // init atomic ints to 0
  }
  int worker_count = 0;
  for (const auto &worker : workers) {
    threads.emplace_back([&] { 
      worker->run(terminate, transaction_count); 
      for (int i = 0; i < NUM_QUERIES; i++) {
        //if (worker->getTime(i) > 0.000000) {
          *query_time[i] = *query_time[i] + worker->getTime(i);
          *query_count[i] = *query_count[i] + worker->getCount(i);
        //}
      }
    });
  }
  
  std::this_thread::sleep_for(std::chrono::seconds(warmup_duration_));

  int warmup_transaction_count = transaction_count;

  std::this_thread::sleep_for(std::chrono::seconds(measurement_duration_));

  int total_transaction_count = transaction_count;

  terminate = true;
  for (auto &thread : threads) {
    thread.join();
  }

  for (int i = 0; i < NUM_QUERIES; i++) {
     std::cout << "Query "<<i<<" Time: " << 1.0 * (*query_time[i]) / n_workers_ << std::endl;
  }
    for (int i = 0; i < NUM_QUERIES; i++) {
        std::cout << "Query "<<i<<" Count: " <<  (*query_count[i]) << std::endl;
    }
  int measurement_transaction_count =
      total_transaction_count - warmup_transaction_count;
  double tps = (double)measurement_transaction_count / measurement_duration_;
  return tps;
}
