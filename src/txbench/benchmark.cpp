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

  std::atomic<double>	 time_1 = 0.0, time_2 = 0.0, time_3 = 0.0, time_4 = 0.0, time_5 = 0.0, time_6 = 0.0, time_7 = 0.0;
  for (const auto &worker : workers) {
    threads.emplace_back([&] { 
      worker->run(terminate, transaction_count); 
      time_1 = time_1 +  worker->getTime1();
      time_2 = time_2 +  worker->getTime2();
      time_3 = time_3 +  worker->getTime3();
      time_4 = time_4 +  worker->getTime4();
      time_5 = time_5 +  worker->getTime5();
      time_6 = time_6 +  worker->getTime6();
      time_7 = time_7 +  worker->getTime7();
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

  std::cout << "Query 1 Time: " << 1.0 * time_1 / n_workers_ << std::endl;
  std::cout << "Query 2 Time: " << 1.0 * time_2 / n_workers_ << std::endl;
  std::cout << "Query 3 Time: " << 1.0 * time_3 / n_workers_ << std::endl;
  std::cout << "Query 4 Time: " << 1.0 * time_4 / n_workers_ << std::endl;
  std::cout << "Query 5 Time: " << 1.0 * time_5 / n_workers_ << std::endl;
  std::cout << "Query 6 Time: " << 1.0 * time_6 / n_workers_ << std::endl;
  std::cout << "Query 7 Time: " << 1.0 * time_7 / n_workers_ << std::endl;
  int measurement_transaction_count =
      total_transaction_count - warmup_transaction_count;
  double tps = (double)measurement_transaction_count / measurement_duration_;
  return tps;
}
