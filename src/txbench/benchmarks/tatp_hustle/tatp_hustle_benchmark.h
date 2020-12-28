#ifndef TXBENCH_BENCHMARKS_TATP_HUSTLE_BENCHMARK_H_
#define TXBENCH_BENCHMARKS_TATP_HUSTLE_BENCHMARK_H_

#include "txbench/benchmark.h"
#include "txbench/loader.h"
#include "api/hustle_db.h"
#include <memory>

namespace txbench {

class TATPHustleBenchmark : public Benchmark {
public:
  TATPHustleBenchmark(int n_workers, int warmup_duration,
                     int measurement_duration, int n_rows);
  static std::shared_ptr<hustle::HustleDB> getHustleDB() { 
    if (hustle_db == nullptr) {
       hustle_db = std::make_shared<hustle::HustleDB>("db_directory2");
       std::cout << "create hustldb" << std::endl;
    }
    std::cout << hustle_db << std::endl;
    return hustle_db; 
    }

~TATPHustleBenchmark() {
  std::cout << "Stop scheduler" << std::endl;
  hustle::HustleDB::stopScheduler();
}
private:
  int n_rows_;

  static std::shared_ptr<hustle::HustleDB> hustle_db;

  std::unique_ptr<Loader> make_loader() override;

  std::unique_ptr<Worker> make_worker() override;
};

} // namespace txbench

#endif // TXBENCH_BENCHMARKS_TATP_Hustle_BENCHMARK_H_
