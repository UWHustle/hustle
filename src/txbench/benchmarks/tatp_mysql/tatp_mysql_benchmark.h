#ifndef TXBENCH_BENCHMARKS_TATP_MYSQL_TATP_MYSQL_BENCHMARK_H_
#define TXBENCH_BENCHMARKS_TATP_MYSQL_TATP_MYSQL_BENCHMARK_H_

#include "benchmark.h"
#include "loader.h"

namespace txbench {

class TATPMySQLBenchmark : public Benchmark {
public:
  TATPMySQLBenchmark(int n_workers, int warmup_duration,
                     int measurement_duration, int n_rows);

private:
  int n_rows_;

  std::unique_ptr<Loader> make_loader() override;

  std::unique_ptr<Worker> make_worker() override;
};

} // namespace txbench

#endif // TXBENCH_BENCHMARKS_TATP_MYSQL_TATP_MYSQL_BENCHMARK_H_
