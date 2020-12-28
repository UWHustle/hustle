#ifndef TXBENCH_BENCHMARKS_BENCHMARK_H_
#define TXBENCH_BENCHMARKS_BENCHMARK_H_

#include "loader.h"
#include "worker.h"
#include <memory>
#include <vector>

namespace txbench {

class Benchmark {
public:
  Benchmark(int n_workers, int warmup_duration, int measurement_duration);

  double run();

protected:
  virtual std::unique_ptr<txbench::Loader> make_loader() = 0;

  virtual std::unique_ptr<txbench::Worker> make_worker() = 0;

private:
  int n_workers_;

  int warmup_duration_;

  int measurement_duration_;
};

} // namespace txbench

#endif // TXBENCH_BENCHMARKS_BENCHMARK_H_
