#ifndef TXBENCH__TATP_WORKER_H
#define TXBENCH__TATP_WORKER_H

#include "txbench/random_generator.h"
#include "tatp_connector.h"
#include "txbench/worker.h"
#include <memory>
#include <random>

namespace txbench {

class TATPWorker : public Worker {
public:
  explicit TATPWorker(int n_rows, std::unique_ptr<TATPConnector> connector);

  void run(std::atomic_bool &terminate, std::atomic_int &commit_count) override;

private:
  int n_rows_;

  int a_val_;

  std::unique_ptr<TATPConnector> connector_;

  RandomGenerator rg_;
};

} // namespace txbench

#endif // TXBENCH__TATP_WORKER_H
