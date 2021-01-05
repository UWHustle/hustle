#ifndef TXBENCH__TATP_WORKER_H
#define TXBENCH__TATP_WORKER_H

#include <memory>
#include <random>

#include "tatp_connector.h"
#include "txbench/random_generator.h"
#include "txbench/worker.h"

namespace txbench {

class TATPWorker : public Worker {
 public:
  explicit TATPWorker(int n_rows, std::unique_ptr<TATPConnector> connector);

  void run(std::atomic_bool& terminate, std::atomic_int& commit_count) override;

  void print(int commit_count) override;

  double getTime(int query_id) override {
    return (1.0 * query_time_[query_id]) / s_trans_count_[query_id];
  }

  ~TATPWorker() {
    free(query_time_);
    free(s_trans_count_);
  }

 private:
  int n_rows_;

  int a_val_;

  int w_trans_count;
  int* s_trans_count_;

  double* query_time_;

  std::unique_ptr<TATPConnector> connector_;

  RandomGenerator rg_;
};

}  // namespace txbench

#endif  // TXBENCH__TATP_WORKER_H
