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

  void print(int commit_count) override;

  double getTime1() override { return (1.0*time_1_)/s_trans_count; }
  
  double getTime2() override { return (1.0*time_2_)/s_trans_count; }
  
  double getTime3() override { return (1.0*time_3_)/s_trans_count; }
  
  double getTime4() override { return (1.0*time_4_)/s_trans_count; }

  double getTime5() override { return (1.0*time_5_)/s_trans_count; }

  double getTime6() override { return (1.0*time_6_)/s_trans_count; }

  double getTime7() override { return (1.0*time_7_)/s_trans_count; }

private:
  int n_rows_;

  int a_val_;

  int w_trans_count;
  int s_trans_count;

  double time_1_, time_2_, time_3_, time_4_, time_5_, time_6_, time_7_;

  std::unique_ptr<TATPConnector> connector_;

  RandomGenerator rg_;
};

} // namespace txbench

#endif // TXBENCH__TATP_WORKER_H
