#ifndef TXBENCH__WORKER_H
#define TXBENCH__WORKER_H

#include <atomic>

namespace txbench {

class Worker {
public:
  virtual ~Worker() = default;

  virtual void run(std::atomic_bool &terminate,
                   std::atomic_int &commit_count) = 0;
};

} // namespace txbench

#endif // TXBENCH__WORKER_H
