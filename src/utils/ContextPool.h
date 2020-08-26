//
// Created by Nicholas Corrado on 6/21/20.
//

#ifndef HUSTLE_CONTEXT_H
#define HUSTLE_CONTEXT_H

#include <utils/arrow_compute_wrappers.h>

#include <queue>

namespace hustle {

class ContextPool {
 public:
  ContextPool();
  Context get_context();
  Context return_context(Context context);

 private:
  std::queue<Context> contexts_;
  std::mutex mutex_;
};

}  // namespace hustle

#endif  // HUSTLE_CONTEXT_H
