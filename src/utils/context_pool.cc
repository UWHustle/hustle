//
// Created by Nicholas Corrado on 6/21/20.
//

#include "context_pool.h"

namespace hustle {

ContextPool::ContextPool() {}

Context ContextPool::get_context() {
  std::scoped_lock lock(mutex_);
  Context context;

  if (!contexts_.empty()) {
    context = contexts_.back();
  }
  return context;
}

Context ContextPool::return_context(Context context) {
  std::scoped_lock lock(mutex_);
  contexts_.push(context);
}

}  // namespace hustle
