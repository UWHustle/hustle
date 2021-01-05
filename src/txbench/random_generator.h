#ifndef TXBENCH__RANDOM_GENERATOR_H_
#define TXBENCH__RANDOM_GENERATOR_H_

#include <random>

namespace txbench {

class RandomGenerator {
public:
  int random_int(int a, int b);

  bool random_bool();

private:
  std::random_device rd_;

  std::mt19937 mt_;
};

} // namespace txbench

#endif // TXBENCH__RANDOM_GENERATOR_H_
