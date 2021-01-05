#include "random_generator.h"

int txbench::RandomGenerator::random_int(int a, int b) {
  std::uniform_int_distribution<int> dis(a, b);
  return dis(mt_);
}

bool txbench::RandomGenerator::random_bool() { return random_int(0, 1); }
