#ifndef TXBENCH_BENCHMARKS_TATP_TATP_UTIL_H_
#define TXBENCH_BENCHMARKS_TATP_TATP_UTIL_H_

#include "txbench/random_generator.h"
#include <string>

namespace txbench {

std::string leading_zero_pad(int length, const std::string &s);

std::string uppercase_string(int length, RandomGenerator &rg);

}

#endif // TXBENCH_BENCHMARKS_TATP_TATP_UTIL_H_
