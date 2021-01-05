#include "tatp_util.h"

#include <algorithm>
#include <cassert>

std::string txbench::leading_zero_pad(int length, const std::string &s) {
  assert(length >= s.length());
  return std::string(length - s.length(), '0') + s;
}

std::string txbench::uppercase_string(int length, RandomGenerator &rg) {
  static std::string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  static std::uniform_int_distribution<int> dis(0, chars.size() - 1);

  std::string s(length, 0);
  std::generate(s.begin(), s.end(),
                [&] { return chars[rg.random_int(0, chars.size() - 1)]; });

  return s;
}
