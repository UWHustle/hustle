//
// Created by Nicholas Corrado on 8/7/20.
//

#ifndef HUSTLE_XORFILTER_WRAPPER_H
#define HUSTLE_XORFILTER_WRAPPER_H

#include <stdexcept>

#include "xorfilter.h"

class Xor8 {
 public:
  explicit Xor8(const size_t size) {
    if (!xor8_allocate(size, &filter)) {
      throw ::std::runtime_error("Allocation failed");
    }
  }
  ~Xor8() { xor8_free(&filter); }

  // if it returns true, check for duplicate keys in data
  bool AddAll(const uint64_t* data, const size_t start, const size_t end) {
    return xor8_buffered_populate(data + start, end - start, &filter);
  }
  inline bool Contain(uint64_t& item) const {
    return xor8_contain(item, &filter);
  }
  inline size_t SizeInBytes() const { return xor8_size_in_bytes(&filter); }
  Xor8(Xor8&& o) : filter(o.filter) {
    o.filter.fingerprints = nullptr;  // we take ownership for the data
  }
  xor8_s filter;

 private:
  Xor8(const Xor8& o) = delete;
};

#endif  // HUSTLE_XORFILTER_WRAPPER_H
