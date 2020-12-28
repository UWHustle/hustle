// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef HUSTLE_BIT_UTILS_H
#define HUSTLE_BIT_UTILS_H

#include <arrow/api.h>

#include <memory>

namespace hustle {
namespace utils {

static void pack(int64_t length, const uint8_t *arr,
                 std::shared_ptr<arrow::BooleanArray> *out) {
  auto packed = std::static_pointer_cast<arrow::BooleanArray>(
      arrow::MakeArrayFromScalar(arrow::BooleanScalar(false), length)
          .ValueOrDie());

  uint8_t *packed_arr = packed->values()->mutable_data();
  uint8_t current_byte;

  const uint8_t *src_byte = arr;
  uint8_t *dst_byte = packed_arr;

  int64_t remaining_bytes = length / 8;
  while (remaining_bytes-- > 0) {
    current_byte = 0u;
    current_byte = *src_byte++ ? current_byte | 0x01u : current_byte;
    current_byte = *src_byte++ ? current_byte | 0x02u : current_byte;
    current_byte = *src_byte++ ? current_byte | 0x04u : current_byte;
    current_byte = *src_byte++ ? current_byte | 0x08u : current_byte;
    current_byte = *src_byte++ ? current_byte | 0x10u : current_byte;
    current_byte = *src_byte++ ? current_byte | 0x20u : current_byte;
    current_byte = *src_byte++ ? current_byte | 0x40u : current_byte;
    current_byte = *src_byte++ ? current_byte | 0x80u : current_byte;
    *dst_byte++ = current_byte;
  }

  int64_t remaining_bits = length % 8;
  if (remaining_bits) {
    current_byte = 0;
    uint8_t bit_mask = 0x01;
    while (remaining_bits-- > 0) {
      current_byte = *src_byte++ ? current_byte | bit_mask : current_byte;
      bit_mask = bit_mask << 1u;
    }
    *dst_byte++ = current_byte;
  }

  *out = packed;
}

static uint8_t* reverse_bytes(uint8_t* arr, int num_bytes) {
    if (num_bytes == 0) return arr;
    uint8_t temp_value;
    uint32_t start_index = 0, end_index = num_bytes - 1;
    while (start_index < end_index) {
        temp_value = arr[start_index];   
        arr[start_index] = arr[end_index];
        arr[end_index] = temp_value;
        start_index++;
        end_index--;
    }   
    return arr;
}     

}  // namespace utils
}  // namespace hustle

#endif  // HUSTLE_BIT_UTILS_H
