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

#include "block.h"

#include <arrow/scalar.h>

#include <iostream>
#include <vector>

#include "absl/strings/numbers.h"
#include "util.h"

#define ESTIMATED_STR_LEN 30

namespace hustle::storage {

Block::Block(int id, const std::shared_ptr<arrow::Schema> &in_schema,
             int capacity)
    : num_rows(0), num_bytes(0), capacity(capacity), id(id), schema(in_schema) {
  arrow::Status status;

  num_cols = schema->num_fields();

  int fixed_record_width = compute_fixed_record_width(schema);
  field_sizes_ = get_field_sizes(schema);
  int num_string_cols = 0;
  for (const auto &field : schema->fields()) {
    if (field->type()->id() == arrow::Type::STRING) {
      num_string_cols++;
    }
  }

  // Estimated number of tuples that will fit in the block, assuming that
  // strings are on average ESTIMATED_STR_LEN characters long.
  int init_rows =
      capacity / (fixed_record_width + ESTIMATED_STR_LEN * num_string_cols);

  // Initialize valid column separately
  auto result = arrow::AllocateResizableBuffer(0);
  evaluate_status(result.status(), __FUNCTION__, __LINE__);
  std::shared_ptr<arrow::ResizableBuffer> valid_buffer =
      std::move(result.ValueOrDie());
  valid = arrow::ArrayData::Make(arrow::boolean(), 0, {nullptr, valid_buffer});

  for (const auto &field : schema->fields()) {
    column_sizes.push_back(0);
    // Empty ArrayData should be constructed using the constructor that
    // accepts buffers as parameters. Other constructors will initialize
    // empty ArrayData (and Array) with nullptrs, making it impossible to
    // insert data.
    std::shared_ptr<arrow::ArrayData> array_data;
    std::shared_ptr<arrow::ResizableBuffer> data;

    switch (field->type()->id()) {
      case arrow::Type::STRING: {
        // Although the data buffer is empty, the offsets buffer should
        // still contain the offset of the first element.
        result = arrow::AllocateResizableBuffer(sizeof(int32_t) * init_rows);
        evaluate_status(result.status(), __FUNCTION__, __LINE__);
        std::shared_ptr<arrow::ResizableBuffer> offsets =
            std::move(result.ValueOrDie());

        // Make sure the first offset value is set to 0
        int32_t initial_offset = 0;
        uint8_t *offsets_data = offsets->mutable_data();
        std::memcpy(&offsets_data[0], &initial_offset, sizeof(initial_offset));

        result = arrow::AllocateResizableBuffer(ESTIMATED_STR_LEN * init_rows);
        evaluate_status(result.status(), __FUNCTION__, __LINE__);
        data = std::move(result.ValueOrDie());
        data->ZeroPadding();

        // Initialize null bitmap buffer to nullptr, since we currently don't
        // use it.
        columns.push_back(
            arrow::ArrayData::Make(field->type(), 0, {nullptr, offsets, data}));
        break;
      }
      case arrow::Type::FIXED_SIZE_BINARY: {
        auto field_size = field->type()->layout().FixedWidth(1).byte_width;

        result = arrow::AllocateResizableBuffer(field_size * init_rows);
        evaluate_status(result.status(), __FUNCTION__, __LINE__);
        data = std::move(result.ValueOrDie());
        data->ZeroPadding();

        columns.push_back(
            arrow::ArrayData::Make(field->type(), 0, {nullptr, data}));
        break;
      }
      case arrow::Type::DOUBLE:
      case arrow::Type::INT64: {
        columns.push_back(
            allocate_column_data<int64_t>(field->type(), init_rows));
        break;
      }
      case arrow::Type::UINT32: {
        columns.push_back(
            allocate_column_data<uint32_t>(field->type(), init_rows));
        break;
      }
      case arrow::Type::UINT16: {
        columns.push_back(
            allocate_column_data<uint16_t>(field->type(), init_rows));
        break;
      }
      case arrow::Type::UINT8: {
        columns.push_back(
            allocate_column_data<uint8_t>(field->type(), init_rows));
        break;
      }
      default: {
        throw std::logic_error(
            std::string("Block created with unsupported type: ") +
            field->type()->ToString());
      }
    }
  }
}
template <typename field_size>
std::shared_ptr<arrow::ArrayData> Block::allocate_column_data(
    std::shared_ptr<arrow::DataType> type, int init_rows) {
  std::shared_ptr<arrow::ResizableBuffer> data;

  auto result = arrow::AllocateResizableBuffer(sizeof(field_size) * init_rows);
  evaluate_status(result.status(), __FUNCTION__, __LINE__);
  data = std::move(result.ValueOrDie());
  data->ZeroPadding();

  // Initialize null bitmap buffer to nullptr, since we currently don't use it.
  return arrow::ArrayData::Make(type, 0, {nullptr, data});
}

void Block::compute_num_bytes() {
  // Start at i=1 to skip valid column
  for (int i = 0; i < num_cols; i++) {
    switch (schema->field(i)->type()->id()) {
      case arrow::Type::STRING: {
        auto *offsets = columns[i]->GetValues<int32_t>(1, 0);
        column_sizes[i] = offsets[num_rows];
        num_bytes += offsets[num_rows];
        break;
      }
      case arrow::Type::FIXED_SIZE_BINARY: {
        int byte_width =
            schema->field(i)->type()->layout().FixedWidth(1).byte_width;
        column_sizes[i] = byte_width * columns[i]->length;
        num_bytes += byte_width * columns[i]->length;
        break;
      }
      case arrow::Type::DOUBLE:
      case arrow::Type::INT64: {
        // buffer at index 1 is the data buffer.
        int byte_width = sizeof(int64_t);
        column_sizes[i] = byte_width * columns[i]->length;
        num_bytes += byte_width * columns[i]->length;
        break;
      }
      case arrow::Type::UINT32: {
        // buffer at index 1 is the data buffer.
        int byte_width = sizeof(uint32_t);
        column_sizes[i] = byte_width * columns[i]->length;
        num_bytes += byte_width * columns[i]->length;
        break;
      }
      case arrow::Type::UINT16: {
        // buffer at index 1 is the data buffer.
        int byte_width = sizeof(uint16_t);
        column_sizes[i] = byte_width * columns[i]->length;
        num_bytes += byte_width * columns[i]->length;
        break;
      }
      case arrow::Type::UINT8: {
        // buffer at index 1 is the data buffer.
        int byte_width = sizeof(uint8_t);
        column_sizes[i] = byte_width * columns[i]->length;
        num_bytes += byte_width * columns[i]->length;
        break;
      }
      default: {
        throw std::logic_error(
            std::string("Cannot compute record width. Unsupported type: ") +
            schema->field(i)->type()->ToString());
      }
    }
  }
}

Block::Block(int id, std::shared_ptr<arrow::RecordBatch> record_batch,
             int capacity)
    : capacity(capacity), id(id), num_bytes(0) {
  arrow::Status status;
  num_rows = record_batch->num_rows();
  schema = std::move(record_batch->schema());
  num_cols = schema->num_fields();
  for (int i = 0; i < record_batch->num_columns(); i++) {
    columns.push_back(record_batch->column_data(i));
    // Column sizes will be computed in compute_num_bytes(). Store 0 for now
    column_sizes.push_back(0);
  }
  compute_num_bytes();

  // Initialize valid column separately
  std::shared_ptr<arrow::ResizableBuffer> valid_buffer;
  auto result = arrow::AllocateResizableBuffer(num_rows);
  evaluate_status(result.status(), __FUNCTION__, __LINE__);
  valid_buffer = std::move(result.ValueOrDie());

  valid = arrow::ArrayData::Make(arrow::boolean(), num_rows,
                                 {nullptr, valid_buffer});

  for (int i = 0; i < num_rows; i++) {
    set_valid(i, true);
  }
}

std::shared_ptr<arrow::RecordBatch> Block::get_records() {
  return arrow::RecordBatch::Make(schema, num_rows, columns);
}

int Block::get_num_rows() const { return num_rows; }

int Block::get_id() const { return id; }

std::shared_ptr<arrow::Array> Block::get_column(int column_index) const {
  return arrow::MakeArray(columns[column_index]);
}

std::shared_ptr<arrow::Array> Block::get_column_by_name(
    const std::string &name) const {
  return arrow::MakeArray(columns[schema->GetFieldIndex(name)]);
}

int Block::get_free_row_index() const {
  // TODO(nicholas): This may need to change when we reserve space in blocks.
  // In particular, num_rows is the number valid rows. We will need another
  // way to fetch the total number of rows in the block (valid + invalid)
  auto *data = valid->GetMutableValues<uint8_t>(1, 0);
  for (int i = 0; i < num_rows; i++) {
    if (data[i / 8] >> (i % 8u)) {
      return i;
    }
  }
  return -1;
}


bool Block::get_valid(std::shared_ptr<arrow::ArrayData> valid_arr, 
                                      unsigned int row_index) const {
  auto *data = valid_arr->GetMutableValues<uint8_t>(1, 0);
  uint8_t byte = data[row_index / 8];
  return ((byte >> (row_index % 8u)) & 1u) == 1u;
}

bool Block::get_valid(unsigned int row_index) const {
  auto *data = valid->GetMutableValues<uint8_t>(1, 0);
  uint8_t byte = data[row_index / 8];
  return ((byte >> (row_index % 8u)) & 1u) == 1u;
}

void Block::set_valid(unsigned int row_index, bool val) {
  auto *data = valid->GetMutableValues<uint8_t>(1, 0);
  if (val) {
    data[row_index / 8] |= (1u << (row_index % 8u));
  } else {
    data[row_index / 8] &=  ~(1u << (row_index % 8u));
  }
}

void Block::increment_num_rows() {
  if (num_rows++ == capacity) {
    throw std::runtime_error("Incremented number of rows beyond capacity");
  }
}

void Block::decrement_num_rows() {
  if (num_rows-- == 0) {
    throw std::runtime_error("Decremented number of rows below zero");
  }
}

void Block::increment_num_bytes(unsigned int n_bytes) {
  if (num_bytes + n_bytes > capacity) {
    std::cout << "num bytes: " << num_bytes << " " << n_bytes << " " << capacity << std::endl;
    throw std::runtime_error(
        "Incremented number of bytes stored in block beyond "
        "capacity:");
  } else {
    num_bytes += n_bytes;
  }
}

int Block::get_bytes_left() { return capacity - num_bytes; }

void Block::print() {
  // Create Arrays from ArrayData so we can easily read column data
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  for (int i = 0; i < num_cols; i++) {
    arrays.push_back(arrow::MakeArray(columns[i]));
  }

  for (int row = 0; row < num_rows; row++) {
    auto valid_col =
        std::static_pointer_cast<arrow::BooleanArray>(arrow::MakeArray(valid));
    std::cout << valid_col->Value(row) << "\t";

    for (int i = 0; i < num_cols; i++) {
      switch (schema->field(i)->type()->id()) {
        case arrow::Type::STRING: {
          auto col = std::static_pointer_cast<arrow::StringArray>(arrays[i]);

          std::cout << col->GetString(row) << "\t";
          break;
        }
        case arrow::Type::type::FIXED_SIZE_BINARY: {
          auto col =
              std::static_pointer_cast<arrow::FixedSizeBinaryArray>(arrays[i]);
          std::cout << col->GetString(row) << "\t";
          break;
        }
        case arrow::Type::type::INT64: {
          auto col = std::static_pointer_cast<arrow::Int64Array>(arrays[i]);
          std::cout << col->Value(row) << "\t";
          break;
        }
        case arrow::Type::type::UINT32: {
          auto col = std::static_pointer_cast<arrow::UInt32Array>(arrays[i]);
          std::cout << col->Value(row) << "\t";
          break;
        }
        case arrow::Type::type::UINT16: {
          auto col = std::static_pointer_cast<arrow::UInt16Array>(arrays[i]);
          std::cout << col->Value(row) << "\t";
          break;
        }
        case arrow::Type::type::UINT8: {
          auto col = std::static_pointer_cast<arrow::UInt8Array>(arrays[i]);
          std::cout << col->Value(row) << "\t";
          break;
        }
        case arrow::Type::type::DOUBLE: {
          auto col = std::static_pointer_cast<arrow::DoubleArray>(arrays[i]);
          std::cout << col->Value(row) << "\t";
          break;
        }
        default: {
          throw std::logic_error(
              std::string("Block created with unsupported type: ") +
              schema->field(i)->type()->ToString());
        }
      }
    }
    std::cout << std::endl;
  }
}

// Note that this funciton assumes that the valid column is in column_data
bool Block::insert_records(
    std::map<int, BlockInfo>& block_map,
    std::map<int, int>& row_map,
    std::shared_ptr<arrow::Array> valid_column,
    std::vector<std::shared_ptr<arrow::ArrayData>> column_data) {
  int l = column_data[0]->length;

  // TODO(nicholas): Optimize this. Calls to schema->field(i) is non-
  //  neglible since we call it once for each column of each record.
  int column_types[num_cols];
  //std::cout << "col length: " << l << std::endl;
  for (int i = 0; i < num_cols; i++) {
    column_types[i] = schema->field(i)->type()->id();
  }
  int data_size = 0;
  int reduced_count = 0;
  auto *filter_data =  valid_column->data()->GetMutableValues<uint8_t>(1, 0);
  for (int row = 0; row < l; row++) {
    int record_size = 0;

    for (int i = 0; i < num_cols; i++) {
      switch (column_types[i]) {
        case arrow::Type::STRING: {
          // TODO(nicholas) schema offsets!!!!
          auto *offsets = column_data[i]->GetValues<int32_t>(1, 0);
          record_size += offsets[row + 1] - offsets[row];
          break;
        }
        case arrow::Type::FIXED_SIZE_BINARY: {
          int byte_width =
              schema->field(i)->type()->layout().FixedWidth(1).byte_width;
          record_size += byte_width;
          break;
        }
        case arrow::Type::DOUBLE:
        case arrow::Type::INT64: {
          int byte_width = sizeof(int64_t);
          record_size += byte_width;
          break;
        }
        case arrow::Type::UINT32: {
          int byte_width = sizeof(uint32_t);
          record_size += byte_width;
          break;
        }
        case arrow::Type::UINT16: {
          int byte_width = sizeof(uint16_t);
          record_size += byte_width;
          break;
        }
        case arrow::Type::UINT8: {
          int byte_width = sizeof(uint8_t);
          record_size += byte_width;
          break;
        }
        default: {
          throw std::logic_error(
              std::string("Cannot compute record width. Unsupported type: ") +
              schema->field(i)->type()->ToString());
        }
      }
    }

    std::vector<std::shared_ptr<arrow::ArrayData>> sliced_column_data;

    for (int i = 0; i < column_data.size(); i++) {
      auto sliced_data = column_data[i]->Slice(row, 1);

      sliced_column_data.push_back(sliced_data);
    }

    if (record_size + num_bytes > capacity) {
      std::cout << "Exceeds!" << std::endl;
    }

    if ((filter_data[row / 8] >> (row % 8u) & 1u) == 1u) {
      //std::cout << "Row: " << row << std::endl;
      int row_id = row_map[row + reduced_count];
      this->row_id_map[row] = row_id;
      BlockInfo blockInfo = block_map[row_id];
      //std::cout << "Row num --  " << blockInfo.rowNum << "  reduced count -- "<< reduced_count << std::endl;
      block_map[row_id] = {blockInfo.blockId, row};
      //std::cout << "Update "  << row << std::endl;
      this->insert_records(sliced_column_data);
      //data_size += record_size;
    } else {
       reduced_count++;
       //std::cout << "Not Row: " << row << std::endl;
    }
    // num_rows++;
    //std::cout << "num rows: " << num_rows << std::endl;
  }
}

// Note that this funciton assumes that the valid column is in column_data
bool Block::insert_records(
    std::vector<std::shared_ptr<arrow::ArrayData>> column_data) {
  if (column_data[0]->length == 0) {
    return true;
  }

  arrow::Status status;
  int n = column_data[0]->length;  // number of elements to be inserted

  auto valid_buffer =
      std::static_pointer_cast<arrow::ResizableBuffer>(valid->buffers[1]);

  status = valid_buffer->Resize(valid_buffer->size() + n / 8 + 1, false);
  valid_buffer->ZeroPadding();  // Ensure the additional byte is zeroed
  // std::cout << "Number of lemen to be inserted: " << n << std::endl;
  // TODO(nicholas)
  for (int k = 0; k < n; k++) {
    set_valid(num_rows + k, true);
  }

  valid->length += n;
  int initial_bytes = num_bytes;
  // NOTE: buffers do NOT account for Slice offsets!!!
  int offset = column_data[0]->offset;

  for (int i = 0; i < num_cols; i++) {
    switch (schema->field(i)->type()->id()) {
      case arrow::Type::STRING: {
        auto offsets_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
            columns[i]->buffers[1]);
        auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
            columns[i]->buffers[2]);
        // Extended the underlying data and offsets buffer. This may
        // result in copying the data.
        // n+1 because we also need to specify the endpoint of the
        // last string.
        if ((num_rows + n + 2) * sizeof(int64_t) > offsets_buffer->capacity()) {
          status = offsets_buffer->Resize(
              offsets_buffer->capacity() + sizeof(int32_t) * (n + 1), false);
          evaluate_status(status, __FUNCTION__, __LINE__);
        }

        auto in_offsets_data =
            column_data[i]->GetMutableValues<int32_t>(1, offset);
        auto *offsets_data = columns[i]->GetMutableValues<int32_t>(1, 0);

        int string_size = in_offsets_data[n] - in_offsets_data[0];

        if (column_sizes[i] + string_size > data_buffer->capacity()) {
          status = data_buffer->Resize(column_sizes[i] + string_size, false);
          evaluate_status(status, __FUNCTION__, __LINE__);
        }

        in_offsets_data = column_data[i]->GetMutableValues<int32_t>(1, 0);

        auto in_values_data = column_data[i]->GetMutableValues<uint8_t>(
            2, in_offsets_data[offset]);

        // Insert new offset
        offsets_data = columns[i]->GetMutableValues<int32_t>(1, 0);

        int32_t current_offset = offsets_data[num_rows];
        // BUG: we do not want to copy the very first offset from the
        // input data, since it's always 0 (or a 0-point i.e.
        // reference point).
        // If the data includes the first element of the input array,
        // we copy n+1 offsets
        if (offset == 0) {
          std::memcpy(&offsets_data[num_rows], &in_offsets_data[offset],
                      sizeof(int32_t) * (n + 1));
          // Correct new offsets
          for (int k = 0; k <= n; k++) {
            // BUG: This assumes the input data was not a slice, i.e.
            // its offsets started at 0
            offsets_data[num_rows + k] += current_offset;
          }
        }
        // If the data does not include the first element of the
        // input array, we copy only n offsets
        else {
          std::memcpy(&offsets_data[num_rows + 1], &in_offsets_data[offset + 1],
                      sizeof(int32_t) * n);
          // Correct new offsets
          for (int k = 1; k <= n; k++) {
            // BUG: This assumes the input data was not a slice, i.e.
            // its offsets started at 0
            offsets_data[num_rows + k] +=
                current_offset - in_offsets_data[offset];
          }
        }

        auto *values_data =
            columns[i]->GetMutableValues<uint8_t>(2, offsets_data[num_rows]);

        std::memcpy(values_data, in_values_data, string_size);

        columns[i]->length += n;
        column_sizes[i] += string_size;
        increment_num_bytes(string_size);
        break;
      }
      case arrow::Type::FIXED_SIZE_BINARY: {
        auto field_size =
            schema->field(i)->type()->layout().FixedWidth(1).byte_width;
        int data_size = field_size * n;

        auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
            columns[i]->buffers[1]);

        if (column_sizes[i] + data_size > data_buffer->capacity()) {
          status =
              data_buffer->Resize(data_buffer->capacity() + data_size, false);
          evaluate_status(status, __FUNCTION__, __LINE__);
        }

        auto *dest =
            columns[i]->GetMutableValues<uint8_t>(1, num_rows * field_size);
        std::memcpy(dest, column_data[i]->GetValues<int64_t>(1, offset),
                    data_size);

        columns[i]->length += n;
        column_sizes[i] += data_size;
        increment_num_bytes(data_size);
        break;
      }
      case arrow::Type::INT64: {
        insert_values_in_column<int64_t>(i, offset, column_data[i], n);
        break;
      }
      case arrow::Type::UINT32: {
        insert_values_in_column<uint32_t>(i, offset, column_data[i], n);
        break;
      }
      case arrow::Type::UINT16: {
        insert_values_in_column<uint32_t>(i, offset, column_data[i], n);
        break;
      }
      case arrow::Type::UINT8: {
        insert_values_in_column<uint8_t>(i, offset, column_data[i], n);
        break;
      }
      case arrow::Type::DOUBLE: {
        insert_values_in_column<double_t>(i, offset, column_data[i], n);
        break;
      }
      default:
        throw std::logic_error(
            std::string("Cannot insert tuple with unsupported type: ") +
            schema->field(i)->type()->ToString());
    }
  }
 // std::cout << "Row bytes - " << (num_bytes - initial_bytes) << std::endl;
  num_rows += n;
  return true;
}

template <typename field_size>
void Block::insert_values_in_column(int i, int offset,
                                    std::shared_ptr<arrow::ArrayData> vals,
                                    int num_vals) {
  int data_size = sizeof(int64_t) * num_vals;

  auto data_buffer =
      std::static_pointer_cast<arrow::ResizableBuffer>(columns[i]->buffers[1]);

  if (column_sizes[i] + data_size > data_buffer->capacity()) {
    auto status =
        data_buffer->Resize(data_buffer->capacity() + data_size, false);
    evaluate_status(status, __FUNCTION__, __LINE__);
  }

  auto *dest = columns[i]->GetMutableValues<int64_t>(1, num_rows);
  std::memcpy(dest, vals->GetValues<int64_t>(1, offset), data_size);

  columns[i]->length += num_vals;
  column_sizes[i] += data_size;
  increment_num_bytes(data_size);
}

// Return true is insertion was successful, false otherwise
int Block::insert_record(uint8_t *record, int32_t *byte_widths) {
  int record_size = 0;
  for (int i = 0; i < num_cols; i++) {
    record_size += byte_widths[i];
  }
  //std::cout << "bytes left: " << get_bytes_left() << std::endl;
  // record does not fit in the block.
  if (record_size > get_bytes_left()) {
    std::cout << "record does not fit" << std::endl;
    return -1;
  }

  arrow::Status status;
  std::vector<arrow::Array> new_columns;

  // Set valid bit
  auto valid_buffer =
      std::static_pointer_cast<arrow::ResizableBuffer>(valid->buffers[1]);
  status = valid_buffer->Resize(valid_buffer->size() + 1, false);
  evaluate_status(status, __FUNCTION__, __LINE__);
  set_valid(num_rows, true);
  valid->length++;
  int initial_bytes = num_bytes;
  // Position in the record array
  int head = 0;

  for (int i = 0; i < num_cols; i++) {
    switch (schema->field(i)->type()->id()) {
      case arrow::Type::STRING: {
        auto offsets_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
            columns[i]->buffers[1]);
        auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
            columns[i]->buffers[2]);

        // Extended the underlying data and offsets buffer. This may
        // result in copying the data.
        // Use index i-1 because byte_widths does not include the byte
        // width of the valid column.
        // TODO(nicholas): size() may not be up to date! Use
        //  column_sizes[i] instead.
        status =
            data_buffer->Resize(data_buffer->size() + byte_widths[i], false);
        evaluate_status(status, __FUNCTION__, __LINE__);

        status = offsets_buffer->Resize(
            offsets_buffer->size() + sizeof(int32_t), false);
        evaluate_status(status, __FUNCTION__, __LINE__);

        // Insert new offset
        auto *offsets_data = columns[i]->GetMutableValues<int32_t>(1, 0);
        int32_t new_offset = offsets_data[num_rows] + byte_widths[i];
        std::memcpy(&offsets_data[num_rows + 1], &new_offset,
                    sizeof(new_offset));

        auto *values_data =
            columns[i]->GetMutableValues<uint8_t>(2, offsets_data[num_rows]);
        std::memcpy(values_data, &record[head], byte_widths[i]);

        columns[i]->length++;
        column_sizes[i] += byte_widths[i];
        head += byte_widths[i];
        std::cerr << "string insert" << byte_widths[i] << std::endl;
        increment_num_bytes(byte_widths[i]);
        break;
      }
      case arrow::Type::DOUBLE:
      case arrow::Type::INT64: {
        insert_value_in_column<int64_t>(i, head, &record[head], byte_widths[i]);
        break;
      }
      case arrow::Type::UINT32: {
        insert_value_in_column<uint32_t>(i, head, &record[head],
                                         byte_widths[i]);
        break;
      }
      case arrow::Type::UINT16: {
        insert_value_in_column<uint32_t>(i, head, &record[head],
                                         byte_widths[i]);
        break;
      }
      case arrow::Type::UINT8: {
        insert_value_in_column<uint8_t>(i, head, &record[head], byte_widths[i]);
        break;
      }
      default:
        throw std::logic_error(
            std::string("Cannot insert tuple with unsupported type: ") +
            schema->field(i)->type()->ToString());
    }
  }
  //std::cout << "bytes: " << num_bytes - initial_bytes << std::endl;
  increment_num_rows();

  return num_rows - 1;
}

template <typename field_size>
void Block::insert_value_in_column(int i, int &head, uint8_t *record_value,
                                   int byte_width) {
  auto data_buffer =
      std::static_pointer_cast<arrow::ResizableBuffer>(columns[i]->buffers[1]);

  auto status = data_buffer->Resize(data_buffer->size() + sizeof(field_size), false);
  evaluate_status(status, __FUNCTION__, __LINE__);

  if (byte_width >= sizeof(field_size)) {
    auto *dest = columns[i]->GetMutableValues<field_size>(1, num_rows);
    std::memcpy(dest, record_value, byte_width);
    head += byte_width;
    column_sizes[i] += byte_width;
    increment_num_bytes(byte_width);
  } else {
    std::cerr << "Block insert else part" << std::endl;
    // TODO(suryadev): Study the scope for optimization
    auto *dest = columns[i]->GetMutableValues<field_size>(1, num_rows);
    //std::cout << "Num rows: " << num_rows << " " << capacity << std::endl;
    uint8_t *value = (uint8_t *)calloc(sizeof(field_size), sizeof(uint8_t));
    std::memcpy(value, utils::reverse_bytes(record_value, byte_width),
                byte_width);
    std::memcpy(dest, value, sizeof(field_size));
    head += byte_width;
    column_sizes[i] += sizeof(field_size);
    increment_num_bytes(sizeof(field_size));
    free(value);
  }

  columns[i]->length++;
}

std::shared_ptr<arrow::Schema> Block::get_schema() { return schema; }

std::shared_ptr<arrow::Array> Block::get_valid_column() const {
  return arrow::MakeArray(valid);
}

// Return true is insertion was successful, false otherwise
bool Block::insert_record(std::vector<std::string_view> record,
                          int32_t *byte_widths) {
  int record_size = 0;
  for (int i = 0; i < num_cols; i++) {
    record_size += byte_widths[i];
  }

  // record does not fit in the block.
  if (record_size > get_bytes_left()) {
    return false;
  }

  arrow::Status status;
  std::vector<arrow::Array> new_columns;

  // Set valid bit
  auto valid_buffer =
      std::static_pointer_cast<arrow::ResizableBuffer>(valid->buffers[1]);
  status = valid_buffer->Resize(valid_buffer->size() + 1, false);
  evaluate_status(status, __FUNCTION__, __LINE__);
  set_valid(num_rows, true);
  valid->length++;

  // Position in the record array
  int head = 0;

  for (int i = 0; i < num_cols; i++) {
    switch (schema->field(i)->type()->id()) {
      case arrow::Type::STRING: {
        auto offsets_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
            columns[i]->buffers[1]);
        auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
            columns[i]->buffers[2]);

        // Extended the underlying data and offsets buffer. This may
        // result in copying the data.

        // IMPORTANT: DO NOT GRAB A POINTER TO THE UNDERLYING DATA
        // BEFORE YOU RESIZE IT. THE DATA WILL BE COPIED TO A NEW
        // LOCATION, AND YOUR POINTER WILL BE GARBAGE.
        auto *offsets_data = columns[i]->GetMutableValues<int32_t>(1, 0);

        if (offsets_data[num_rows] + record[i].length() >
            data_buffer->capacity()) {
          status = data_buffer->Resize(
              data_buffer->capacity() + record[i].length(), false);
          evaluate_status(status, __FUNCTION__, __LINE__);
        }

        // There are length+1 offsets, and we are going ot add
        // another offsets, so +2
        if ((columns[i]->length + 2) * sizeof(int32_t) >
            offsets_buffer->capacity()) {
          // Resize will not resize the buffer if the inputted size
          // equals the current size of the buffer. To force
          // resizing in this case, we add +1.
          status = offsets_buffer->Resize((num_rows + 2) * sizeof(int32_t) + 1,
                                          false);
          evaluate_status(status, __FUNCTION__, __LINE__);
          // TODO(nicholas): is this necessary?
          offsets_buffer->ZeroPadding();
        }
        // We must fetch the offsets data again, since resizing might
        // have moved its location in memory.
        offsets_data = columns[i]->GetMutableValues<int32_t>(1, 0);

        // Insert new offset
        // you must zero the padding or else you might get bogus
        // offset data!

        int32_t new_offset = offsets_data[num_rows] + record[i].length();
        std::memcpy(&offsets_data[num_rows + 1], &new_offset,
                    sizeof(new_offset));

        auto *values_data =
            columns[i]->GetMutableValues<uint8_t>(2, offsets_data[num_rows]);
        std::memcpy(values_data, &record[i].front(), record[i].length());

        columns[i]->length++;
        head += record[i].length();
        column_sizes[i] += record[i].length();
        break;
      }
      case arrow::Type::FIXED_SIZE_BINARY: {
        auto field_size = record[i].length();
        auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
            columns[i]->buffers[1]);

        if (column_sizes[i] + field_size > data_buffer->capacity()) {
          // Resize will not resize the buffer if the inputted size
          // equals the current size of the buffer. To force
          // resizing in this case, we add +1.
          auto status = data_buffer->Resize(
              data_buffer->capacity() + field_size + 1, false);
          evaluate_status(status, __FUNCTION__, __LINE__);
        }

        auto *dest = columns[i]->GetMutableValues<uint8_t>(1, num_rows);
        std::memcpy(dest, &record[i].front(), field_size);

        head += field_size;
        column_sizes[i] += field_size;
        columns[i]->length++;
        break;
      }
      case arrow::Type::INT64: {
        insert_csv_value_in_column<int64_t>(i, head, record[i], byte_widths[i]);
        break;
      }
      case arrow::Type::UINT32: {
        insert_csv_value_in_column<uint32_t>(i, head, record[i],
                                             byte_widths[i]);
        break;
      }
      case arrow::Type::UINT16: {
        insert_csv_value_in_column<uint16_t>(i, head, record[i],
                                             byte_widths[i]);
        break;
      }
      case arrow::Type::UINT8: {
        insert_csv_value_in_column<uint8_t>(i, head, record[i], byte_widths[i]);
        break;
      }

      default:
        throw std::logic_error(
            std::string("Cannot insert tuple with unsupported type: ") +
            schema->field(i)->type()->ToString());
    }
  }
  increment_num_bytes(head);
  increment_num_rows();

  return true;
}

template <typename field_size>
void Block::insert_csv_value_in_column(int i, int &head,
                                       std::string_view record,
                                       int byte_width) {
  auto data_buffer =
      std::static_pointer_cast<arrow::ResizableBuffer>(columns[i]->buffers[1]);

  if (column_sizes[i] + sizeof(field_size) > data_buffer->capacity()) {
    // Resize will not resize the buffer if the inputted size
    // equals the current size of the buffer. To force
    // resizing in this case, we add +1.
    auto status = data_buffer->Resize(
        data_buffer->capacity() + sizeof(field_size) + 1, false);
    evaluate_status(status, __FUNCTION__, __LINE__);
  }

  int64_t out;
  auto result = absl::SimpleAtoi(record, &out);

  auto *dest = columns[i]->GetMutableValues<field_size>(1, num_rows);
  std::memcpy(dest, &out, sizeof(field_size));

  head += sizeof(field_size);
  column_sizes[i] += sizeof(field_size);
  columns[i]->length++;
}

int Block::get_num_cols() const { return num_cols; }

void Block::truncate_buffers() {
  arrow::Status status;

  for (int i = 0; i < num_cols; i++) {
    switch (schema->field(i)->type()->id()) {
      case arrow::Type::STRING: {
        auto offsets_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
            columns[i]->buffers[1]);
        auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
            columns[i]->buffers[2]);

        status = data_buffer->Resize(column_sizes[i], true);
        evaluate_status(status, __FUNCTION__, __LINE__);

        status = offsets_buffer->Resize((num_rows + 1) * sizeof(int32_t), true);
        evaluate_status(status, __FUNCTION__, __LINE__);

        break;
      }
      case arrow::Type::FIXED_SIZE_BINARY: {
        auto field_size =
            schema->field(i)->type()->layout().FixedWidth(1).byte_width;
        auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
            columns[i]->buffers[1]);

        status = data_buffer->Resize(num_rows * field_size, true);
        evaluate_status(status, __FUNCTION__, __LINE__);
      }
        // This works with any fixed-width type, but for now, I specify INT64
      case arrow::Type::DOUBLE:
      case arrow::Type::INT64: {
        truncate_column_buffer<int64_t>(i);
        break;
      }
      case arrow::Type::UINT32: {
        truncate_column_buffer<uint32_t>(i);
        break;
      }
      case arrow::Type::UINT16: {
        truncate_column_buffer<uint16_t>(i);
        break;
      }
      case arrow::Type::UINT8: {
        truncate_column_buffer<uint8_t>(i);
        break;
      }
      default:
        throw std::logic_error(
            std::string("Cannot insert tuple with unsupported type: ") +
            schema->field(i)->type()->ToString());
    }
  }
}

template <typename field_size>
void Block::truncate_column_buffer(int i) {
  auto data_buffer =
      std::static_pointer_cast<arrow::ResizableBuffer>(columns[i]->buffers[1]);

  auto status = data_buffer->Resize(num_rows * sizeof(int64_t), true);
  evaluate_status(status, __FUNCTION__, __LINE__);
}
}  // namespace hustle::storage
