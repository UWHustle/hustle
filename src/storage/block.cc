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
#include "storage/record_transformer.h"
#include "absl/strings/numbers.h"
#include "util.h"

#define ESTIMATED_STR_LEN 30

namespace hustle::storage {

Block::Block(int id, const std::shared_ptr<arrow::Schema> &in_schema,
             int capacity, bool enable_metadata)
    : num_rows(0),
      num_bytes(0),
      capacity(capacity),
      id(id),
      schema(in_schema),
      metadata_enabled(enable_metadata) {
  arrow::Status status;
  num_cols = schema->num_fields();
  this->fixed_record_width = compute_fixed_record_width(schema);
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
            AllocateColumnData<int64_t>(field->type(), init_rows));
        break;
      }
      case arrow::Type::UINT32: {
        columns.push_back(
            AllocateColumnData<uint32_t>(field->type(), init_rows));
        break;
      }
      case arrow::Type::UINT16: {
        columns.push_back(
            AllocateColumnData<uint16_t>(field->type(), init_rows));
        break;
      }
      case arrow::Type::UINT8: {
        columns.push_back(
            AllocateColumnData<uint8_t>(field->type(), init_rows));
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

Block::Block(int id, std::shared_ptr<arrow::RecordBatch> record_batch,
             int capacity, bool enable_metadata)
    : capacity(capacity), id(id), num_bytes(0), metadata_enabled(enable_metadata){
  arrow::Status status;
  num_rows = record_batch->num_rows();
  schema = std::move(record_batch->schema());
  num_cols = schema->num_fields();
  for (int i = 0; i < record_batch->num_columns(); i++) {
    columns.push_back(record_batch->column_data(i));
    // Column sizes will be computed in ComputeByteSize(). Store 0 for now
    column_sizes.push_back(0);
  }
  ComputeByteSize();

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

template <typename field_size>
std::shared_ptr<arrow::ArrayData> Block::AllocateColumnData(
    std::shared_ptr<arrow::DataType> type, int init_rows) {
  std::shared_ptr<arrow::ResizableBuffer> data;

  auto result = arrow::AllocateResizableBuffer(sizeof(field_size) * init_rows);
  evaluate_status(result.status(), __FUNCTION__, __LINE__);
  data = std::move(result.ValueOrDie());
  data->ZeroPadding();

  // Initialize null bitmap buffer to nullptr, since we currently don't use it.
  return arrow::ArrayData::Make(type, 0, {nullptr, data});
}

void Block::ComputeByteSize() {
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

// Note that this function assumes that the valid column is in column_data
int Block::InsertRecords(
    std::map<int, BlockInfo> &block_map, std::map<int, int> &row_map,
    const std::shared_ptr<arrow::Array> valid_column,
    const std::vector<std::shared_ptr<arrow::ArrayData>> column_data) {
  int col_length = column_data[0]->length;
  int column_types[num_cols];
  for (int i = 0; i < num_cols; i++) {
    column_types[i] = schema->field(i)->type()->id();
  }
  int data_size = 0;
  int reduced_count = 0;
  auto *filter_data = valid_column->data()->GetMutableValues<uint8_t>(1, 0);
  for (int row = 0; row < col_length; row++) {
    std::vector<std::shared_ptr<arrow::ArrayData>> sliced_column_data;
    for (int i = 0; i < column_data.size(); i++) {
      auto sliced_data = column_data[i]->Slice(row, 1);
      sliced_column_data.push_back(sliced_data);
    }
    if ((filter_data[row / 8] >> (row % 8u) & 1u) == 1u) {
      int row_id = row_map[row + reduced_count];
      this->row_id_map[row] = row_id;
      BlockInfo blockInfo = block_map[row_id];
      block_map[row_id] = {blockInfo.block_id, row};
      this->InsertRecords(sliced_column_data);
    } else {
      reduced_count++;
    }
  }
  return num_rows - 1;
}

// Note that this function assumes that the valid column is in column_data
int Block::InsertRecords(
    std::vector<std::shared_ptr<arrow::ArrayData>> column_data) {
  if (column_data[0]->length == 0) {
    return -1;
  }

  arrow::Status status;
  int n = column_data[0]->length;  // number of elements to be inserted
  auto valid_buffer =
      std::static_pointer_cast<arrow::ResizableBuffer>(valid->buffers[1]);
  status = valid_buffer->Resize(valid_buffer->size() + n / 8 + 1, false);
  valid_buffer->ZeroPadding();  // Ensure the additional byte is zeroed
  for (int k = 0; k < n; k++) {
    set_valid(num_rows + k, true);
  }
  valid->length += n;
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
        num_bytes += string_size;
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
        num_bytes += data_size;
        break;
      }
      case arrow::Type::INT64: {
        InsertValues<int64_t>(i, offset, column_data[i], n);
        break;
      }
      case arrow::Type::UINT32: {
        InsertValues<uint32_t>(i, offset, column_data[i], n);
        break;
      }
      case arrow::Type::UINT16: {
        InsertValues<uint32_t>(i, offset, column_data[i], n);
        break;
      }
      case arrow::Type::UINT8: {
        InsertValues<uint8_t>(i, offset, column_data[i], n);
        break;
      }
      case arrow::Type::DOUBLE: {
        InsertValues<double_t>(i, offset, column_data[i], n);
        break;
      }
      default:
        throw std::logic_error(
            std::string("Cannot insert tuple with unsupported type: ") +
            schema->field(i)->type()->ToString());
    }
  }
  num_rows += n;
  return num_rows - 1;
}

template <typename field_size>
void Block::InsertValues(int col_num, int offset,
                         std::shared_ptr<arrow::ArrayData> vals, int num_vals) {
  int data_size = sizeof(int64_t) * num_vals;
  auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
      columns[col_num]->buffers[1]);
  if (column_sizes[col_num] + data_size > data_buffer->capacity()) {
    auto status =
        data_buffer->Resize(data_buffer->capacity() + data_size, false);
    evaluate_status(status, __FUNCTION__, __LINE__);
  }
  auto *dest = columns[col_num]->GetMutableValues<int64_t>(1, num_rows);
  std::memcpy(dest, vals->GetValues<int64_t>(1, offset), data_size);
  columns[col_num]->length += num_vals;
  column_sizes[col_num] += data_size;
  num_bytes += data_size;
}

int Block::InsertRecord(uint8_t *record, int32_t *serial_types) {
  int record_size = 0;
  for (int i = 0; i < num_cols; i++) {
    record_size += recordSerialGet(record, serial_types[i], NULL);
  }
  // record does not fit in the block.
  if (record_size > get_bytes_left()) {
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
  // Position in the record array
  int head = 0;

  for (int i = 0; i < num_cols; i++) {
      //std::cerr << "type:  " << schema->field(i)->type()->id() << " "  << i << std::endl;
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
        int32_t byte_width = recordSerialGet(&record[head], serial_types[i], NULL);
        status =
            data_buffer->Resize(data_buffer->size() + recordSerialGet(&record[head], byte_width, NULL), false);
        evaluate_status(status, __FUNCTION__, __LINE__);
        status = offsets_buffer->Resize(
            offsets_buffer->size() + sizeof(int32_t), false);
        evaluate_status(status, __FUNCTION__, __LINE__);

        // Insert new offset
        auto *offsets_data = columns[i]->GetMutableValues<int32_t>(1, 0);
        int32_t new_offset = offsets_data[num_rows] + byte_width;
        std::memcpy(&offsets_data[num_rows + 1], &new_offset,
                    sizeof(new_offset));
        auto *values_data =
            columns[i]->GetMutableValues<uint8_t>(2, offsets_data[num_rows]);
        std::memcpy(values_data, &record[head], byte_width);
        columns[i]->length++;
        column_sizes[i] += byte_width;
        head += byte_width;
        num_bytes += byte_width;
        break;
      }
      case arrow::Type::DOUBLE:
      case arrow::Type::INT64: {
         // std::cerr << "width " << serial_types[i] << std::endl;
        InsertValue<int64_t>(i, head, &record[head], serial_types[i]);
        break;
      }
      case arrow::Type::UINT32: {
        InsertValue<uint32_t>(i, head, &record[head], serial_types[i]);
        break;
      }
      case arrow::Type::UINT16: {
        InsertValue<uint32_t>(i, head, &record[head], serial_types[i]);
        break;
      }
      case arrow::Type::UINT8: {
        InsertValue<uint8_t>(i, head, &record[head], serial_types[i]);
        break;
      }
      default:
        throw std::logic_error(
            std::string("Cannot insert tuple with unsupported type: ") +
            schema->field(i)->type()->ToString());
    }
  }
  return num_rows++;
}

template <typename field_size>
void Block::InsertValue(int col_num, int &head, uint8_t *record_value,
                        int serial_type) {
  auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
      columns[col_num]->buffers[1]);
  auto status =
      data_buffer->Resize(data_buffer->size() + sizeof(field_size), false);
  evaluate_status(status, __FUNCTION__, __LINE__);
  RecordValue  record_val;
  int32_t byte_width = recordSerialGet(record_value, serial_type, &record_val);

  if (byte_width >= sizeof(field_size)) {
    auto *dest = columns[col_num]->GetMutableValues<field_size>(1, num_rows);
    std::memcpy(dest, record_value, byte_width);
    head += byte_width;
    column_sizes[col_num] += byte_width;
    num_bytes += byte_width;
  } else {
    // TODO(suryadev): Study the scope for optimization
    auto *dest = columns[col_num]->GetMutableValues<field_size>(1, num_rows);
    uint8_t *value = NULL;
    bool isZeroOneOpt = (record_val.flags & VAL_Int) && byte_width == 0;
    if (isZeroOneOpt) {
      value = reinterpret_cast<uint8_t *>(&record_val.val);
    } else {
        value = (uint8_t *) calloc(sizeof(field_size), sizeof(uint8_t));
        std::memcpy(value, utils::reverse_bytes(record_value, byte_width),
                    byte_width);
    }
    std::memcpy(dest, value, sizeof(field_size));
    head += byte_width;
    column_sizes[col_num] += sizeof(field_size);
    num_bytes += sizeof(field_size);
    if (!isZeroOneOpt)
        free(value);
  }

  columns[col_num]->length++;
}

// Return true is insertion was successful, false otherwise
int Block::InsertRecord(std::vector<std::string_view> record,
                        int32_t *byte_widths) {
  int record_size = 0;
  for (int i = 0; i < num_cols; i++) {
    record_size += byte_widths[i];
  }

  // record does not fit in the block.
  if (record_size > get_bytes_left()) {
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
          // optimize? is this necessary?
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
        InsertCsvValue<int64_t>(i, head, record[i]);
        break;
      }
      case arrow::Type::UINT32: {
        InsertCsvValue<uint32_t>(i, head, record[i]);
        break;
      }
      case arrow::Type::UINT16: {
        InsertCsvValue<uint16_t>(i, head, record[i]);
        break;
      }
      case arrow::Type::UINT8: {
        InsertCsvValue<uint8_t>(i, head, record[i]);
        break;
      }
      default:
        throw std::logic_error(
            std::string("Cannot insert tuple with unsupported type: ") +
            schema->field(i)->type()->ToString());
    }
  }
  num_bytes += head;
  return num_rows++;
}

template <typename field_size>
void Block::InsertCsvValue(int col_num, int &head, std::string_view record) {
  auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
      columns[col_num]->buffers[1]);
  if (column_sizes[col_num] + sizeof(field_size) > data_buffer->capacity()) {
    // Resize will not resize the buffer if the inputted size
    // equals the current size of the buffer. To force
    // resizing in this case, we add +1.
    auto status = data_buffer->Resize(
        data_buffer->capacity() + sizeof(field_size) + 1, false);
    evaluate_status(status, __FUNCTION__, __LINE__);
  }
  int64_t out;
  auto result = absl::SimpleAtoi(record, &out);
  auto *dest = columns[col_num]->GetMutableValues<field_size>(1, num_rows);
  std::memcpy(dest, &out, sizeof(field_size));
  head += sizeof(field_size);
  column_sizes[col_num] += sizeof(field_size);
  columns[col_num]->length++;
}

void Block::TruncateBuffers() {
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
        TruncateColumnBuffer<int64_t>(i);
        break;
      }
      case arrow::Type::UINT32: {
        TruncateColumnBuffer<uint32_t>(i);
        break;
      }
      case arrow::Type::UINT16: {
        TruncateColumnBuffer<uint16_t>(i);
        break;
      }
      case arrow::Type::UINT8: {
        TruncateColumnBuffer<uint8_t>(i);
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
void Block::TruncateColumnBuffer(int col_num) {
  auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
      columns[col_num]->buffers[1]);
  auto status = data_buffer->Resize(num_rows * sizeof(int64_t), true);
  evaluate_status(status, __FUNCTION__, __LINE__);
}
}  // namespace hustle::storage
