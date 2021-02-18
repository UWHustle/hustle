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

#include "util.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/scalar.h>

#include <fstream>
#include <iostream>

#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "scheduler/scheduler.h"
#include "storage/block.h"
#include "storage/table.h"

void evaluate_status(const arrow::Status& status, const char* function_name,
                     int line_no) {
  if (!status.ok()) {
    std::cout << "\nInvalid status: " << function_name << ", line " << line_no
              << std::endl;
    throw std::runtime_error(status.ToString());
  }
}

std::shared_ptr<arrow::RecordBatch> copy_record_batch(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  arrow::Status status;
  std::vector<std::shared_ptr<arrow::ArrayData>> arraydatas;

  for (int i = 0; i < batch->num_columns(); i++) {
    auto column = batch->column(i);
    auto buffers = column->data()->buffers;
    switch (column->type_id()) {
      case arrow::Type::STRING: {
        std::shared_ptr<arrow::Buffer> offsets;
        std::shared_ptr<arrow::Buffer> data;
        auto result = buffers[1]->CopySlice(0, buffers[1]->size());
        evaluate_status(result.status(), __FUNCTION__, __LINE__);
        offsets = result.ValueOrDie();

        result = buffers[2]->CopySlice(0, buffers[2]->size());
        evaluate_status(result.status(), __FUNCTION__, __LINE__);
        data = result.ValueOrDie();

        arraydatas.push_back(arrow::ArrayData::Make(
            arrow::utf8(), column->length(), {nullptr, offsets, data}));
        break;
      }

      case arrow::Type::BOOL: {
        std::shared_ptr<arrow::Buffer> data;
        auto result = buffers[1]->CopySlice(0, buffers[1]->size());
        evaluate_status(result.status(), __FUNCTION__, __LINE__);
        data = result.ValueOrDie();

        arraydatas.push_back(arrow::ArrayData::Make(
            arrow::boolean(), column->length(), {nullptr, data}));
        break;
      }
      case arrow::Type::INT64: {
        std::shared_ptr<arrow::Buffer> data;
        auto result = buffers[1]->CopySlice(0, buffers[1]->size());
        evaluate_status(result.status(), __FUNCTION__, __LINE__);
        data = result.ValueOrDie();

        arraydatas.push_back(arrow::ArrayData::Make(
            arrow::int64(), column->length(), {nullptr, data}));
        break;
      }
      case arrow::Type::UINT32: {
        std::shared_ptr<arrow::Buffer> data;
        auto result = buffers[1]->CopySlice(0, buffers[1]->size());
        evaluate_status(result.status(), __FUNCTION__, __LINE__);
        data = result.ValueOrDie();

        arraydatas.push_back(arrow::ArrayData::Make(
            arrow::uint32(), column->length(), {nullptr, data}));
        break;
      }
      case arrow::Type::UINT16: {
        std::shared_ptr<arrow::Buffer> data;
        auto result = buffers[1]->CopySlice(0, buffers[1]->size());
        evaluate_status(result.status(), __FUNCTION__, __LINE__);
        data = result.ValueOrDie();

        arraydatas.push_back(arrow::ArrayData::Make(
            arrow::uint16(), column->length(), {nullptr, data}));
        break;
      }
      case arrow::Type::UINT8: {
        std::shared_ptr<arrow::Buffer> data;
        auto result = buffers[1]->CopySlice(0, buffers[1]->size());
        evaluate_status(result.status(), __FUNCTION__, __LINE__);
        data = result.ValueOrDie();

        arraydatas.push_back(arrow::ArrayData::Make(
            arrow::uint8(), column->length(), {nullptr, data}));
        break;
      }
      default: {
        throw std::logic_error(std::string("Cannot copy data of type ") +
                               column->type()->ToString());
      }
    }
  }

  return arrow::RecordBatch::Make(batch->schema(), batch->num_rows(),
                                  arraydatas);
}

void read_record_batch(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader>&
        record_batch_reader,
    int i, bool read_only,
    std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches) {
  auto result3 = record_batch_reader->ReadRecordBatch(i);
  evaluate_status(result3.status(), __FUNCTION__, __LINE__);
  auto in_batch = result3.ValueOrDie();
  if (in_batch != nullptr) {
    if (read_only) {
      record_batches[i] = in_batch;
    } else {
      auto batch_copy = copy_record_batch(in_batch);
      record_batches[i] = batch_copy;
    }
  }
}
// TOOO(nicholas): Distinguish between reading blocks we intend to mutate vs.
// reading blocks we do not intend to mutate.
std::shared_ptr<hustle::storage::DBTable> read_from_file(const char* path,
                                                         bool read_only) {
  auto& scheduler = hustle::Scheduler::GlobalInstance();

  arrow::Status status;
  std::shared_ptr<arrow::io::ReadableFile> infile;
  auto result = arrow::io::ReadableFile::Open(path);
  evaluate_status(result.status(), __FUNCTION__, __LINE__);
  infile = result.ValueOrDie();

  std::shared_ptr<arrow::ipc::RecordBatchFileReader> record_batch_reader;
  auto result2 = arrow::ipc::RecordBatchFileReader::Open(infile);
  evaluate_status(result2.status(), __FUNCTION__, __LINE__);
  record_batch_reader = result2.ValueOrDie();

  std::shared_ptr<arrow::RecordBatch> in_batch;
  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
  record_batches.resize(record_batch_reader->num_record_batches());

  for (int i = 0; i < record_batch_reader->num_record_batches(); i++) {
    scheduler.addTask(hustle::CreateLambdaTask(
        [i, read_only, record_batch_reader, &record_batches]() {
          read_record_batch(record_batch_reader, i, read_only, record_batches);
        }));
  }

  scheduler.start();
  scheduler.join();

  return std::make_shared<hustle::storage::DBTable>("table", record_batches,
                                                    BLOCK_SIZE);
}

std::vector<std::shared_ptr<arrow::Array>> get_columns_from_record_batch(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  std::vector<std::shared_ptr<arrow::Array>> columns;

  for (int i = 0; i < record_batch->num_columns(); i++) {
    columns.push_back(record_batch->column(i));
  }

  return columns;
}

void write_to_file(const char* path, hustle::storage::DBTable& table) {
  std::shared_ptr<arrow::io::FileOutputStream> file;
  std::shared_ptr<arrow::ipc::RecordBatchWriter> record_batch_writer;
  arrow::Status status;

  evaluate_status(status, __FUNCTION__, __LINE__);

  auto result = arrow::io::FileOutputStream::Open(path, false);
  if (result.ok()) {
    file = result.ValueOrDie();
  } else {
    evaluate_status(result.status(), __FUNCTION__, __LINE__);
  }
  evaluate_status(status, __FUNCTION__, __LINE__);
  auto result2 = arrow::ipc::NewFileWriter(file.get(), table.get_schema());
  evaluate_status(result2.status(), __FUNCTION__, __LINE__);
  record_batch_writer = result2.ValueOrDie();

  auto blocks = table.get_blocks();

  for (int i = 0; i < blocks.size(); i++) {
    // IMPORTANT: The buffer size must be consistent with the ArrayData
    // length, or else data will not be properly written to file.
    blocks[i]->TruncateBuffers();
    status = record_batch_writer->WriteRecordBatch(*blocks[i]->get_records());
    evaluate_status(status, __FUNCTION__, __LINE__);
  }
  status = record_batch_writer->Close();
  evaluate_status(status, __FUNCTION__, __LINE__);
}

int compute_fixed_record_width(const std::shared_ptr<arrow::Schema>& schema) {
  int fixed_width = 0;

  for (auto& field : schema->fields()) {
    switch (field->type()->id()) {
      case arrow::Type::STRING: {
        break;
      }
      case arrow::Type::BOOL: {
        break;
      }
      case arrow::Type::FIXED_SIZE_BINARY: {
        fixed_width += field->type()->layout().FixedWidth(1).byte_width;
        break;
      }
      case arrow::Type::DOUBLE:
      case arrow::Type::INT64: {
        fixed_width += sizeof(int64_t);
        break;
      }
      case arrow::Type::UINT32: {
        fixed_width += sizeof(uint32_t);
        break;
      }
      case arrow::Type::UINT8: {
        fixed_width += sizeof(uint8_t);
        break;
      }
      default: {
        throw std::logic_error(
            std::string(
                "Cannot compute fixed record width. Unsupported type: ") +
            field->type()->ToString());
      }
    }
  }
  return fixed_width;
}

std::vector<int32_t> get_field_sizes(
    const std::shared_ptr<arrow::Schema>& schema) {
  std::vector<int32_t> field_sizes;

  for (auto& field : schema->fields()) {
    switch (field->type()->id()) {
      case arrow::Type::STRING: {
        field_sizes.push_back(-1);
        break;
      }
      case arrow::Type::FIXED_SIZE_BINARY: {
        field_sizes.push_back(field->type()->layout().FixedWidth(1).byte_width);
        break;
      }
      case arrow::Type::BOOL: {
        field_sizes.push_back(-1);
        break;
      }
      case arrow::Type::DOUBLE:
      case arrow::Type::INT64: {
        field_sizes.push_back(sizeof(int64_t));
        break;
      }
      case arrow::Type::UINT32: {
        field_sizes.push_back(sizeof(uint32_t));
        break;
      }
      case arrow::Type::UINT16: {
        field_sizes.push_back(sizeof(uint16_t));
        break;
      }
      case arrow::Type::UINT8: {
        field_sizes.push_back(sizeof(uint8_t));
        break;
      }
      default: {
        throw std::logic_error(
            std::string("Cannot get field size. Unsupported type: ") +
            field->type()->ToString());
      }
    }
  }
  return field_sizes;
}

std::shared_ptr<hustle::storage::DBTable> read_from_csv_file(
    const char* path, std::shared_ptr<arrow::Schema> schema, int block_size,
    bool metadata_enabled) {
  arrow::Status status;

  // RecordBatchBuilder initializes ArrayBuilders for each field in schema
  std::unique_ptr<arrow::RecordBatchBuilder> record_batch_builder;
  status = arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool(),
                                           &record_batch_builder);
  evaluate_status(status, __FUNCTION__, __LINE__);
  record_batch_builder->SetInitialCapacity(BLOCK_SIZE);

  // We will output a table constructed from these RecordBatches
  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;

  FILE* file;
  file = fopen(path, "rb");
  if (file == nullptr) {
    std::__throw_runtime_error("Cannot open file.");
  }

  char buf[1024];

  int num_bytes = 0;
  int variable_record_width;
  int fixed_record_width = compute_fixed_record_width(schema);

  std::vector<int> string_column_indices;

  int32_t byte_widths[schema->num_fields()];
  int32_t byte_offsets[schema->num_fields()];

  for (int i = 0; i < schema->num_fields(); i++) {
    switch (schema->field(i)->type()->id()) {
      case arrow::Type::STRING: {
        string_column_indices.push_back(i);
        byte_widths[i] = -1;
        break;
      }
      case arrow::Type::FIXED_SIZE_BINARY: {
        byte_widths[i] =
            schema->field(i)->type()->layout().FixedWidth(1).byte_width;
        break;
      }
      case arrow::Type::INT64: {
        byte_widths[i] = sizeof(int64_t);
        break;
      }
      case arrow::Type::UINT32: {
        byte_widths[i] = sizeof(uint32_t);
        break;
      }
      case arrow::Type::UINT16: {
        byte_widths[i] = sizeof(uint16_t);
        break;
      }
      case arrow::Type::UINT8: {
        byte_widths[i] = sizeof(uint8_t);
        break;
      }
      default: {
        throw std::logic_error(
            std::string("Cannot get byte width. Unsupported type: ") +
            schema->field(i)->type()->ToString());
      }
    }
  }

  std::string line;

  auto out_table = std::make_shared<hustle::storage::DBTable>(
      "table", schema, block_size, metadata_enabled);

  while (fgets(buf, 1024, file)) {
    // Note that the newline character is still included!
    auto buf_stripped = absl::StripTrailingAsciiWhitespace(buf);
    std::vector<absl::string_view> values = absl::StrSplit(buf_stripped, '|');

    for (int index : string_column_indices) {
      byte_widths[index] = values[index].length();
    }

    out_table->InsertRecord(values, byte_widths);
  }
  if(metadata_enabled) {
    out_table->BuildMetadata();
  }
  return out_table;
}

std::shared_ptr<arrow::Schema> make_schema(
    const hustle::catalog::TableSchema& catalog_schema) {
  arrow::Status status;
  arrow::SchemaBuilder schema_builder;

  std::shared_ptr<arrow::Field> field;

  for (auto& col : catalog_schema.getColumns()) {
    switch (col.getHustleType()) {
      // TODO(nicholas): distinguish between different integer types
      case hustle::catalog::INTEGER: {
        field = arrow::field(col.getName(), arrow::int64());
        break;
      }
      case hustle::catalog::CHAR: {
        field = arrow::field(col.getName(), arrow::utf8());
        break;
      }
    }
    status = schema_builder.AddField(field);
    evaluate_status(status, __FUNCTION__, __LINE__);
  }

  auto result = schema_builder.Finish();
  evaluate_status(result.status(), __FUNCTION__, __LINE__);

  return result.ValueOrDie();
}

void skew_column(std::shared_ptr<arrow::ChunkedArray>& col) {
  auto num_chunks = col->num_chunks();

  for (int i = 0; i < num_chunks; ++i) {
    auto chunk = col->chunk(i);
    auto chunk_data = chunk->data()->GetMutableValues<uint32_t>(1, 0);
    auto chunk_length = chunk->length();

    for (int j = 0; j < chunk_length; ++j) {
      chunk_data[j] = 0;
    }
  }
}

std::shared_ptr<arrow::ChunkedArray> array_to_chunkedarray(
    std::shared_ptr<arrow::Array> array, int num_chunks) {
  arrow::ArrayVector vec;
  vec.resize(num_chunks);

  int slice_length = -1;
  if (array->length() <= num_chunks) {
    slice_length = array->length();
    for (int i = 1; i < num_chunks; ++i) {
      vec[i] =
          arrow::MakeArrayOfNull(array->type(), 0, arrow::default_memory_pool())
              .ValueOrDie();
    }
    num_chunks = 1;

  } else {
    slice_length = array->length() / num_chunks;
  }

  std::shared_ptr<arrow::Array> sliced_array;
  for (int i = 0; i < num_chunks - 1; ++i) {
    sliced_array = array->Slice(i * slice_length, slice_length);
    vec[i] = sliced_array;
  }

  if (num_chunks > 1) {
    sliced_array =
        array->Slice(vec[num_chunks - 2]->offset() + slice_length,
                     array->length() - vec[num_chunks - 2]->offset());
    vec[num_chunks - 1] = sliced_array;
  } else {
    vec[0] = array;
  }

  return std::make_shared<arrow::ChunkedArray>(vec);
}