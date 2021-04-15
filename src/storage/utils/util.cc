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

#include <iostream>

#include "arrow_utils.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "scheduler/scheduler.h"
#include "storage/hustle_block.h"
#include "storage/hustle_table.h"
#include "storage/base_table.h"
#include "storage/index_aware_table.h"
#include "type/type_helper.h"

// TODO: Refactor this function.
//  I don't think we need to do this.
std::shared_ptr<arrow::RecordBatch> copy_record_batch(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  arrow::Status status;
  std::vector<std::shared_ptr<arrow::ArrayData>> arraydatas;

  for (int i = 0; i < batch->num_columns(); i++) {
    auto column = batch->column(i);
    auto buffers = column->data()->buffers;

    auto data_type = column->type();
    auto lambda_func = [&]<typename T>(T*) {
      // TODO: This case should capture all the type with OFFSET?
      if constexpr (hustle::isOneOf<T, arrow::StringType>::value) {
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
      } else if constexpr (hustle::has_builder_type<
                               T>::is_defalut_constructable_v) {
        auto type_singleton = arrow::TypeTraits<T>::type_singleton();

        std::shared_ptr<arrow::Buffer> data;
        auto result = buffers[1]->CopySlice(0, buffers[1]->size());
        evaluate_status(result.status(), __FUNCTION__, __LINE__);
        data = result.ValueOrDie();

        arraydatas.push_back(arrow::ArrayData::Make(
            type_singleton, column->length(), {nullptr, data}));

      } else {
        throw std::logic_error(std::string("Cannot copy data of type ") +
                               column->type()->ToString());
      }
    };

    hustle::type_switcher(data_type, lambda_func);
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
std::shared_ptr<hustle::storage::HustleTable> read_from_file(
    const char* path, bool read_only, const char* table_name) {
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

  return std::make_shared<hustle::storage::BaseTable>(
      table_name, record_batches, BLOCK_SIZE);
}

std::vector<std::shared_ptr<arrow::Array>> get_columns_from_record_batch(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  std::vector<std::shared_ptr<arrow::Array>> columns;

  for (int i = 0; i < record_batch->num_columns(); i++) {
    columns.push_back(record_batch->column(i));
  }

  return columns;
}

void write_to_file(const char* path, hustle::storage::HustleTable& table) {
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
    auto data_type = field->type();
    // TODO: (Refactor) Review the function design.
    // TODO: (Type Coverage) What is the benaviour of other types?

    auto handler = [&]<typename T>(T*) {
      if constexpr (hustle::has_ctype_member<T>::value) {
        using CType = typename hustle::ArrowGetCType<T>;
        fixed_width += sizeof(CType);
      } else if constexpr (hustle::isOneOf<T,
                                           arrow::FixedSizeBinaryType>::value) {
        fixed_width += field->type()->layout().FixedWidth(1).byte_width;
      } else if constexpr (hustle::isOneOf<T, arrow::BooleanType,
                                           arrow::StringType>::value) {
        // Do nothing for these types
        return;
      } else {
        throw std::logic_error(
            std::string(
                "Cannot compute fixed record width. Unsupported type: ") +
            field->type()->ToString());
      }
    };
    hustle::type_switcher(data_type, handler);
  }

  return fixed_width;
}

std::vector<int32_t> get_field_sizes(
    const std::shared_ptr<arrow::Schema>& schema) {
  std::vector<int32_t> field_sizes;

  for (auto& field : schema->fields()) {
    auto data_type = field->type();

    auto handler = [&]<typename T>(T*) {
      if constexpr (hustle::has_ctype_member<T>::value) {
        using CType = typename hustle::ArrowGetCType<T>;
        field_sizes.push_back(sizeof(CType));

      } else if constexpr (hustle::isOneOf<T,
                                           arrow::FixedSizeBinaryType>::value) {
        field_sizes.push_back(field->type()->layout().FixedWidth(1).byte_width);

      } else if constexpr (hustle::isOneOf<T, arrow::BooleanType,
                                           arrow::StringType>::value) {
        field_sizes.push_back(-1);

      } else {
        throw std::logic_error(
            std::string(
                "Cannot compute fixed record width. Unsupported type: ") +
            field->type()->ToString());
      }
    };

    hustle::type_switcher(data_type, handler);
  }
  return field_sizes;
}

std::shared_ptr<hustle::storage::HustleTable> read_from_csv_file(
    const char* path, std::shared_ptr<arrow::Schema> schema, int block_size,
    bool index_enabled) {
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

  // TODO: (Refactor) Remove these unused vars?
  int num_bytes = 0;
  int variable_record_width;
  int fixed_record_width = compute_fixed_record_width(schema);

  std::vector<int> string_column_indices;

  int32_t byte_widths[schema->num_fields()];
  // TODO: (Refactor) Remove these unused vars?
  int32_t byte_offsets[schema->num_fields()];

  for (int i = 0; i < schema->num_fields(); i++) {
    auto data_type = schema->field(i)->type();

    // TODO: (Refactor) If specify [&] only, the lambda will get deleted and
    //  throw error.
    auto handler = [&byte_widths, &i, &string_column_indices,
                    &data_type]<typename T>(T*) {
      if constexpr (hustle::has_ctype_member<T>::value) {
        using CType = typename hustle::ArrowGetCType<T>;
        byte_widths[i] = sizeof(CType);

      } else if constexpr (hustle::isOneOf<T,
                                           arrow::FixedSizeBinaryType>::value) {
        byte_widths[i] = data_type->layout().FixedWidth(1).byte_width;

      } else if constexpr (hustle::isOneOf<T, arrow::StringType>::value) {
        string_column_indices.push_back(i);
        byte_widths[i] = -1;

      } else {
        throw std::logic_error(
            std::string("Cannot get byte width. Unsupported type: ") +
            data_type->ToString());
      }
    };

    hustle::type_switcher(data_type, handler);
  }

  std::string line;

  std::shared_ptr<hustle::storage::HustleTable> out_table;
  if (index_enabled) {
    out_table = std::make_shared<hustle::storage::IndexAwareTable>("table", schema,
                                                             block_size);
  } else {
    out_table = std::make_shared<hustle::storage::BaseTable>(
        "table", schema, block_size);
  }

  while (fgets(buf, 1024, file)) {
    // Note that the newline character is still included!
    auto buf_stripped = absl::StripTrailingAsciiWhitespace(buf);
    std::vector<absl::string_view> values = absl::StrSplit(buf_stripped, '|');

    for (int index : string_column_indices) {
      byte_widths[index] = values[index].length();
    }

    out_table->InsertRecord(values, byte_widths);
  }

  if (index_enabled) {
    std::dynamic_pointer_cast<hustle::storage::IndexAwareTable>(out_table)
        ->GenerateIndices();
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