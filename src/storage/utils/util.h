
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

#ifndef HUSTLE_OFFLINE_UTIL_H
#define HUSTLE_OFFLINE_UTIL_H

#include <arrow/memory_pool.h>
#include <arrow/table.h>

#include <string>

//#include "catalog/table_schema.h"
#include "storage/base_table.h"
#include "storage/hustle_table.h"
#include "storage/utils/util.h"

#define IMPORT_CSV_TO_INDEX_ENABLED_BLOCK_BY_DEFAULT true

/**
 * Write a table to a file. Currently, all blocks are written to the same file.
 *
 * @param path Relative path to the file
 * @param table Table to write to file
 *
 * TODO(nicholas): write one file for each block
 */
void write_to_file(const char* path, hustle::storage::HustleTable& table);

/**
 * Construct a table from RecordBatches read from a file.
 *
 * @param path Relative path to the file
 * @param read_only Flag indicating that data need not be copied into memory
 * @return A Table containing data from all RecordBatches read from the file.
 *
 * TODO: Assuming all blocks are written to separate files, read in one block.
 */
std::shared_ptr<hustle::storage::HustleTable> read_from_file(
    const char* path, bool read_only = true, const char* table_name = "table");

/**
 * Return the columns of a RecordBatch as a vector of Arrays. This is a special
 * utility function for resolving internal inconsistencies after inserting a
 * record, e.g. see Block::insert_record()
 *
 * @param record_batch A RecordBatch
 * @return A vector containing all columns of the inputted RecordBatch
 */
std::vector<std::shared_ptr<arrow::Array>> get_columns_from_record_batch(
    const std::shared_ptr<arrow::RecordBatch>& record_batch);

/**
 * When a RecordBatch is read from a file, the read is zero-copy, and thus we
 * cannot mutate the Block's data. This function copies the data into a new,
 * mutable RecordBatch.
 *
 * @param batch An immutable RecordBatch (read from a file)
 * @return An equivalent mutable RecordBatch
 */
std::shared_ptr<arrow::RecordBatch> copy_record_batch(
    const std::shared_ptr<arrow::RecordBatch>& batch);

/**
 * @param schema A Block's schema
 * @return The minimum number of bytes contained in each record.
 */
int compute_fixed_record_width(const std::shared_ptr<arrow::Schema>& schema);

std::vector<int32_t> get_field_sizes(
    const std::shared_ptr<arrow::Schema>& schema);

std::shared_ptr<hustle::storage::HustleTable> read_from_csv_file(
    const char* path, std::shared_ptr<arrow::Schema> schema, int block_size,
    bool index_enabled);

inline std::shared_ptr<hustle::storage::HustleTable> read_from_csv_file(
    const char* path, std::shared_ptr<arrow::Schema> schema, int block_size) {
  return read_from_csv_file(path, schema, block_size,
                            IMPORT_CSV_TO_INDEX_ENABLED_BLOCK_BY_DEFAULT);
}

//std::shared_ptr<arrow::Schema> make_schema(
//    const hustle::catalog::TableSchema& schema);

std::shared_ptr<arrow::ChunkedArray> array_to_chunkedarray(
    std::shared_ptr<arrow::Array> array, int num_chunks);

#endif  // HUSTLE_OFFLINE_UTIL_H
