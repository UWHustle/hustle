
#ifndef HUSTLE_OFFLINE_UTIL_H
#define HUSTLE_OFFLINE_UTIL_H

#include <string>
#include <arrow/memory_pool.h>
#include <arrow/table.h>
#include "table.h"

// If a status outcome is an error, print its error message and throw an
// expection. Otherwise, do nothing.
//
// TODO: Do we want to throw an expection on error?
//
// status: status outcome object
// function_name: name of the function that called EvaluateStatus
// line_no: line number of the call to EvaluateStatus
//
// The function_name and line_no parameters allow us to find exactly where
// the error occured, since the same error can be thrown from different places.
void evaluate_status(const arrow::Status &status, const char *function_name, int line_no);

// Write a table to a file. Currently, all blocks are written to the same file.
// TODO(nicholas): write one file for each block
void write_to_file(const char *path, Table &table);

// Construct a table from RecordBatches read from a file.
// TODO: Assuming all blocks are written to separate files, read in one block.
Table read_from_file(const char *path);

// Return the columns of a RecordBatch as a vector of Arrays. This is a special
// utility function for resolving internal inconsistencies.
std::vector<std::shared_ptr<arrow::Array>>
get_columns_from_record_batch(std::shared_ptr<arrow::RecordBatch> record_batch);

// When a RecordBatch is read from a file, the read is zero-copy, and thus we
// cannot mutate the Block's data. This function copies the data into a new,
// mutable RecordBatch.
std::shared_ptr<arrow::RecordBatch> copy_record_batch(std::shared_ptr<arrow::RecordBatch> batch);

#endif //HUSTLE_OFFLINE_UTIL_H
