
#ifndef HUSTLE_OFFLINE_UTIL_H
#define HUSTLE_OFFLINE_UTIL_H

#include <string>
#include <arrow/memory_pool.h>
#include <arrow/table.h>
#include "table.h"

Table
build_table(const std::string& file_path, arrow::MemoryPool *pool, std::vector<std::string> &column_names, std::vector<std::shared_ptr<arrow::DataType>> &column_types, int block_size) ;
void EvaluateStatus(const arrow::Status& status, const char* function_name, int line_no);
void write_to_file(const char* path, Table &table);
std::shared_ptr<arrow::RecordBatch> fix_last_batch(std::shared_ptr<arrow::RecordBatch> last_batch, int records_per_block);
Table test(std::shared_ptr<arrow::Table>);
Table read_from_file(const char* path);
#endif //HUSTLE_OFFLINE_UTIL_H
