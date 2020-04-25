#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <iostream>
#include <fstream>
#include <arrow/scalar.h>
#include "absl/strings/str_split.h"
#include "absl/strings/numbers.h"

#include "table.h"
#include "block.h"
#include "util.h"

void evaluate_status(const arrow::Status &status, const char *function_name,
                     int line_no) {
    if (!status.ok()) {
        std::cout << "\nInvalid status: " << function_name << ", line "
                  << line_no << std::endl;
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

                arraydatas.push_back(
                        arrow::ArrayData::Make(
                                arrow::utf8(), column->length(),
                                {nullptr, offsets, data}));
                break;
            }

            case arrow::Type::BOOL: {
                std::shared_ptr<arrow::Buffer> data;
                auto result = buffers[1]->CopySlice(0, buffers[1]->size());
                evaluate_status(result.status(), __FUNCTION__, __LINE__);
                data = result.ValueOrDie();

                arraydatas.push_back(
                        arrow::ArrayData::Make(
                                arrow::boolean(), column->length(),
                                {nullptr, data}));
                break;
            }
            case arrow::Type::INT64: {
                std::shared_ptr<arrow::Buffer> data;
                auto result = buffers[1]->CopySlice(0, buffers[1]->size());
                evaluate_status(result.status(), __FUNCTION__, __LINE__);
                data = result.ValueOrDie();

                arraydatas.push_back(
                        arrow::ArrayData::Make(
                                arrow::int64(), column->length(),
                                {nullptr, data}));
                break;
            }
            default: {
                throw std::logic_error(
                        std::string("Cannot copy data of type ") +
                        column->type()->ToString());
            }
        }
    }

    return arrow::RecordBatch::Make(
            batch->schema(), batch->num_rows(), arraydatas);

}

// TOOO(nicholas): Distinguish between reading blocks we intend to mutate vs.
// reading blocks we do not intend to mutate.
std::shared_ptr<Table> read_from_file(const char *path, bool read_only) {

    arrow::Status status;
    int x = 0;
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

    for (int i = 0; i < record_batch_reader->num_record_batches(); i++) {
        auto result3 = record_batch_reader->ReadRecordBatch(i);
        evaluate_status(result3.status(), __FUNCTION__, __LINE__);
        in_batch = result3.ValueOrDie();
        if (in_batch != nullptr) {
            if (read_only) {
                record_batches.push_back(in_batch);
            }
            else {
                auto batch_copy = copy_record_batch(in_batch);
                record_batches.push_back(batch_copy);
            }
        }
    }

    return std::make_shared<Table>("table", record_batches, BLOCK_SIZE);
}

std::vector<std::shared_ptr<arrow::Array>>
get_columns_from_record_batch(
        const std::shared_ptr<arrow::RecordBatch>& record_batch) {

    std::vector<std::shared_ptr<arrow::Array>> columns;

    for (int i = 0; i < record_batch->num_columns(); i++) {
        columns.push_back(record_batch->column(i));
    }

    return columns;
}

void write_to_file(const char *path, Table &table) {

    std::shared_ptr<arrow::io::FileOutputStream> file;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> record_batch_writer;
    arrow::Status status;

    evaluate_status(status, __FUNCTION__, __LINE__);

    auto result = arrow::io::FileOutputStream::Open(path, false);
    if (result.ok()) {
        file = result.ValueOrDie();
    }
    else {
        evaluate_status(result.status(), __FUNCTION__, __LINE__);
    }
    evaluate_status(status, __FUNCTION__, __LINE__);
    auto result2 = arrow::ipc::NewFileWriter(
            file.get(), table.get_schema());
    evaluate_status(result2.status(), __FUNCTION__, __LINE__);
    record_batch_writer = result2.ValueOrDie();

    auto blocks = table.get_blocks();

    for (int i = 0; i < blocks.size(); i++) {
        // IMPORTANT: The buffer size must be consistent with the ArrayData
        // length, or else data will not be properly written to file.
        blocks[i]->truncate_buffers();
        status = record_batch_writer->WriteRecordBatch(
                *blocks[i]->get_records());
        evaluate_status(status, __FUNCTION__, __LINE__);
    }
    status = record_batch_writer->Close();
    evaluate_status(status, __FUNCTION__, __LINE__);
}



int compute_fixed_record_width(const std::shared_ptr<arrow::Schema>& schema) {

    int fixed_width = 0;

    for (auto &field : schema->fields()) {

        switch (field->type()->id()) {
            case arrow::Type::STRING: {
                break;
            }
            case arrow::Type::BOOL: {
                break;
            }
            case arrow::Type::DOUBLE:
            case arrow::Type::INT64: {
                fixed_width += sizeof(int64_t);
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

std::shared_ptr<Table> read_from_csv_file(const char* path,
        std::shared_ptr<arrow::Schema>
        schema, int block_size) {

    arrow::Status status;

    // RecordBatchBuilder initializes ArrayBuilders for each field in schema
    std::unique_ptr<arrow::RecordBatchBuilder> record_batch_builder;
    status = arrow::RecordBatchBuilder::Make(schema,
            arrow::default_memory_pool(), &record_batch_builder);
    evaluate_status(status, __FUNCTION__, __LINE__);
    record_batch_builder->SetInitialCapacity(8192);

    // We will output a table constructed from these RecordBatches
    std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;

    FILE * file;
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
            default: {
                // TODO(nicholas) this will not properly handle boolean values!
                byte_widths[i] = sizeof(int64_t);
                // TODO(nicholas): something here?
            }
        }
    }

    std::string line;

    auto out_table = std::make_shared<Table>("table", schema, block_size);

    while (fgets(buf, 1024, file)) {

        // Note that the newline character is still included!
        auto buf_stripped = absl::StripTrailingAsciiWhitespace(buf);
        std::vector<absl::string_view> values = absl::StrSplit(buf_stripped,
                '|');

        for (int index : string_column_indices) {
            byte_widths[index] = values[index].length();
        }

        out_table->insert_record(values, byte_widths);
    }
    return out_table;
}

std::shared_ptr<arrow::Schema> make_schema(
        const hustle::catalog::TableSchema& catalog_schema) {

    arrow::Status status;
    arrow::SchemaBuilder schema_builder;

    std::shared_ptr<arrow::Field> field;
    
    for (auto &col : catalog_schema.getColumns()) {
        switch(col.getHustleType()) {

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