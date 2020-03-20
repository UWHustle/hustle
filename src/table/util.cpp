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

void evaluate_status(const arrow::Status &status, const char *function_name,
                     int line_no) {
    if (!status.ok()) {
        std::cout << "\nInvalid status: " << function_name << ", line "
                  << line_no << std::endl;
        throw std::runtime_error(status.ToString());
    }
}


std::shared_ptr<arrow::RecordBatch> copy_record_batch(
        std::shared_ptr<arrow::RecordBatch> batch) {

    arrow::Status status;
    std::vector<std::shared_ptr<arrow::ArrayData>> arraydatas;

    for (int i = 0; i < batch->num_columns(); i++) {

        auto column = batch->column(i);
        auto buffers = column->data()->buffers;
        switch (column->type_id()) {

            case arrow::Type::STRING: {
                std::shared_ptr<arrow::Buffer> offsets;
                std::shared_ptr<arrow::Buffer> data;
                status = buffers[1]->Copy(
                        0, buffers[1]->size(), &offsets);
                evaluate_status(status, __FUNCTION__, __LINE__);
                status = buffers[2]->Copy(
                        0, buffers[2]->size(), &data);
                evaluate_status(status, __FUNCTION__, __LINE__);

                arraydatas.push_back(
                        arrow::ArrayData::Make(
                                arrow::utf8(), column->length(),
                                {nullptr, offsets, data}));
                break;
            }

            case arrow::Type::BOOL: {
                std::shared_ptr<arrow::Buffer> data;
                status = buffers[1]->Copy(0, buffers[1]->size(), &data);
                evaluate_status(status, __FUNCTION__, __LINE__);

                arraydatas.push_back(
                        arrow::ArrayData::Make(
                                arrow::boolean(), column->length(),
                                {nullptr, data}));
                break;
            }
            case arrow::Type::INT64: {
                std::shared_ptr<arrow::Buffer> data;
                status = buffers[1]->Copy(
                        0, buffers[1]->size(), &data);
                evaluate_status(status, __FUNCTION__, __LINE__);

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
std::shared_ptr<Table> read_from_file(const char *path) {

    arrow::Status status;

    std::shared_ptr<arrow::io::ReadableFile> infile;
    auto result = arrow::io::ReadableFile::Open(path,
            arrow::default_memory_pool());
    if (result.ok()) {
        infile = result.ValueOrDie();
    }
    else {
        evaluate_status(result.status(), __FUNCTION__, __LINE__);
    }
    evaluate_status(status, __FUNCTION__, __LINE__);

    std::shared_ptr<arrow::ipc::RecordBatchFileReader> record_batch_reader;
    status = arrow::ipc::RecordBatchFileReader::Open(
            infile, &record_batch_reader);
    evaluate_status(status, __FUNCTION__, __LINE__);

    std::shared_ptr<arrow::RecordBatch> in_batch;
    std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;

    for (int i = 0; i < record_batch_reader->num_record_batches(); i++) {
        status = record_batch_reader->ReadRecordBatch(i, &in_batch);
        evaluate_status(status, __FUNCTION__, __LINE__);
        if (in_batch != nullptr) {
            auto batch_copy = copy_record_batch(in_batch);
            record_batches.push_back(batch_copy);
        }
    }

    return std::make_shared<Table>("table", record_batches, BLOCK_SIZE);
}

std::vector<std::shared_ptr<arrow::Array>>
get_columns_from_record_batch(
        std::shared_ptr<arrow::RecordBatch> record_batch) {

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
    status = arrow::ipc::RecordBatchFileWriter::Open(
            &*file, table.get_schema()).status();
    evaluate_status(status, __FUNCTION__, __LINE__);

    if (status.ok()) {
        record_batch_writer = arrow::ipc::RecordBatchFileWriter::Open(
                &*file, table.get_schema()).ValueOrDie();
    }

    auto blocks = table.get_blocks();

    for (int i = 0; i < blocks.size(); i++) {
        status = record_batch_writer->WriteRecordBatch(
                *blocks[i]->get_records());
        evaluate_status(status, __FUNCTION__, __LINE__);
    }
    status = record_batch_writer->Close();
    evaluate_status(status, __FUNCTION__, __LINE__);
}



int compute_fixed_record_width(std::shared_ptr<arrow::Schema> schema) {

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

//    std::fstream file;
//    file.open(path, std::ios::in);
//    if (!file.is_open()) {
//        std::__throw_runtime_error("Cannot open file.");
//    }

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

    auto test = std::make_shared<Table>("table", schema, block_size);

    auto t1 = std::chrono::high_resolution_clock::now();
    while (fgets(buf, 1024, file)) {

        // Note that the newline character is still included!
        auto buf_stripped = absl::StripTrailingAsciiWhitespace(buf);
        std::vector<absl::string_view> values = absl::StrSplit(buf_stripped,
                '|');

        for (int index : string_column_indices) {
            byte_widths[index] = values[index].length();
        }

        test->insert_record(values, byte_widths);
    }


    auto t2 = std::chrono::high_resolution_clock::now();
    std::cout << "READ TIME = " <<
    std::chrono::duration_cast<std::chrono::nanoseconds>
    (t2-t1).count
            () <<
              std::endl;
    fclose(file);
    return test;

    // TODO(nicholas):  Add a Block construct that accepts a vector of
    //  ArrayData so that you don't need to construct a RecordBatch

    // Finish the final RecordBatch
    std::shared_ptr<arrow::RecordBatch> record_batch;
    record_batch_builder->Flush(&record_batch);
    record_batches.push_back(record_batch);

    fclose(file);
//    file.close();

    return std::make_shared<Table>("table", record_batches, block_size);

//    std::vector<std::shared_ptr<arrow::ArrayData>> columns;
//
//    for (int i = 0; i<schema->num_fields(); i++) {
//        std::shared_ptr<arrow::ArrayData> out;
//        record_batch_builder->GetField(i)->FinishInternal(&out);
//        columns.push_back(out);
//    }

}

int64_t convert_slice(const char *s, size_t a, size_t b) {
    int64_t val = 0;
    while (a < b) {
        val = val * 10 + s[a++] - '0';
    }
    return val;
}