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
Table read_from_file(const char *path) {

    arrow::Status status;

    std::shared_ptr<arrow::io::ReadableFile> infile;
    status = arrow::io::ReadableFile::Open(path, &infile);
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

    return Table("table", record_batches, BLOCK_SIZE);
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

    std::shared_ptr<arrow::Schema> write_schema;
    status = table.get_schema()->AddField(0, arrow::field(
            "valid", arrow::boolean()), &write_schema);
    evaluate_status(status, __FUNCTION__, __LINE__);

    status = arrow::io::FileOutputStream::Open(path, &file);
    evaluate_status(status, __FUNCTION__, __LINE__);
    status = arrow::ipc::RecordBatchFileWriter::Open(
            &*file, table.get_schema()).status();
    evaluate_status(status, __FUNCTION__, __LINE__);

    if (status.ok()) {
        record_batch_writer = arrow::ipc::RecordBatchFileWriter::Open(
                &*file, write_schema).ValueOrDie();
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

    for (auto field : schema->fields()) {

        switch (field->type()->id()) {
            case arrow::Type::STRING: {
                break;
            }
            case arrow::Type::BOOL:
            case arrow::Type::INT64: {
                fixed_width += field->type()->layout().bit_widths[1] / 8;
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

Table read_from_csv_file(const char* path, std::shared_ptr<arrow::Schema>
        schema) {

    arrow::Status status;

    // Add valid column
    status = schema->AddField(0,arrow::field("valid", arrow::boolean()),
            &schema);

    // RecordBatchBuilder initializes ArrayBuilders for each field in schema
    std::unique_ptr<arrow::RecordBatchBuilder> record_batch_builder;
    arrow::RecordBatchBuilder::Make(schema,arrow::default_memory_pool(),
                                    &record_batch_builder);

    // We will output a table constructed from these RecordBatches
    std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;

    std::fstream file;
    file.open(path, std::ios::in);

    if (!file.is_open()) {
        std::__throw_runtime_error("Cannot open file.");
    }

    int num_bytes = 0;
    int variable_record_width;
    int fixed_record_width = compute_fixed_record_width(schema);

    std::vector<int> string_column_indices;

    for (int i = 1; i < schema->num_fields(); i++) {
        switch (schema->field(i)->type()->id()) {
            case arrow::Type::STRING: {
                string_column_indices.push_back(i);
                break;
            }
            default: {
                // TODO(nicholas): something here?
            }
        }
    }

    std::string line;

    while (getline(file, line)) {

        std::vector<std::string> values = absl::StrSplit(line, '|');

        variable_record_width = 0;
        for (int index : string_column_indices) {
            variable_record_width += values[index-1].length();
        }

        // If adding this record will make the current RecordBatch exceed our
        // block size, then start building a new RecordBatch.
        if (num_bytes + fixed_record_width + variable_record_width > BLOCK_SIZE) {
            std::shared_ptr<arrow::RecordBatch> record_batch;
            record_batch_builder->Flush(&record_batch);
            record_batches.push_back(record_batch);
            num_bytes = 0;
        }

        // Update valid column
        auto builder = record_batch_builder->
                GetFieldAs<arrow::BooleanBuilder>(0);
        builder->Append(true);

        // Start at i=1 to skip the valid column
        for (int i = 1; i < schema->num_fields(); i++) {

            // Use index i-1 when indexing values, because it does not include
            // valid column.
            switch (schema->field(i)->type()->id()) {
                case arrow::Type::STRING: {
                    auto builder = record_batch_builder->
                            GetFieldAs<arrow::StringBuilder>(i);
                    builder->Append(values[i-1]);
                    num_bytes += values[i-1].length();
                    break;
                }
                case arrow::Type::INT64: {
                    int64_t out;
                    absl::SimpleAtoi(values[i-1], &out);
                    auto builder = record_batch_builder->
                            GetFieldAs<arrow::Int64Builder>(i);
                    builder->Append(out);
                    num_bytes += sizeof(out);
                    break;
                }
                default: {
                    throw std::logic_error(
                            std::string("Cannot load data of type ") +
                            schema->field(i)->type()->ToString());
                }
            }
        }
    }

    // TODO(nicholas):  Add a Block construct that accepts a vector of
    //  ArrayData so that you don't need to construct a RecordBatch

    // Finish the final RecordBatch
    std::shared_ptr<arrow::RecordBatch> record_batch;
    record_batch_builder->Flush(&record_batch);
    record_batches.push_back(record_batch);

    file.close();

    return Table("table", record_batches, BLOCK_SIZE);

//    std::vector<std::shared_ptr<arrow::ArrayData>> columns;
//
//    for (int i = 0; i<schema->num_fields(); i++) {
//        std::shared_ptr<arrow::ArrayData> out;
//        record_batch_builder->GetField(i)->FinishInternal(&out);
//        columns.push_back(out);
//    }

}