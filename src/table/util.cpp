

#include "util.h"
#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <iostream>
#include <arrow/scalar.h>

#include "table.h"
#include "block.h"


void EvaluateStatus(const arrow::Status& status, const char* function_name, int line_no) {
    if (!status.ok()) {
        std::cout << "\nInvalid status: " << function_name << ", line " << line_no << std::endl;
        throw std::runtime_error(status.ToString());
    }
}

std::shared_ptr<arrow::RecordBatch> copy_record_batch(std::shared_ptr<arrow::RecordBatch> batch) {

    arrow::Status status;
    std::vector<std::shared_ptr<arrow::ArrayData>> arraydatas;

    for (int i=0; i<batch->num_columns(); i++) {

        auto column = batch->column(i);
        auto buffers = column->data()->buffers;
        switch (column->type_id()) {

            case arrow::Type::STRING: {
                std::shared_ptr<arrow::Buffer> offsets;
                std::shared_ptr<arrow::Buffer> data;
                status = buffers[1]->Copy(0, buffers[1]->size(), &offsets);
                EvaluateStatus(status, __FUNCTION__, __LINE__);
                status = buffers[2]->Copy(0, buffers[2]->size(), &data);
                EvaluateStatus(status, __FUNCTION__, __LINE__);

                arraydatas.push_back(arrow::ArrayData::Make(arrow::utf8(), column->length(), {nullptr, offsets, data}));
                break;
            }

            case arrow::Type::BOOL: {
                std::shared_ptr<arrow::Buffer> data;
                status = buffers[1]->Copy(0, buffers[1]->size(), &data);
                EvaluateStatus(status, __FUNCTION__, __LINE__);

                arraydatas.push_back(arrow::ArrayData::Make(arrow::boolean(), column->length(), {nullptr, data}));
                break;
            }
            case arrow::Type::INT64: {
                std::shared_ptr<arrow::Buffer> data;
                status = buffers[1]->Copy(0, buffers[1]->size(), &data);
                EvaluateStatus(status, __FUNCTION__, __LINE__);

                arraydatas.push_back(arrow::ArrayData::Make(arrow::int64(), column->length(), {nullptr, data}));
                break;
            }
            default: {
                throw std::logic_error(
                        std::string("Cannot copy data of type ") +
                        column->type()->ToString());
            }
        }
    }

    return arrow::RecordBatch::Make(batch->schema(), batch->num_rows(), arraydatas);

}

Table read_from_file(const char* path) {

    arrow::Status status;

    std::shared_ptr<arrow::io::ReadableFile> infile;
    status = arrow::io::ReadableFile::Open(path, &infile);
    EvaluateStatus(status, __FUNCTION__, __LINE__);

    std::shared_ptr<arrow::ipc::RecordBatchFileReader> record_batch_reader;
    status = arrow::ipc::RecordBatchFileReader::Open(infile, &record_batch_reader);
    EvaluateStatus(status, __FUNCTION__, __LINE__);

    std::shared_ptr<arrow::RecordBatch> in_batch;
    std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;

   std::cout << record_batch_reader << std::endl;
    for (int i=0; i< record_batch_reader->num_record_batches(); i++) {

        if(record_batch_reader->ReadRecordBatch(i, &in_batch).ok() && in_batch != nullptr) {
            auto batch_copy = copy_record_batch(in_batch);
            record_batches.push_back(batch_copy);
        }

    }

    int record_width = 0;
    // Assume that the first batch is the same size as all other batches.
    for (const auto &field : record_batches[0]->schema()->fields()) {
        record_width += field->type()->layout().bit_widths[1]/8;
    }

    return Table("table", record_batches, BLOCK_SIZE);
}

std::vector<std::shared_ptr<arrow::Array>>
get_columns_from_record_batch(std::shared_ptr<arrow::RecordBatch> record_batch) {

    std::vector<std::shared_ptr<arrow::Array>> columns;

    for (int i=0; i<record_batch->num_columns(); i++) {
        columns.push_back(record_batch->column(i));
    }

    return columns;
}

void write_to_file(const char* path, Table &table) {

    std::shared_ptr<arrow::io::FileOutputStream> file;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> record_batch_writer;
    arrow::Status status;

    std::shared_ptr<arrow::Schema> write_schema;
    status = table.get_schema()->AddField(0, arrow::field("valid", arrow::boolean()), &write_schema);
    EvaluateStatus(status, __FUNCTION__, __LINE__);

    status = arrow::io::FileOutputStream::Open(path,&file);
    EvaluateStatus(status, __FUNCTION__, __LINE__);
    status = arrow::ipc::RecordBatchFileWriter::Open(&*file, table.get_schema()).status();
    EvaluateStatus(status, __FUNCTION__, __LINE__);

    if (status.ok()) {
        record_batch_writer = arrow::ipc::RecordBatchFileWriter::Open(&*file, write_schema).ValueOrDie();
    }

    auto blocks = table.get_blocks();

    for (int i=0; i<blocks.size(); i++) {
        auto record_batch = blocks[i]->get_view();

        auto correctly_sized_batch = arrow::RecordBatch::Make(
                record_batch->schema(),
                blocks[i]->num_rows,
                get_columns_from_record_batch(record_batch));

        status = record_batch_writer->WriteRecordBatch(*correctly_sized_batch);
        EvaluateStatus(status, __FUNCTION__, __LINE__);
    }
    status = record_batch_writer->Close();
    EvaluateStatus(status, __FUNCTION__, __LINE__);
}