
#include "block.h"
#include <arrow/scalar.h>
#include <vector>
#include <iostream>
#include "util.h"

Block::Block(int id, const std::shared_ptr<arrow::Schema> &in_schema, int capacity)
        : num_rows(0), record_width(0), capacity(capacity), id(id) {

    arrow::Status status;
    status = in_schema->AddField(0, arrow::field("valid", arrow::boolean()), &schema);
    EvaluateStatus(status, __FUNCTION__, __LINE__);

    for (const auto &field : schema->fields()) {
        record_width += field->type()->layout().bit_widths[1]/8;
    }

    std::vector<std::shared_ptr<arrow::Array>> columns;

    // Create a scalar of each field's type, and initialize each column to the value of that scalar.
    for (const auto &field : schema->fields()) {
        std::shared_ptr<arrow::Scalar> scalar;
        std::shared_ptr<arrow::Array> column;

        switch (field->type()->id()) {
            case arrow::Type::BOOL: {
                scalar = std::make_shared<arrow::BooleanScalar>();
                break;
            }
            case arrow::Type::INT64: {
                scalar = std::make_shared<arrow::Int64Scalar>();
                break;
            }
            case arrow::Type::FIXED_SIZE_BINARY: {
                int byte_width =
                        std::static_pointer_cast<arrow::FixedSizeBinaryType>(field->type())
                                ->byte_width();
                std::string data(byte_width, '*');
                scalar = std::make_shared<arrow::FixedSizeBinaryScalar>(
                        arrow::Buffer::FromString(std::move(data)),
                        arrow::fixed_size_binary(byte_width), false);

                break;
            }
            case arrow::Type::STRING:{
                scalar = std::make_shared<arrow::StringScalar>();

                break;
            }
            default:
                throw std::logic_error(
                        std::string("Block created with unsupported type: ") +
                        field->type()->ToString());
        }

        status = arrow::MakeArrayFromScalar(*scalar, (int) capacity/record_width, &column);
        EvaluateStatus(status, __FUNCTION__, __LINE__);

        columns.push_back(std::static_pointer_cast<arrow::PrimitiveArray>(column));
    }

    records = arrow::RecordBatch::Make(this->schema, capacity/record_width, columns);
}


// Initialize block from record batch
Block::Block(int id, std::shared_ptr<arrow::RecordBatch> record_batch, int capacity)
        : capacity(capacity), id(id), record_width(0) {

    arrow::Status status;

    for (const auto &field : record_batch->schema()->fields()) {
        record_width += field->type()->layout().bit_widths[1]/8;
    }

    num_rows = record_batch->num_rows(); // TODO: is this correct?
    records = std::move(record_batch);

}

const std::shared_ptr<arrow::RecordBatch> Block::get_view() {
    return records;
}

int Block::get_id() { return id; }

bool Block::is_full() { return (num_rows+1)*record_width > capacity; }

std::shared_ptr<arrow::Array> Block::get_column(int column_index) {
    return records->column(column_index);
}

std::shared_ptr<arrow::Array>
Block::get_column_by_name(const std::string &name) {
    return records->GetColumnByName(name);
}

int Block::get_free_row_index() {
    auto valid = std::static_pointer_cast<arrow::BooleanArray>(records->column(0));

    // Determine the first available row that can be used to store the data.
    for (int row_index = 0; row_index < capacity; ++row_index) {
        if (!valid->Value(row_index)) {
            return row_index;
        }
    }
    return -1;
}

bool Block::get_valid(unsigned int row_index) {
    auto valid = std::static_pointer_cast<arrow::BooleanArray>(records->column(0));
    return valid->Value(row_index);
}

void Block::set_valid(unsigned int row_index, bool val) {
    auto valid = std::static_pointer_cast<arrow::BooleanArray>(records->column(0));
    auto* data = valid->values()->mutable_data();
    if (val) {
        data[row_index / 8] |= (1u << (row_index % 8u));
    }
    else {
        data[row_index / 8] &= (1u << (row_index % 8u));
    }

}

void Block::increment_num_rows() {
    if (num_rows++ == capacity) {
        throw std::runtime_error("Incremented number of rows beyond capacity");
    }
}

void Block::decrement_num_rows() {
    if (num_rows-- == 0) {
        throw std::runtime_error("Decremented number of rows below zero");
    }
}

void Block::print() {

    for (int row = 0; row < records->num_rows(); row++) {
        for (int i = 0; i < records->schema()->num_fields(); i++) {

            int type = records->schema()->field(i)->type()->id();

            switch (type) {
                case arrow::Type::STRING: {
                    auto col = std::static_pointer_cast<arrow::StringArray>(records->column(i));
                    std::cout << col->GetString(row) << "\t";
                    break;
                }
                case arrow::Type::type::FIXED_SIZE_BINARY: {
                    auto col = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(records->column(i));
                    std::cout << col->GetString(row) << "\t";
                    break;
                }
                case arrow::Type::type::INT64: {
                    auto col = std::static_pointer_cast<arrow::Int64Array>(records->column(i));
                    std::cout << col->Value(row) << "\t";
                    break;
                }
                case arrow::Type::BOOL: {
                    auto col = std::static_pointer_cast<arrow::BooleanArray>(records->column(i));
                    std::cout << col->Value(row) << "\t";
                    break;
                }
            }
        }
        std::cout << std::endl;
    }
}

// Assumes fixed-size binary data
void Block::insert_record(uint8_t* record, int n_bytes) {

    int row_index = get_free_row_index();
    if (row_index < 0) {
        throw std::runtime_error("Cannot insert record into block. Block is full.");
    }

    int head = 0;
    while (head < n_bytes){
        // Skip the valid column
        for (int i=1; i<records->num_columns(); i++) {

            std::shared_ptr<arrow::Field> field = records->schema()->field(i);
            std::shared_ptr<arrow::Array> column = records->column(i);
            // index 0 corresponds to null bitmap, index 1 corresponds to data
            int byte_width = field->type()->layout().bit_widths[1] / 8;

            // Assumes we the underlying data has fixed length. Note that we do not need to downcast
            // the array. We are simply adding values to the array; we are not performing any operations
            // with the data.
            column = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(records->column(i));
            // GetMutableValues() returns a pointer to the ith underlying buffer. Again, the 0th buffer
            // corresponds to the null bitmap, while the 1st buffer corresponds to the data.
            // This allows us to access the underlying data arrow without casting to a concrete array type.
            auto* dest = column->data()->GetMutableValues<uint8_t>(1, num_rows*byte_width);
            std::memcpy(dest, &record[head], byte_width);

//            auto* null_bitmap = column->null_bitmap()->mutable_data();
//            null_bitmap[num_rows / 8] |= (1u << (num_rows % 8u));
            head += byte_width;
        }
    }
    set_valid(num_rows, true);
    increment_num_rows();
}



