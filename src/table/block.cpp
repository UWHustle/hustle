
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
                scalar = std::make_shared<arrow::StringScalar>(arrow::Buffer::FromString("placeholder"));

                break;
            }
            default:
                throw std::logic_error(
                        std::string("Block created with unsupported type: ") +
                        field->type()->ToString());
        }

        status = arrow::MakeArrayFromScalar(*scalar, 1, &column);
        EvaluateStatus(status, __FUNCTION__, __LINE__);

        columns.push_back(std::static_pointer_cast<arrow::PrimitiveArray>(column));
    }

    records = arrow::RecordBatch::Make(this->schema, 1, columns);
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
    for (int row_index = 0; row_index < num_rows+1; ++row_index) {
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

    for (int row = 0; row < num_rows; row++) {
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
                default: {
                    throw std::logic_error(
                            std::string("Block created with unsupported type: ") +
                            records->schema()->field(i)->type()->ToString());
                }
            }
        }
        std::cout << std::endl;
    }
}

void Block::insert_record(uint8_t* record, int32_t* byte_widths) {

    int row_index = get_free_row_index();
    if (row_index < 0) {
        throw std::runtime_error("Cannot insert record into block. Block is full.");
    }

    arrow::Status status;

    int head = 0;
    for (int i=0; i<records->num_columns(); i++) {

        std::shared_ptr<arrow::Field> field = records->schema()->field(i);

        switch (field->type()->id()) {
            case arrow::Type::BOOL: {
                auto column = std::static_pointer_cast<arrow::BooleanArray>(records->column(i));
                auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
                        records->column(i)->data()->buffers[1]);

                // Extended the underlying buffers. This may result in copying the data.
                if (data_buffer->size() % 8 == 0) {
                    status = data_buffer->Resize(data_buffer->size() + 1);
                    data_buffer->ZeroPadding(); // Ensure the additional byte is zeroed
                }

                // Recall that we initialized our column arrays with length 1 so that the underlying buffers are
                // properly initialized. So, if num_rows == 0, we do not need to update the length of the column data.
                if (num_rows > 0) {
                    column->data()->length++;
                }

                set_valid(num_rows, true);

                break;
            }

            case arrow::Type::STRING: {
                auto column = std::static_pointer_cast<arrow::StringArray>(records->column(i));

                auto offsets_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(column->data()->buffers[1]);
                auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(column->data()->buffers[2]);

                // Extended the underlying data buffer. This may result in copying the data.
                // Note that we use index i-1 because byte_widths does not include the byte width of the valid column.
                status = data_buffer->Resize(data_buffer->size() + byte_widths[i - 1]);
                EvaluateStatus(status, __FUNCTION__, __LINE__);
                status = offsets_buffer->Resize(offsets_buffer->size() + sizeof(int32_t));
                EvaluateStatus(status, __FUNCTION__, __LINE__);

                // Note that we do not need to call data_buffer->ZeroPadding() because we will be memcpy-ing over
                // the added bytes anyways

                // Recall that we initialized our column arrays with length 1 so that the underlying buffers are
                // properly initialized. So, if num_rows == 0, we do not need to update the length of the column data.
                if (num_rows > 0) {
                    column->data()->length++;
                }

                // Insert data
                auto *values_data = column->data()->GetMutableValues<uint8_t>(2, column->value_offset(num_rows));
                std::memcpy(values_data, &record[head], byte_widths[i - 1]);

                // Update offsets
                auto offsets_data = (int *) column->value_offsets()->mutable_data();
                offsets_data[num_rows + 1] = offsets_data[num_rows] + byte_widths[i - 1];

                head += byte_widths[i - 1];

                break;
            }
            default:
                throw std::logic_error(
                        std::string("Block created with unsupported type: ") +
                        field->type()->ToString());
        }
    }

    increment_num_rows();
}



