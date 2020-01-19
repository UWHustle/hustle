
#include "block.h"
#include <arrow/scalar.h>
#include <vector>
#include <iostream>
#include "util.h"

Block::Block(int id, const std::shared_ptr<arrow::Schema> &in_schema, int capacity)
        : num_rows(0), record_width(0), num_bytes(0), capacity(capacity), id(id) {

    arrow::Status status;
    status = in_schema->AddField(0, arrow::field("valid", arrow::boolean()), &schema);
    EvaluateStatus(status, __FUNCTION__, __LINE__);

    for (const auto &field : schema->fields()) {
        record_width += field->type()->layout().bit_widths[1]/8;
    }

    std::vector<std::shared_ptr<arrow::Array>> columns;

    // TODO: Initialize arrays from ArrayData (i.e. using buffers)
    // so that we can initialize to length 0 while still having valid pointers to the underlying buffers.
    // Create a scalar of each field's type, and initialize each column to the value of that scalar.
    for (const auto &field : schema->fields()) {
        std::shared_ptr<arrow::Scalar> scalar;
        std::shared_ptr<arrow::Array> column;

        switch (field->type()->id()) {
            case arrow::Type::BOOL: {
                scalar = std::make_shared<arrow::BooleanScalar>(0);
                break;
            }
            case arrow::Type::INT64: {
                scalar = std::make_shared<arrow::Int64Scalar>(0);
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

        columns.push_back(std::static_pointer_cast<arrow::Array>(column));
    }

    records = arrow::RecordBatch::Make(this->schema, 1, columns);
}

int Block::compute_num_bytes() {

    // start at i=1 to skip valid column
    for (int i=1; i<records->num_columns(); i++) {

        std::shared_ptr<arrow::Field> field =  records->schema()->field(i);

        switch (field->type()->id()) {

            case arrow::Type::STRING: {
                auto column = std::static_pointer_cast<arrow::StringArray>(records->column(i));
                num_bytes += column->value_offset(column->length());
                break;
            }
            default: {
                auto test = records->column(i);
                auto column = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(records->column(i));
                int byte_width = field->type()->layout().bit_widths[1] / 8;

                num_bytes += byte_width*column->length();
            }
        }
    }
}

// Initialize block from record batch
Block::Block(int id, std::shared_ptr<arrow::RecordBatch> record_batch, int capacity)
        : capacity(capacity), id(id), record_width(0), num_bytes(0) {

    arrow::Status status;

    for (const auto &field : record_batch->schema()->fields()) {
        record_width += field->type()->layout().bit_widths[1]/8;
    }

    num_rows = record_batch->num_rows(); // TODO: is this correct?
    records = std::move(record_batch);
    compute_num_bytes();
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
    std::cout <<"GET FREE " << valid->ToString() << std::endl;
    std::cout << "NUM ROWS = " << num_rows << std::endl;
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

    std::cout <<"AFTER SET VALID " << valid->ToString() << std::endl;

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

void Block::increment_num_bytes(unsigned int n_bytes) {
    if (num_bytes + n_bytes > capacity) {
        throw std::runtime_error("Incremented number of bytes stored in block beyond capacity");
    }
    else {
        num_bytes += n_bytes;
    }
}

int Block::get_bytes_left() {
    return capacity - num_bytes;
}

void Block::print() {

    // @TODO: The issue is that raw_data_ and raw_values_data_ are not being updated.
    // We had the correct data buffers, but GetString() fetches from the internal pointers!
    for (int row = 0; row < num_rows; row++) {
        for (int i = 0; i < records->schema()->num_fields(); i++) {

            int type = records->schema()->field(i)->type()->id();

            switch (type) {
                case arrow::Type::STRING: {
                    auto col = std::static_pointer_cast<arrow::StringArray>(records->column(i));
                    auto test = col->value_data()->data();

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

// Return true is insertion was successful, false otherwise
bool Block::insert_record(uint8_t* record, int32_t* byte_widths) {

    // This method is no longer relevant because we do not preallocate empty records.
//    int row_index = get_free_row_index();
//    if (row_index < 0) {
//        throw std::runtime_error("Cannot insert record into block. Invalid row not found.");
//    }

    int32_t record_size = 0;
    for (int i=0; i<records->num_columns()-1; i++) { // - 1 to exclude valid column
        record_size += byte_widths[i]; // does record size include valid column?
    }

    arrow::Status status;

    std::vector<arrow::Array> new_columns;

    int head = 0;
    for (int i=0; i<records->num_columns(); i++) {

        std::shared_ptr<arrow::Field> field = records->schema()->field(i);
        switch (field->type()->id()) {
            // Although BOOL is a fixed-width type, we must handle it separately, since data is stored at the
            // bit-level rather than the byte level.
            case arrow::Type::BOOL: {
                auto column = std::static_pointer_cast<arrow::BooleanArray>(records->column(i));
                auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
                        records->column(i)->data()->buffers[1]);

                // Extended the underlying buffers. This may result in copying the data.
                if (data_buffer->size() % 8 == 0) {
                    status = data_buffer->Resize(data_buffer->size() + 1);
                    data_buffer->ZeroPadding(); // Ensure the additional byte is zeroed
                    std::cout << "RESIZING VALID BUFFER" << std::endl;
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
                auto off = column->data()->offset;
                // Extended the underlying data buffer. This may result in copying the data. If data is copied, the
                // internal variables raw_data_ and raw_values_offsets_ in Array are NOT updated. Thus, calls to e.g.
                // GetString(i) which index from these variables and not directly from the buffers will return garbage
                // data.
                // Note that we use index i-1 because byte_widths does not include the byte width of the valid column.
                if (num_rows == 0) {
                    // No need to resize offsets buffer in this case
                    status = data_buffer->Resize(byte_widths[i - 1]);
                    EvaluateStatus(status, __FUNCTION__, __LINE__);
                }
                else {
                    status = offsets_buffer->Resize(offsets_buffer->size() + sizeof(int32_t));
                    EvaluateStatus(status, __FUNCTION__, __LINE__);
                    status = data_buffer->Resize(data_buffer->size() + byte_widths[i - 1]);
                    EvaluateStatus(status, __FUNCTION__, __LINE__);
                    column->data()->length++;
                }

                // Insert data
                int x = column->value_offset(num_rows);
                auto *values_data = column->data()->GetMutableValues<uint8_t>(2, column->value_offset(num_rows));
                std::memcpy(values_data, &record[head], byte_widths[i - 1]);

                // Update offsets
//                auto offsets_data = (int32_t *) column->value_offsets()->mutable_data();
                // num_rows + 1 because num_rows index tells us where the stirng starts. We want to write where the stirng ends.
                auto *offsets_data = column->data()->GetMutableValues<int32_t>(1, num_rows);
                int32_t new_offset;
                if ( num_rows == 0) new_offset = byte_widths[i - 1];
                else new_offset = offsets_data[0] + byte_widths[i - 1];
                std::memcpy(&offsets_data[1], &new_offset, sizeof(new_offset));


                head += byte_widths[i - 1];

                // Recreate Array so that it's length is consistent
                std::shared_ptr<arrow::ArrayData> new_arraydata;
                if (num_rows == 0) {
                    new_arraydata = arrow::ArrayData::Make(arrow::utf8(), 1,{nullptr, offsets_buffer, data_buffer});
                }
                else {
                    new_arraydata = arrow::ArrayData::Make(arrow::utf8(), num_rows+1,{nullptr, offsets_buffer, data_buffer});
                }

                auto new_array = arrow::MakeArray(new_arraydata);
                auto c = std::static_pointer_cast<arrow::StringArray>(new_array);

                break;
            }
            // This works with any fixed-width type, but for now, I specify INT64
            case arrow::Type::INT64: {

                std::shared_ptr<arrow::Array> column = records->column(i);

                // TODO: buffers still need to be resized in this case!
                auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(column->data()->buffers[1]);
                status = data_buffer->Resize(data_buffer->size() + byte_widths[i-1]);
                EvaluateStatus(status, __FUNCTION__, __LINE__);
                // Assumes we the underlying data has fixed length. Note that we do not need to downcast
                // the array. We are simply adding values to the array; we are not performing any operations
                // with the data.
                column = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(records->column(i));
                // GetMutableValues() returns a pointer to the ith underlying buffer. Again, the 0th buffer
                // corresponds to the null bitmap, while the 1st buffer corresponds to the data.
                // This allows us to access the underlying data arrow without casting to a concrete array type.
                auto* dest = column->data()->GetMutableValues<uint8_t>(1, num_rows*byte_widths[i-1]);
                std::memcpy(dest, &record[head], byte_widths[i-1]);

                head += byte_widths[i-1];

                if (num_rows > 0) {
                    column->data()->length++;
                }
                break;
            }

            default:
                throw std::logic_error(
                        std::string("Inserting unsupported type: ") +
                        field->type()->ToString());
        }
    }
    increment_num_bytes(head);
    increment_num_rows();

    records = arrow::RecordBatch::Make(records->schema(), 1, get_columns_from_record_batch(records));
}



