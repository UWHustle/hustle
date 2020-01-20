
#include "block.h"
#include <arrow/scalar.h>
#include <vector>
#include <iostream>
#include "util.h"

Block::Block(int id, const std::shared_ptr<arrow::Schema> &in_schema, int capacity)
        : num_rows(0), num_bytes(0), capacity(capacity), id(id) {

    arrow::Status status;

    // Add valid column to the schema
    status = in_schema->AddField(0, arrow::field("valid", arrow::boolean()), &schema);
    EvaluateStatus(status, __FUNCTION__, __LINE__);

    std::vector<std::shared_ptr<arrow::ArrayData>> columns;

    for (const auto &field : schema->fields()) {

        std::shared_ptr<arrow::ArrayData> array_data;
        std::shared_ptr<arrow::ResizableBuffer> data;

        // Buffers are always padded to multiples of 64 bytes
        status = arrow::AllocateResizableBuffer(64, &data);
        EvaluateStatus(status, __FUNCTION__, __LINE__);
        data->ZeroPadding();

        switch (field->type()->id()) {

            case arrow::Type::STRING: {
                std::shared_ptr<arrow::ResizableBuffer> offsets;
                // Although the data buffer is empty, the offsets buffer should still contain the offset of the
                // first element. This offset is always 0.
                status = arrow::AllocateResizableBuffer(sizeof(int32_t), &offsets);
                EvaluateStatus(status, __FUNCTION__, __LINE__);

                // Make sure the first offset value is set to 0, otherwise we're in trouble.
                int32_t initial_offset = 0;
                uint8_t* offsets_data = offsets->mutable_data();
                std::memcpy(&offsets_data[0], &initial_offset, sizeof(initial_offset));

                columns.push_back(arrow::ArrayData::Make(field->type(), 0, {nullptr, offsets, data}));
                break;
            }

            case arrow::Type::BOOL:
            case arrow::Type::INT64: {
                columns.push_back(arrow::ArrayData::Make(field->type(), 0, {nullptr, data}));
                break;
            }
            default: {
                throw std::logic_error(
                        std::string("Block created with unsupported type: ") +
                        field->type()->ToString());
            }
        }
    }

    records = arrow::RecordBatch::Make(this->schema, 0, columns);
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
            case arrow::Type::BOOL:
            case arrow::Type::INT64: {
                auto column = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(records->column(i));
                int byte_width = field->type()->layout().bit_widths[1] / 8;
                num_bytes += byte_width*column->length();
                break;
            }
            default: {
                throw std::logic_error(
                        std::string("Cannot compute fixed record width. Unsupported type: ") +
                        field->type()->ToString());
            }
        }
    }
}

// Initialize block from record batch
Block::Block(int id, std::shared_ptr<arrow::RecordBatch> record_batch, int capacity)
        : capacity(capacity), id(id), num_bytes(0) {

    num_rows = record_batch->num_rows();
    records = std::move(record_batch);
    compute_num_bytes();
}

const std::shared_ptr<arrow::RecordBatch> Block::get_view() {
    return records;
}

int Block::get_id() { return id; }

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

    // TODO: valid bit is being correctly set, but after the second insertion, we get
//    std::cout <<"AFTER SET VALID " << valid->ToString() << std::endl;

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
                if (data_buffer->size() % 8 == 0 && num_rows > 0) {
                    status = data_buffer->Resize(data_buffer->size() + 1);
                    data_buffer->ZeroPadding(); // Ensure the additional byte is zeroed
                }
                column->data()->length++;
                set_valid(num_rows, true);

                break;
            }

            case arrow::Type::STRING: {
                auto column = std::static_pointer_cast<arrow::StringArray>(records->column(i));

                auto offsets_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(column->data()->buffers[1]);
                auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(column->data()->buffers[2]);

                // Extended the underlying data buffer. This may result in copying the data. If data is copied, the
                // internal variables raw_data_ and raw_values_offsets_ in Array are NOT updated. Thus, calls to e.g.
                // GetString(i) which index from these variables and not directly from the buffers will return garbage
                // data.
                // Note that we use index i-1 because byte_widths does not include the byte width of the valid column.
                status = offsets_buffer->Resize(offsets_buffer->size() + sizeof(int32_t));
                EvaluateStatus(status, __FUNCTION__, __LINE__);
                status = data_buffer->Resize(data_buffer->size() + byte_widths[i - 1]);
                EvaluateStatus(status, __FUNCTION__, __LINE__);
                column->data()->length++;

                // Update offsets. Should be done before data updates, since data updates reference the offsets
//                auto offsets_data = (int32_t *) column->value_offsets()->mutable_data();
                // num_rows + 1 because num_rows index tells us where the stirng starts. We want to write where the stirng ends.
                auto *offsets_data = column->data()->GetMutableValues<int32_t>(1, 0);
                int32_t new_offset;
                new_offset = offsets_data[num_rows] + byte_widths[i - 1];
                std::memcpy(&offsets_data[num_rows+1], &new_offset, sizeof(new_offset));

                // Insert data
                int32_t x = column->value_offset(num_rows); // sometimes this value is garbage???
                auto *values_data = column->data()->GetMutableValues<uint8_t>(2, column->value_offset(num_rows));
                std::memcpy(values_data, &record[head], byte_widths[i - 1]);


                head += byte_widths[i - 1];

                // Recreate Array so that it's length is consistent
                std::shared_ptr<arrow::ArrayData> new_arraydata;
                new_arraydata = arrow::ArrayData::Make(arrow::utf8(), num_rows+1,{nullptr, offsets_buffer, data_buffer});


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
                column->data()->length++;
                break;
            }

            default:
                throw std::logic_error(
                        std::string("Cannot insert tuple with unsupported type: ") +
                        field->type()->ToString());
        }
    }
    increment_num_bytes(head);
    increment_num_rows();

    records = arrow::RecordBatch::Make(records->schema(), num_rows, get_columns_from_record_batch(records));
}



