#include "block.h"
#include <arrow/scalar.h>
#include <vector>
#include <iostream>
#include "util.h"

Block::Block(int id, const std::shared_ptr<arrow::Schema> &in_schema, int capacity)
        : num_rows(0), num_bytes(0), capacity(capacity), id(id) {

    arrow::Status status;

    // Add the valid column to the schema
    status = in_schema->AddField(0,
            arrow::field("valid", arrow::boolean()),
            &schema);
    evaluate_status(status, __FUNCTION__, __LINE__);

    std::vector<std::shared_ptr<arrow::ArrayData>> columns;


    for (const auto &field : schema->fields()) {

        // Empty ArrayData should be constructed using the constructor that
        // accepts buffers as parameters. Other constructors will initialize
        // empty ArrayData (and Array) with nullptrs, making it impossible to
        // insert data.
        std::shared_ptr<arrow::ArrayData> array_data;
        std::shared_ptr<arrow::ResizableBuffer> data;

        // Buffers are always padded to multiples of 64 bytes
        status = arrow::AllocateResizableBuffer(64, &data);
        evaluate_status(status, __FUNCTION__, __LINE__);
        data->ZeroPadding();

        switch (field->type()->id()) {

            case arrow::Type::STRING: {
                std::shared_ptr<arrow::ResizableBuffer> offsets;
                // Although the data buffer is empty, the offsets buffer should
                // still contain the offset of the first element.
                status = arrow::AllocateResizableBuffer(sizeof(int32_t), &offsets);
                evaluate_status(status, __FUNCTION__, __LINE__);

                // Make sure the first offset value is set to 0
                int32_t initial_offset = 0;
                uint8_t *offsets_data = offsets->mutable_data();
                std::memcpy(&offsets_data[0], &initial_offset, sizeof(initial_offset));

                // Initialize null bitmap buffer to nullptr, since we currently don't use it.
                columns.push_back(arrow::ArrayData::Make(field->type(), 0,
                        {nullptr, offsets, data}));
                break;
            }

            case arrow::Type::BOOL:
            case arrow::Type::INT64: {
                // Initialize null bitmap buffer to nullptr, since we currently don't use it.
                columns.push_back(arrow::ArrayData::Make(field->type(), 0,
                        {nullptr, data}));
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

    // Start at i=1 to skip valid column
    for (int i = 1; i < records->num_columns(); i++) {

        std::shared_ptr<arrow::Field> field = records->schema()->field(i);

        switch (field->type()->id()) {

            case arrow::Type::STRING: {
                auto column = std::static_pointer_cast<arrow::StringArray>(
                        records->column(i));
                num_bytes += column->value_offset(column->length());
                break;
            }
            case arrow::Type::BOOL:
            case arrow::Type::INT64: {
                auto column = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(
                        records->column(i));
                // buffer at index 1 is the data buffer.
                int byte_width = field->type()->layout().bit_widths[1] / 8;
                num_bytes += byte_width * column->length();
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

Block::Block(int id, std::shared_ptr<arrow::RecordBatch> record_batch, int capacity)
        : capacity(capacity), id(id), num_bytes(0) {

    num_rows = record_batch->num_rows();
    records = std::move(record_batch);
    compute_num_bytes();
}

std::shared_ptr<arrow::RecordBatch> Block::get_records() { return records; }

int Block::get_num_rows() const { return num_rows; }

int Block::get_id() const { return id; }

std::shared_ptr<arrow::Array> Block::get_column(int column_index) const { return records->column(column_index); }

std::shared_ptr<arrow::Array>
Block::get_column_by_name(const std::string &name) const { return records->GetColumnByName(name); }

int Block::get_free_row_index() const {
    auto valid = std::static_pointer_cast<arrow::BooleanArray>(records->column(0));
    for (int row_index = 0; row_index < num_rows + 1; ++row_index) {
        if (!valid->Value(row_index)) {
            return row_index;
        }
    }

    return -1;
}

bool Block::get_valid(unsigned int row_index) const {
    auto valid = std::static_pointer_cast<arrow::BooleanArray>(records->column(0));
    return valid->Value(row_index);
}

void Block::set_valid(unsigned int row_index, bool val) {
    auto valid = std::static_pointer_cast<arrow::BooleanArray>(records->column(0));
    auto *data = valid->values()->mutable_data();
    if (val) {
        data[row_index / 8] |= (1u << (row_index % 8u));
    } else {
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

void Block::increment_num_bytes(unsigned int n_bytes) {
    if (num_bytes + n_bytes > capacity) {
        throw std::runtime_error("Incremented number of bytes stored in block beyond capacity");
    } else {
        num_bytes += n_bytes;
    }
}

int Block::get_bytes_left() {
    return capacity - num_bytes;
}

void Block::print() {

    for (int row = 0; row < num_rows; row++) {
        for (int i = 0; i < records->schema()->num_fields(); i++) {

            int type = records->schema()->field(i)->type()->id();

            switch (type) {
                case arrow::Type::STRING: {
                    auto col = std::static_pointer_cast<arrow::StringArray>(
                            records->column(i));
                    auto test = col->value_data()->data();

                    std::cout << col->GetString(row) << "\t";
                    break;
                }
                case arrow::Type::type::FIXED_SIZE_BINARY: {
                    auto col = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(
                            records->column(i));
                    std::cout << col->GetString(row) << "\t";
                    break;
                }
                case arrow::Type::type::INT64: {
                    auto col = std::static_pointer_cast<arrow::Int64Array>(
                            records->column(i));
                    std::cout << col->Value(row) << "\t";
                    break;
                }
                case arrow::Type::BOOL: {
                    auto col = std::static_pointer_cast<arrow::BooleanArray>(
                            records->column(i));
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
bool Block::insert_record(uint8_t *record, int32_t *byte_widths) {

    arrow::Status status;

    std::vector<arrow::Array> new_columns;

    // Position in the record array
    int head = 0;

    for (int i = 0; i < records->num_columns(); i++) {

        std::shared_ptr<arrow::Field> field = records->schema()->field(i);
        switch (field->type()->id()) {
            // Although BOOL is a fixed-width type, we must handle it
            // separately, since data is stored at the bit-level rather than
            // the byte level.
            case arrow::Type::BOOL: {
                auto column = std::static_pointer_cast<arrow::BooleanArray>(
                        records->column(i));
                auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
                        records->column(i)->data()->buffers[1]);

                // Extended the underlying buffers. This may result in copying
                // the data.
                if (data_buffer->size() % 8 == 0 && num_rows > 0) {
                    status = data_buffer->Resize(data_buffer->size() + 1);
                    data_buffer->ZeroPadding(); // Ensure the additional byte is zeroed
                }

                set_valid(num_rows, true);
                column->data()->length++;
                break;
            }

            case arrow::Type::STRING: {
                auto column = std::static_pointer_cast<arrow::StringArray>(
                        records->column(i));
                auto offsets_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
                        column->data()->buffers[1]);
                auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
                        column->data()->buffers[2]);

                // Extended the underlying data and offsets buffer. This may
                // result in copying the data.
                status = offsets_buffer->Resize(
                        offsets_buffer->size() + sizeof(int32_t));
                evaluate_status(status, __FUNCTION__, __LINE__);
                // Use index i-1 because byte_widths does not include the byte
                // width of the valid column.
                status = data_buffer->Resize(
                        data_buffer->size() + byte_widths[i - 1]);
                evaluate_status(status, __FUNCTION__, __LINE__);

                // Insert new offset
                auto *offsets_data = column->data()->GetMutableValues<int32_t>(
                        1, 0);
                int32_t new_offset = offsets_data[num_rows] + byte_widths[i - 1];
                std::memcpy(&offsets_data[num_rows + 1], &new_offset,
                        sizeof(new_offset));

                // Insert new data
                int32_t x = column->value_offset(num_rows);
                auto *values_data = column->data()->GetMutableValues<uint8_t>(
                        2, column->value_offset(num_rows));
                std::memcpy(values_data, &record[head], byte_widths[i - 1]);

                column->data()->length++;
                head += byte_widths[i - 1];
                break;
            }
                // This works with any fixed-width type, but for now, I specify INT64
            case arrow::Type::INT64: {

                std::shared_ptr<arrow::Array> column = records->column(i);

                auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
                        column->data()->buffers[1]);
                status = data_buffer->Resize(
                        data_buffer->size() + byte_widths[i - 1]);
                evaluate_status(status, __FUNCTION__, __LINE__);

                column = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(
                        records->column(i));

                auto *dest = column->data()->GetMutableValues<uint8_t>(
                        1, num_rows * byte_widths[i - 1]);
                std::memcpy(dest, &record[head], byte_widths[i - 1]);

                head += byte_widths[i - 1];
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

    // Create a new RecordBatch object to assure that records->num_rows_ is
    // consistent with the length of the underlying ArrayData.
    records = arrow::RecordBatch::Make(records->schema(),
            num_rows,
            get_columns_from_record_batch(records));
}



