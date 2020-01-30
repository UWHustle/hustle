#include "block.h"
#include <arrow/scalar.h>
#include <vector>
#include <iostream>
#include "util.h"

Block::Block(int id, const std::shared_ptr<arrow::Schema> &in_schema,
             int capacity)
        : num_rows(0), num_bytes(0), capacity(capacity), id(id) {

    arrow::Status status;

    // Add the valid column to the schema
    status = in_schema->AddField(0,
                                 arrow::field("valid", arrow::boolean()),
                                 &schema);
    evaluate_status(status, __FUNCTION__, __LINE__);

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
                status = arrow::AllocateResizableBuffer(sizeof(int32_t),
                                                        &offsets);
                evaluate_status(status, __FUNCTION__, __LINE__);

                // Make sure the first offset value is set to 0
                int32_t initial_offset = 0;
                uint8_t *offsets_data = offsets->mutable_data();
                std::memcpy(&offsets_data[0], &initial_offset,
                            sizeof(initial_offset));

                // Initialize null bitmap buffer to nullptr, since we currently don't use it.
                columns.push_back(arrow::ArrayData::Make(field->type(), 0,
                                                         {nullptr, offsets,
                                                          data}));
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
}


int Block::compute_num_bytes() {

    // Start at i=1 to skip valid column
    for (int i = 1; i < schema->num_fields(); i++) {

        std::shared_ptr<arrow::Field> field = schema->field(i);
        switch (field->type()->id()) {

            case arrow::Type::STRING: {
                auto *offsets = columns[i]->GetValues<int32_t>(1, 0);
                num_bytes += offsets[num_rows];
                break;
            }
            case arrow::Type::BOOL:
            case arrow::Type::INT64: {
                // buffer at index 1 is the data buffer.
                int byte_width = field->type()->layout().bit_widths[1] / 8;
                num_bytes += byte_width * columns[i]->length;
                break;
            }
            default: {
                throw std::logic_error(
                        std::string(
                                "Cannot compute record width. Unsupported type: ") +
                        field->type()->ToString());
            }
        }
    }
}

Block::Block(int id, std::shared_ptr<arrow::RecordBatch> record_batch,
             int capacity)
        : capacity(capacity), id(id), num_bytes(0) {

    num_rows = record_batch->num_rows();
    schema = std::move(record_batch->schema());
    for (int i = 0; i< record_batch->num_columns(); i++) {
        columns.push_back(record_batch->column_data(i));
    }
    compute_num_bytes();
}

std::shared_ptr<arrow::RecordBatch> Block::get_records() { return
    arrow::RecordBatch::Make(schema, num_rows, columns); }

int Block::get_num_rows() const { return num_rows; }

int Block::get_id() const { return id; }

std::shared_ptr<arrow::Array> Block::get_column(int column_index) const {
    return arrow::MakeArray(columns[column_index]);
}

std::shared_ptr<arrow::Array>
Block::get_column_by_name(const std::string &name) const {
    return arrow::MakeArray(columns[schema->GetFieldIndex(name)]);
}

int Block::get_free_row_index() const {
    //TODO(nicholas): This may need to change when we reserve space in blocks.
    // In particular, num_rows is the number valid rows. We will need another
    // way to fetch the total number of rows in the block (valid + invalid)
    auto *data = columns[0]->GetMutableValues<uint8_t>(1, 0);
    for (int i = 0; i < num_rows; i++) {
        if (data[i/8] >> (i % 8u)) {
            return i;
        }
    }
    return -1;
}

bool Block::get_valid(unsigned int row_index) const {
    auto *data = columns[0]->GetMutableValues<uint8_t>(1, 0);
    uint8_t byte = data[row_index / 8];
    return (byte >> (row_index % 8u)) == 1u;

}

void Block::set_valid(unsigned int row_index, bool val) {
    auto *data = columns[0]->GetMutableValues<uint8_t>(1, 0);
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
        throw std::runtime_error(
                "Incremented number of bytes stored in block beyond capacity");
    } else {
        num_bytes += n_bytes;
    }
}

int Block::get_bytes_left() {
    return capacity - num_bytes;
}

void Block::print() {

    // Create Arrays from ArrayData so we can easily read column data
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (int i = 0; i < schema->num_fields(); i++) {
        arrays.push_back(arrow::MakeArray(columns[i]));
    }

    for (int row = 0; row < num_rows; row++) {
        for (int i = 0; i < schema->num_fields(); i++) {

            int type = schema->field(i)->type()->id();

            switch (type) {
                case arrow::Type::STRING: {
                    auto col = std::static_pointer_cast<arrow::StringArray>(
                            arrays[i]);

                    std::cout << col->GetString(row) << "\t";
                    break;
                }
                case arrow::Type::type::FIXED_SIZE_BINARY: {
                    auto col = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(
                            arrays[i]);
                    std::cout << col->GetString(row) << "\t";
                    break;
                }
                case arrow::Type::type::INT64: {
                    auto col = std::static_pointer_cast<arrow::Int64Array>(
                            arrays[i]);
                    std::cout << col->Value(row) << "\t";
                    break;
                }
                case arrow::Type::BOOL: {
                    auto col = std::static_pointer_cast<arrow::BooleanArray>(
                            arrays[i]);
                    std::cout << col->Value(row) << "\t";
                    break;
                }
                default: {
                    throw std::logic_error(
                            std::string(
                                    "Block created with unsupported type: ") +
                            schema->field(i)->type()->ToString());
                }
            }
        }
        std::cout << std::endl;
    }
}

// Return true is insertion was successful, false otherwise
bool Block::insert_record(uint8_t *record, int32_t *byte_widths) {

    int record_size = 0;
    // start at i=1 to skip valid column
    for (int i=1; i<schema->num_fields(); i++) {
        record_size += byte_widths[i-1];
    }

    // record does not fit in the block.
    if (record_size > get_bytes_left()) {
        return false;
    }

    arrow::Status status;
    std::vector<arrow::Array> new_columns;

    // Position in the record array
    int head = 0;

    for (int i = 0; i < schema->num_fields(); i++) {

        std::shared_ptr<arrow::Field> field = schema->field(i);
        switch (field->type()->id()) {
            // Although BOOL is a fixed-width type, we must handle it
            // separately, since data is stored at the bit-level rather than
            // the byte level.
            case arrow::Type::BOOL: {
                auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
                        columns[i]->buffers[1]);

                // Extended the underlying buffers. This may result in copying
                // the data.
                if (data_buffer->size() % 8 == 0 && num_rows > 0) {
                    status = data_buffer->Resize(data_buffer->size() + 1);
                    data_buffer->ZeroPadding(); // Ensure the additional byte is zeroed
                }

                set_valid(num_rows, true);
                columns[i]->length++;
                break;
            }

            case arrow::Type::STRING: {
                auto offsets_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
                        columns[i]->buffers[1]);
                auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
                        columns[i]->buffers[2]);

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
                auto *offsets_data = columns[i]->GetMutableValues<int32_t>(
                        1, 0);
                int32_t new_offset =
                        offsets_data[num_rows] + byte_widths[i - 1];
                std::memcpy(&offsets_data[num_rows + 1], &new_offset,
                            sizeof(new_offset));

                // Insert new data
                auto *values_data = columns[i]->GetMutableValues<uint8_t>(
                        2, offsets_data[num_rows]);
                std::memcpy(values_data, &record[head], byte_widths[i - 1]);

                columns[i]->length++;
                head += byte_widths[i - 1];
                break;
            }
                // This works with any fixed-width type, but for now, I specify INT64
            case arrow::Type::INT64: {

                auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
                        columns[i]->buffers[1]);
                status = data_buffer->Resize(
                        data_buffer->size() + byte_widths[i - 1]);
                evaluate_status(status, __FUNCTION__, __LINE__);

                auto *dest = columns[i]->GetMutableValues<uint8_t>(
                        1, num_rows * byte_widths[i - 1]);
                std::memcpy(dest, &record[head], byte_widths[i - 1]);

                head += byte_widths[i - 1];
                columns[i]->length++;
                break;
            }

            default:
                throw std::logic_error(
                        std::string(
                                "Cannot insert tuple with unsupported type: ") +
                        field->type()->ToString());
        }
    }
    increment_num_bytes(head);
    increment_num_rows();

    return true;
}



