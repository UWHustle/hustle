#include "block.h"
#include <arrow/scalar.h>
#include <vector>
#include <iostream>
#include "util.h"
#include "absl/strings/numbers.h"

#define ESTIMATED_STR_LEN 30

Block::Block(int id, const std::shared_ptr<arrow::Schema> &in_schema,
             int capacity)
        : num_rows(0), num_bytes(0), capacity(capacity), id(id), schema
        (in_schema) {

    arrow::Status status;


    int fixed_record_width = compute_fixed_record_width(schema);
    int num_string_cols = 0;
    for (const auto &field : schema->fields()) {
        if (field->type()->id() == arrow::Type::STRING) {
            num_string_cols++;
        }
    }

    // Estimated number of tuples that will fit in the block, assuming that
    // strings are on average ESTIMATED_STR_LEN characters long.
    int init_rows = capacity /
                    (fixed_record_width + ESTIMATED_STR_LEN * num_string_cols);

    // Initialize valid column separately
    std::shared_ptr<arrow::ResizableBuffer> valid_buffer;
    status = arrow::AllocateResizableBuffer(0, &valid_buffer);
    evaluate_status(status, __FUNCTION__, __LINE__);
    valid = arrow::ArrayData::Make(arrow::boolean(),0,{nullptr,valid_buffer});

    for (const auto &field : schema->fields()) {

        column_sizes.push_back(0);
        // Empty ArrayData should be constructed using the constructor that
        // accepts buffers as parameters. Other constructors will initialize
        // empty ArrayData (and Array) with nullptrs, making it impossible to
        // insert data.
        std::shared_ptr<arrow::ArrayData> array_data;
        std::shared_ptr<arrow::ResizableBuffer> data;

        switch (field->type()->id()) {

            case arrow::Type::STRING: {

                std::shared_ptr<arrow::ResizableBuffer> offsets;
                // Although the data buffer is empty, the offsets buffer should
                // still contain the offset of the first element.
                status = arrow::AllocateResizableBuffer(
                        sizeof(int32_t) * init_rows, &offsets);
                evaluate_status(status, __FUNCTION__, __LINE__);

                // Make sure the first offset value is set to 0
                int32_t initial_offset = 0;
                uint8_t *offsets_data = offsets->mutable_data();
                std::memcpy(&offsets_data[0], &initial_offset,
                            sizeof(initial_offset));

                status = arrow::AllocateResizableBuffer(
                        ESTIMATED_STR_LEN * init_rows, &data);
                evaluate_status(status, __FUNCTION__, __LINE__);
                data->ZeroPadding();

                // Initialize null bitmap buffer to nullptr, since we currently don't use it.
                columns.push_back(arrow::ArrayData::Make(field->type(), 0,
                                                         {nullptr, offsets,
                                                          data}));
                break;
            }

            case arrow::Type::DOUBLE:
            case arrow::Type::INT64: {

                status = arrow::AllocateResizableBuffer
                        (sizeof(int64_t) * init_rows,
                         &data);
                evaluate_status(status, __FUNCTION__, __LINE__);
                data->ZeroPadding();

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
    for (int i = 0; i < schema->num_fields(); i++) {

        std::shared_ptr<arrow::Field> field = schema->field(i);
        switch (field->type()->id()) {

            case arrow::Type::STRING: {
                auto *offsets = columns[i]->GetValues<int32_t>(1, 0);
                num_bytes += offsets[num_rows];
                break;
            }
            case arrow::Type::DOUBLE:
            case arrow::Type::INT64: {
                // buffer at index 1 is the data buffer.
                int byte_width = sizeof(int64_t);
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

Block::Block(int id, std::shared_ptr<arrow::RecordBatch> record_batch, int
capacity) : capacity(capacity), id(id), num_bytes(0) {

    arrow::Status status;

    num_rows = record_batch->num_rows();
    schema = std::move(record_batch->schema());
    for (int i = 0; i < record_batch->num_columns(); i++) {
        columns.push_back(record_batch->column_data(i));
        column_sizes.push_back(0);
    }
    compute_num_bytes();

    // Initialize valid column separately
    std::shared_ptr<arrow::ResizableBuffer> valid_buffer;
    status = arrow::AllocateResizableBuffer(num_rows, &valid_buffer);
    evaluate_status(status, __FUNCTION__, __LINE__);
    valid = arrow::ArrayData::Make(arrow::boolean(),num_rows,{nullptr,
                                                          valid_buffer});

    for (int i=0; i<num_rows; i++) {
        set_valid(i,true);
    }
}

std::shared_ptr<arrow::RecordBatch> Block::get_records() {
    return
            arrow::RecordBatch::Make(schema, num_rows, columns);
}

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
    auto *data = valid->GetMutableValues<uint8_t>(1, 0);
    for (int i = 0; i < num_rows; i++) {
        if (data[i / 8] >> (i % 8u)) {
            return i;
        }
    }
    return -1;
}

bool Block::get_valid(unsigned int row_index) const {
    auto *data = valid->GetMutableValues<uint8_t>(1, 0);
    uint8_t byte = data[row_index / 8];
    return (byte >> (row_index % 8u)) == 1u;

}

void Block::set_valid(unsigned int row_index, bool val) {
    auto *data = valid->GetMutableValues<uint8_t>(1, 0);
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
                "Incremented number of bytes stored in block beyond "
                "capacity:");
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

        auto valid_col = std::static_pointer_cast<arrow::BooleanArray>
                (arrow::MakeArray(valid));
        std::cout << valid_col->Value(row) << "\t";

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
                case arrow::Type::type::DOUBLE: {
                    auto col = std::static_pointer_cast<arrow::DoubleArray>(
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

// TODO(nicholas) cuyrrently there is no attempt to allocate new memory if
//  the buffers are too small.
// Note that this funciton assumes that the valid column is in column_data
bool Block::insert_records(std::vector<std::shared_ptr<arrow::ArrayData>>
                           column_data) {

    if (column_data[0]->length == 0) {
        return true;
    }

    arrow::Status status;
    int n = column_data[0]->length; // number of elements to be inserted

    auto valid_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
            valid->buffers[1]);

    status = valid_buffer->Resize(valid_buffer->size() + n/8 + 1);
    valid_buffer->ZeroPadding(); // Ensure the additional byte is zeroed

    // TODO(nicholas)
    for (int k = 0; k < n; k++) {
        set_valid(num_rows+k, true);
    }

    valid->length += n;

    // NOTE: buffers do NOT account for Slice offsets!!!
    int offset = column_data[0]->offset;

    for (int i = 0; i < schema->num_fields(); i++) {

        std::shared_ptr<arrow::Field> field = schema->field(i);
        switch (field->type()->id()) {
            case arrow::Type::STRING: {
                auto offsets_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
                        columns[i]->buffers[1]);
                auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
                        columns[i]->buffers[2]);

                // Extended the underlying data and offsets buffer. This may
                // result in copying the data.
                // n+1 because we also need to specify the endpoint of the
                // last string.
                    status = offsets_buffer->Resize(
                            offsets_buffer->size() + sizeof(int32_t) * (n + 1));
                    evaluate_status(status, __FUNCTION__, __LINE__);


                status = data_buffer->Resize(
                        data_buffer->size() +
                        column_data[i]->buffers[2]->size());
                evaluate_status(status, __FUNCTION__, __LINE__);

                auto in_offsets_data =
                        column_data[i]->GetMutableValues<int32_t>(
                                1, offset);
                auto in_values_data =
                        column_data[i]->GetMutableValues<uint8_t>(
                                2, in_offsets_data[0]);

                // Insert new offset

                auto *offsets_data = columns[i]->GetMutableValues<int32_t>(
                        1, 0);

                int32_t current_offset = offsets_data[num_rows];

                std::memcpy(&offsets_data[num_rows], in_offsets_data,
                            sizeof(int32_t) * (n + 1));

                // Correct new offsets
                for (int k = 0; k <= n; k++) {
                    offsets_data[num_rows + k] += current_offset;
                }

                auto *values_data = columns[i]->GetMutableValues<uint8_t>(
                        2, offsets_data[num_rows]);
                int string_size = in_offsets_data[n] - in_offsets_data[0];
                std::memcpy(values_data, in_values_data, string_size);


                columns[i]->length += n;
                column_sizes[i] += string_size;
                increment_num_bytes(string_size);
                break;
            }
            case arrow::Type::INT64: {

                auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
                        columns[i]->buffers[1]);
                status = data_buffer->Resize(
                        data_buffer->size() + sizeof(int64_t) * n);
                evaluate_status(status, __FUNCTION__, __LINE__);

                auto *dest = columns[i]->GetMutableValues<int64_t>(
                        1, num_rows);
                std::memcpy(dest, column_data[i]->GetMutableValues<int64_t>
                        (1, offset), sizeof(int64_t) * n);

                columns[i]->length += n;
                column_sizes[i] += sizeof(int64_t) * n;
                increment_num_bytes(n * sizeof(int64_t));
                break;
            }
            case arrow::Type::DOUBLE: {
                auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
                        columns[i]->buffers[1]);
                status = data_buffer->Resize(
                        data_buffer->size() + sizeof(double_t) * n);
                evaluate_status(status, __FUNCTION__, __LINE__);

                auto *dest = columns[i]->GetMutableValues<double_t>(
                        1, num_rows);
                auto asdf = dest[0];
                std::memcpy(dest, column_data[i]->GetMutableValues<double_t>
                        (1, offset), sizeof(double_t) * n);

                columns[i]->length += n;
                column_sizes[i] += sizeof(double_t) * n;
                increment_num_bytes(n * sizeof(double_t));
                break;
            }
            default:
                throw std::logic_error(
                        std::string(
                                "Cannot insert tuple with unsupported type: ") +
                        field->type()->ToString());
        }
    }
    num_rows += n;
    return true;
}

// Return true is insertion was successful, false otherwise
bool Block::insert_record(uint8_t *record, int32_t *byte_widths) {

    int record_size = 0;
    for (int i = 0; i < schema->num_fields(); i++) {
        record_size += byte_widths[i];
    }

    // record does not fit in the block.
    if (record_size > get_bytes_left()) {
        return false;
    }

    arrow::Status status;
    std::vector<arrow::Array> new_columns;

    // Set valid bit
    auto valid_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
            valid->buffers[1]);
    status = valid_buffer->Resize(valid_buffer->size() + 1);
    evaluate_status(status, __FUNCTION__, __LINE__);
    set_valid(num_rows, true);
    valid->length++;

    // Position in the record array
    int head = 0;

    for (int i = 0; i < schema->num_fields(); i++) {

        std::shared_ptr<arrow::Field> field = schema->field(i);
        switch (field->type()->id()) {
            case arrow::Type::STRING: {
                auto offsets_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
                        columns[i]->buffers[1]);
                auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
                        columns[i]->buffers[2]);

                // Extended the underlying data and offsets buffer. This may
                // result in copying the data.
                // Use index i-1 because byte_widths does not include the byte
                // width of the valid column.
                    status = data_buffer->Resize(
                            data_buffer->size() + byte_widths[i]);
                    evaluate_status(status, __FUNCTION__, __LINE__);


                    status = offsets_buffer->Resize(
                            offsets_buffer->size() + sizeof(int32_t));
                    evaluate_status(status, __FUNCTION__, __LINE__);


                // Insert new offset
                auto *offsets_data = columns[i]->GetMutableValues<int32_t>(
                        1, 0);
                int32_t new_offset =
                        offsets_data[num_rows] + byte_widths[i];
                std::memcpy(&offsets_data[num_rows + 1], &new_offset,
                            sizeof(new_offset));

                auto *values_data = columns[i]->GetMutableValues<uint8_t>(
                        2, offsets_data[num_rows]);
                std::memcpy(values_data, &record[head], byte_widths[i]);

                columns[i]->length++;
                column_sizes[i] += byte_widths[i];
                head += byte_widths[i];
                break;
            }
                // This works with any fixed-width type, but for now, I specify INT64
            case arrow::Type::DOUBLE:
            case arrow::Type::INT64: {

                auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
                        columns[i]->buffers[1]);

                status = data_buffer->Resize(
                        data_buffer->size() + byte_widths[i]);
                evaluate_status(status, __FUNCTION__, __LINE__);


                auto *dest = columns[i]->GetMutableValues<int64_t>(
                        1, num_rows);
                std::memcpy(dest, &record[head], byte_widths[i]);

                head += byte_widths[i];
                column_sizes[i] += byte_widths[i];
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

std::shared_ptr<arrow::Schema> Block::get_schema() {
    return schema;
}

std::shared_ptr<arrow::Array> Block::get_valid_column() const{
    return arrow::MakeArray(valid);
}








// Return true is insertion was successful, false otherwise
bool Block::insert_record(std::vector<std::string_view> record, int32_t
*byte_widths, int delimiter_size) {

    int record_size = 0;
    for (int i = 0; i < schema->num_fields(); i++) {
        record_size += byte_widths[i];
    }

    // record does not fit in the block.
    if (record_size > get_bytes_left()) {
        return false;
    }

    arrow::Status status;
    std::vector<arrow::Array> new_columns;

    // Set valid bit
    auto valid_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
            valid->buffers[1]);
    status = valid_buffer->Resize(valid_buffer->size() + 1);
    evaluate_status(status, __FUNCTION__, __LINE__);
    set_valid(num_rows, true);
    valid->length++;

    // Position in the record array
    int head = 0;

    for (int i = 0; i < schema->num_fields(); i++) {

        std::shared_ptr<arrow::Field> field = schema->field(i);
        switch (field->type()->id()) {
            case arrow::Type::STRING: {
                auto offsets_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
                        columns[i]->buffers[1]);
                auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
                        columns[i]->buffers[2]);

                // Extended the underlying data and offsets buffer. This may
                // result in copying the data.

                // IMPORTANT: DO NOT GRAB A POINTER TO THE UNDERLYING DATA
                // BEFORE YOU RESIZE IT. THE DATA WILL BE COPIED TO A NEW
                // LOCATION, AND YOUR POINTER WILL BE GARBAGE.
                auto *offsets_data = columns[i]->GetMutableValues<int32_t>(
                        1, 0);

                if (offsets_data[num_rows] + record[i].length() >
                    data_buffer->capacity()) {
                    // TODO(nicholas): do we need a +1 here too?
                    status = data_buffer->Resize(
                            data_buffer->size() + record[i].length());
                    evaluate_status(status, __FUNCTION__, __LINE__);
                }

                // There are length+1 offsets, and we are going ot add
                // another offsets, so +2
                if ((columns[i]->length + 2) * sizeof(int32_t) >
                    offsets_buffer->capacity()) {
                    // Resize will not resize the buffer if the inputted size
                    // equals the current size of the buffer. To force
                    // resizing in this case, we add +1.
                    status = offsets_buffer->Resize(
                            (num_rows + 2) * sizeof(int32_t)+1);
                    evaluate_status(status, __FUNCTION__, __LINE__);
                    // TODO(nicholas): is this necessary?
                    offsets_buffer->ZeroPadding();
                }
                // We must fetch the offsets data again, since resizing might
                // have moved its location in memory.
                offsets_data = columns[i]->GetMutableValues<int32_t>(1, 0);

                // Insert new offset
                // you must zero the padding or else you might get bogus
                // offset data!

                int32_t new_offset =
                        offsets_data[num_rows] + record[i].length();
                std::memcpy(&offsets_data[num_rows + 1], &new_offset,
                            sizeof(new_offset));

                auto *values_data = columns[i]->GetMutableValues<uint8_t>(
                        2, offsets_data[num_rows]);
                std::memcpy(values_data, &record[i].front(), record[i].length
                ());

                columns[i]->length++;
                head += record[i].length();
                column_sizes[i] += record[i].length();
                break;
            }
                // This works with any fixed-width type, but for now, I specify INT64
            case arrow::Type::INT64: {

                auto data_buffer = std::static_pointer_cast<arrow::ResizableBuffer>(
                        columns[i]->buffers[1]);

                if (column_sizes[i] + sizeof(int64_t) > data_buffer->capacity
                ()) {
                    // Resize will not resize the buffer if the inputted size
                    // equals the current size of the buffer. To force
                    // resizing in this case, we add +1.
                    status = data_buffer->Resize(
                            data_buffer->capacity() + sizeof(int64_t)+1);
                    evaluate_status(status, __FUNCTION__, __LINE__);
                }


                int64_t out;
                absl::SimpleAtoi(record[i], &out);

                auto *dest = columns[i]->GetMutableValues<int64_t>(
                        1, num_rows);
                std::memcpy(dest, &out, sizeof(int64_t));

                head += sizeof(int64_t);
                column_sizes[i] += sizeof(int64_t);
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