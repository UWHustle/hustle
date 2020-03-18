#include "table.h"
#include "block.h"
#include <arrow/api.h>
#include <iostream>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include "util.h"


Table::Table(std::string name, std::shared_ptr<arrow::Schema> schema,
             int block_capacity)
        : table_name(std::move(name)), schema(schema), block_counter(0),
          num_rows(0), block_capacity(block_capacity), block_row_offsets({}) {

    fixed_record_width = compute_fixed_record_width(schema);
}


Table::Table(
        std::string name,
        std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches,
        int block_capacity)
        : table_name(std::move(name)), block_counter(0), num_rows(0),
          block_capacity(block_capacity), block_row_offsets({0}) {
    // TODO(nicholas): Be consistent with how/when block_row_offsets is
    //  initialized.
    schema = std::move(record_batches[0]->schema());
    // Must be called only after schema is set
    fixed_record_width = compute_fixed_record_width(schema);

    for (auto batch : record_batches) {
        auto block = std::make_shared<Block>(block_counter, batch, BLOCK_SIZE);
        blocks.emplace(block_counter, block);
        block_counter++;
        num_rows += batch->num_rows();
        block_row_offsets.push_back(num_rows);
    }

    if (blocks[blocks.size() - 1]->get_bytes_left() > fixed_record_width) {
        mark_block_for_insert(blocks[blocks.size() - 1]);
        // The final block is not full, so do not include an offset for the
        // next block.
        block_row_offsets[block_row_offsets.size()-1] = -1;
    }
}

std::shared_ptr<Block> Table::create_block() {
    std::scoped_lock blocks_lock(blocks_mutex);
    int block_id = block_counter++;
    auto block = std::make_shared<Block>(block_id, schema, block_capacity);
    blocks.emplace(block_id, block);

    block_row_offsets.push_back(num_rows);

    return block;
}



void Table::insert_blocks(std::vector<std::shared_ptr<Block>> input_blocks) {

    if (input_blocks.empty()) {
        return;
    }

    for (auto &block : input_blocks) {
        blocks.emplace(block_counter, block);
        block_counter++;
        num_rows += block->get_num_rows();
        block_row_offsets.push_back(num_rows);
    }

    if (blocks[blocks.size() - 1]->get_bytes_left() > fixed_record_width) {
        mark_block_for_insert(blocks[blocks.size() - 1]);
        // The final block is not full, so do not include an offset for the
        // next new block.
        block_row_offsets[block_row_offsets.size()] = -1;
    }

}

int Table::get_num_rows() const{
    return num_rows;
}

std::shared_ptr<Block> Table::get_block(int block_id) const {
//  std::scoped_lock blocks_lock(blocks_mutex);
    return blocks.at(block_id);
}

std::shared_ptr<Block> Table::get_block_for_insert() {
    std::scoped_lock insert_pool_lock(insert_pool_mutex);
    if (insert_pool.empty()) {
        return create_block();
    }

    auto iter = insert_pool.begin();
    std::shared_ptr<Block> block = iter->second;
    insert_pool.erase(iter->first);
    return block;
}

void Table::mark_block_for_insert(const std::shared_ptr<Block> &block) {
    std::scoped_lock insert_pool_lock(insert_pool_mutex);
    assert(block->get_bytes_left() > fixed_record_width);

    insert_pool[block->get_id()] = block;
}

std::shared_ptr<arrow::Schema> Table::get_schema() const { return schema; }

int Table::get_num_blocks() const {
    return blocks.size();
}

void Table::print() {

    if (blocks.size() == 0) {
        std::cout << "Table is empty." << std::endl;
    } else {
        for (int i = 0; i < blocks.size(); i++) {
            blocks[i]->print();
        }
    }
}

std::unordered_map<int, std::shared_ptr<Block>>
Table::get_blocks() { return blocks; }


void Table::insert_records(std::vector<std::shared_ptr<arrow::ArrayData>>
                           column_data) {

    int l = column_data[0]->length;
    int data_size = 0;

    auto block = get_block_for_insert();

    int offset = 0;
    int length = l;

    for (int row=0; row<l; row++) {

        int record_size = 0;

        for (int i = 0; i < schema->num_fields(); i++) {

            std::shared_ptr<arrow::Field> field = schema->field(i);

            switch (field->type()->id()) {

                case arrow::Type::STRING: {
                    // TODO(nicholas) schema offsets!!!!
                    auto *offsets = column_data[i]->GetValues<int32_t>(1, 0);
                    record_size += offsets[row+1] - offsets[row];
                    break;
                }
                case arrow::Type::DOUBLE:
                case arrow::Type::INT64: {
                    // buffer at index 1 is the data buffer.
                    int byte_width = sizeof(int64_t);
                    record_size += byte_width;
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

        std::vector<std::shared_ptr<arrow::ArrayData>> sliced_column_data;

        if (data_size + record_size > block->get_bytes_left()) {

            for (int i=0; i<column_data.size(); i++) {
                // Note that row is equal to the index of the first record we
                // cannot fit in the block.
                auto sliced_data = std::make_shared<arrow::ArrayData>
                        (column_data[i]->Slice(offset,row-offset));
                sliced_column_data.push_back(sliced_data);
            }

            block->insert_records(sliced_column_data);
//            sliced_column_data.clear(); // no need to clear; a new vector
//            is declared in each loop.

            offset = row;
            data_size = 0;

            block = create_block();
        }

        data_size += record_size;
    }

    std::vector<std::shared_ptr<arrow::ArrayData>> sliced_column_data;
    // Insert the last of the records
    for (int i=0; i<column_data.size(); i++) {
        // Note that row is equal to the index of the first record we
        // cannot fit in the block.
        auto sliced_data = std::make_shared<arrow::ArrayData>
                (column_data[i]->Slice(offset,l-offset));
        sliced_column_data.push_back(sliced_data);
    }
    block->insert_records(sliced_column_data);
//    sliced_column_data.clear();
// no need to clear. We only use this vector once.

    if (block->get_bytes_left() > fixed_record_width) {
        insert_pool[block->get_id()] = block;
    }

    num_rows += l;
}
// Tuple is passed in as an array of bytes which must be parsed.
void Table::insert_record(uint8_t *record, int32_t *byte_widths) {

    std::shared_ptr<Block> block = get_block_for_insert();

    int32_t record_size = 0;
    for (int i = 0; i < schema->num_fields(); i++) {
        record_size += byte_widths[i];
    }

    auto test = block->get_bytes_left();
    if (block->get_bytes_left() < record_size) {
        block = create_block();
    }

    block->insert_record(record, byte_widths);
    num_rows++;

    if (block->get_bytes_left() > fixed_record_width) {
        insert_pool[block->get_id()] = block;
    }
}

void Table::insert_record(std::vector<std::string_view> values, int32_t
*byte_widths, int
delimiter_size) {

    std::shared_ptr<Block> block = get_block_for_insert();

    int32_t record_size = 0;
    // record size is incorrectly computed!
    for (int i = 0; i < schema->num_fields(); i++) {
        record_size += byte_widths[i];
    }

    auto test = block->get_bytes_left();
    if (block->get_bytes_left() < record_size) {
        block = create_block();
    }

    block->insert_record(values, byte_widths, delimiter_size);
    num_rows++;

    if (block->get_bytes_left() > fixed_record_width) {
        insert_pool[block->get_id()] = block;
    }
}

int Table::get_block_row_offset(int i) const{
    return block_row_offsets[i];
}

std::shared_ptr<arrow::ChunkedArray> Table::get_column(int col_index)  {

    if (blocks.empty()) {
        return nullptr;
    }

    arrow::ArrayVector array_vector;
    for (int i = 0; i < blocks.size(); i++) {
        array_vector.push_back(blocks[i]->get_column(col_index));
    }
    return std::make_shared<arrow::ChunkedArray>(array_vector);
}


std::shared_ptr<arrow::ChunkedArray> Table::get_column_by_name(std::string
name) {
    return get_column(schema->GetFieldIndex(name));
}

std::shared_ptr<arrow::ChunkedArray> Table::get_valid_column(){

    arrow::ArrayVector array_vector;
    for (int i = 0; i < blocks.size(); i++) {
        array_vector.push_back(blocks[i]->get_valid_column());
    }
    return std::make_shared<arrow::ChunkedArray>(array_vector);
}

