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
          num_rows(0), block_capacity(block_capacity) {

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
    // The first column of the record batch is the valid column, which should
    // not be visible to Table. So we remove it.
    auto fields = record_batches[0]->schema()->fields();
    fields.erase(fields.begin());
    schema = arrow::schema(fields);
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



void Table::add_blocks(std::vector<std::shared_ptr<Block>> input_blocks) {

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

    std::vector<std::shared_ptr<arrow::ArrayData>> sliced_column_data;

    for (int row=0; row<l; row++) {

        int record_size = 0;

        for (int i = 0; i < schema->num_fields(); i++) {

            std::shared_ptr<arrow::Field> field = schema->field(i);

            switch (field->type()->id()) {

                case arrow::Type::STRING: {
                    auto *offsets = column_data[i]->GetValues<int32_t>(1, 0);
                    record_size += offsets[row+1] - offsets[row];
                    break;
                }
                case arrow::Type::BOOL:
                case arrow::Type::INT64: {
                    // buffer at index 1 is the data buffer.
                    int byte_width = field->type()->layout().bit_widths[1] / 8;
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

        if (data_size + record_size > block->get_bytes_left()) {

            for (int i=0; i<schema->num_fields(); i++) {
                // Note that row is equal to the index of the first record we
                // cannot fit in the block.
                auto sliced_data = std::make_shared<arrow::ArrayData>
                        (column_data[i]->Slice(offset,row-offset));
                sliced_column_data.push_back(sliced_data);
            }

            block->insert_records(sliced_column_data);
            sliced_column_data.clear();

            num_rows += row - offset;
            offset = row;
            data_size = 0;

            block = create_block();
        }

        data_size += record_size;
    }

    // Insert the last of the records
    for (int i=0; i<schema->num_fields(); i++) {
        // Note that row is equal to the index of the first record we
        // cannot fit in the block.
        auto sliced_data = std::make_shared<arrow::ArrayData>
                (column_data[i]->Slice(offset,l-offset));
        sliced_column_data.push_back(sliced_data);
    }
    block->insert_records(sliced_column_data);
    sliced_column_data.clear();

    if (block->get_bytes_left() > fixed_record_width) {
        insert_pool[block->get_id()] = block;
    }
}

// Tuple is passed in as an array of bytes which must be parsed.
void Table::insert_record(uint8_t *record, int32_t *byte_widths) {

    std::shared_ptr<Block> block = get_block_for_insert();

    int32_t record_size = 0;
    for (int i = 0; i < schema->num_fields(); i++) {
        record_size += byte_widths[i];
    }

    if (block->get_bytes_left() < record_size) {
        block = create_block();
    }

    block->insert_record(record, byte_widths);
    num_rows++;

    if (block->get_bytes_left() > fixed_record_width) {
        insert_pool[block->get_id()] = block;
    }
}

int Table::get_block_row_offset(int i) const{
    return block_row_offsets[i];
}

// TODO(nicholas): note that this function can "see" the valid columns
std::shared_ptr<arrow::ChunkedArray> Table::get_column(int col_index)  {
    arrow::ArrayVector array_vector;
    for (int i = 0; i < blocks.size(); i++) {
        array_vector.push_back(blocks[i]->get_column(col_index));
    }
    return std::make_shared<arrow::ChunkedArray>(array_vector);
}
