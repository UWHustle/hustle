#include "table.h"
#include "block.h"
#include <arrow/api.h>
#include <iostream>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>


Table::Table(std::string name, std::shared_ptr<arrow::Schema> schema,
             int block_capacity)
        : table_name(std::move(name)), schema(schema), block_counter(0), num_rows(0),
          block_capacity(block_capacity) {

    fixed_record_width = compute_fixed_record_width();
}


Table::Table(std::string name, std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches,
        int block_capacity)
        : table_name(std::move(name)), block_counter(0), num_rows(0),
          block_capacity(block_capacity) {

    // The first column of the record batch is the valid column, which should not be visible to Table. So we remove it.
    auto fields = record_batches[0]->schema()->fields();
    fields.erase(fields.begin());
    schema = arrow::schema(fields);
    fixed_record_width = compute_fixed_record_width(); // must be called after schema is set

    for (auto batch : record_batches) {
        auto block = std::make_shared<Block>(block_counter, batch, BLOCK_SIZE);
        blocks.emplace(block_counter, block);
        block_counter++;
        num_rows += batch->num_rows();
    }

    if (blocks[blocks.size()-1]->get_bytes_left() > fixed_record_width) {
        mark_block_for_insert(blocks[blocks.size()-1]);
    }
}

std::shared_ptr<Block> Table::create_block() {
    std::scoped_lock blocks_lock(blocks_mutex);
    int block_id = block_counter++;
    auto block = std::make_shared<Block>(block_id, schema, block_capacity);
    blocks.emplace(block_id, block);
    return block;
}

std::shared_ptr<Block> Table::get_block(int block_id) const {
//  std::scoped_lock blocks_lock(blocks_mutex);
    return blocks.at(block_id);
}

std::shared_ptr<Block> Table::get_block_for_insert()  {
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
    }
    else {
        for (int i = 0; i < blocks.size(); i++) {
            blocks[i]->print();
        }
    }
}

std::unordered_map<int, std::shared_ptr<Block>> Table::get_blocks() { return blocks; }

int Table::compute_fixed_record_width() {

    int fixed_width = 0;

    for (auto field : schema->fields()) {

        switch (field->type()->id()) {
            case arrow::Type::STRING: {
                break;
            }
            case arrow::Type::BOOL:
            case arrow::Type::INT64: {
                fixed_width += field->type()->layout().bit_widths[1] / 8;
                break;
            }
            default: {
                throw std::logic_error(
                        std::string("Cannot compute fixed record width. Unsupported type: ") +
                        field->type()->ToString());
            }
        }
    }
    return fixed_width;

}



// Tuple is passed in as an array of bytes which must be parsed.
void Table::insert_record(uint8_t* record, int32_t* byte_widths) {

    std::shared_ptr<Block> block = get_block_for_insert();

    int32_t record_size = 0;
    for (int i=0; i<schema->num_fields(); i++) {
        record_size += byte_widths[i]; // TODO: does record size include valid column? If so, this needs to be changed.
    }

    // If the record won't fit in the current block, create a new one. Of course, this is dumb, and needs to be changed
    // later. We could have get_block_for_insert() accept the record size and then search for a block that has enough
    // room for it.
    if (block->get_bytes_left() < record_size ) {
        block = create_block();
    }

    block->insert_record(record, byte_widths);
    num_rows++;

    if (block->get_bytes_left() > fixed_record_width) {
        insert_pool[block->get_id()] = block;
    }
}
