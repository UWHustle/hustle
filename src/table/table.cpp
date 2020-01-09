
#include "table.h"
#include "block.h"
#include <arrow/api.h>
#include <iostream>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>


Table::Table(std::string name, std::shared_ptr<arrow::Schema> schema,
             int block_capacity)
        : name(std::move(name)), schema(schema), block_counter(0), record_width(0),
          block_capacity(block_capacity) {

    for (auto field : schema->fields()) {
        record_width += field->type()->layout().bit_widths[1] / 8;
    }

}


Table::Table(std::string name, std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches,
        int block_capacity)
        : name(std::move(name)), block_counter(0), record_width(0),
          block_capacity(block_capacity) {

    for (auto field : record_batches[0]->schema()->fields()) {
        record_width += field->type()->layout().bit_widths[1] / 8;
    }

    schema = record_batches[0]->schema();

    for (auto batch : record_batches) {
        auto block = std::make_shared<Block>(block_counter,batch, BLOCK_SIZE);
        blocks.emplace(block_counter, block);
        block_counter++;
    }

    if (!blocks[blocks.size()-1]->is_full()) {
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

std::shared_ptr<Block> Table::get_block(int block_id) {
//  std::scoped_lock blocks_lock(blocks_mutex);
    return blocks[block_id];
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
    assert(!block->is_full());

    insert_pool[block->get_id()] = block;
}

const std::shared_ptr<arrow::Schema> Table::get_schema() { return schema; }

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

// Tuple is passed in as an array of bytes which must be parsed.
char Table::insert_record(uint8_t* record){

    std::shared_ptr<Block> block = get_block_for_insert();
    block->insert_record(record, record_width);

    if (!block->is_full()) {
        insert_pool[block->get_id()] = block;
    }
}
