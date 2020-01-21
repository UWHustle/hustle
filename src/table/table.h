

#ifndef HUSTLE_OFFLINE_TABLE_H
#define HUSTLE_OFFLINE_TABLE_H

#include "block.h"
#include <mutex>
#include <string>
#include <unordered_map>

class Table {
public:
    Table(std::string name, std::shared_ptr<arrow::Schema> schema,
          int block_capacity);
    Table(std::string name, std::vector<std::shared_ptr<arrow::RecordBatch>>,
          int block_capacity);
    std::shared_ptr<Block> create_block();
    std::shared_ptr<Block> get_block(int block_id) const;
    std::shared_ptr<Block> get_block_for_insert();
    void mark_block_for_insert(const std::shared_ptr<Block> &block);

    int get_num_blocks() const;
    // TODO: Current cannot be made const because num_rows and insert_pool are updated
    void insert_record(uint8_t* record, int32_t* byte_lengths);
    std::shared_ptr<arrow::Schema> get_schema() const;
    std::unordered_map<int, std::shared_ptr<Block>> get_blocks();

private:
    std::string name;
    std::shared_ptr<arrow::Schema> schema;
    int block_capacity;
    std::unordered_map<int, std::shared_ptr<Block>> blocks;
    std::mutex blocks_mutex;
    std::unordered_map<int, std::shared_ptr<Block>> insert_pool;
    std::mutex insert_pool_mutex;

    void print();
    int compute_fixed_record_width();

    int block_counter;
    int fixed_record_width;
    int num_rows;

};


#endif //HUSTLE_OFFLINE_TABLE_H
