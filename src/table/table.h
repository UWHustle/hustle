#ifndef HUSTLE_OFFLINE_TABLE_H
#define HUSTLE_OFFLINE_TABLE_H

#include "block.h"
#include <mutex>
#include <string>
#include <unordered_map>

// A Table is collection of Blocks. The Table schema does not include the valid
// column, i.e. the valid column of each Block is hidden from Table.
class Table {
public:
    // Construct an empty table with no blocks.
    Table(std::string name, std::shared_ptr<arrow::Schema> schema,
          int block_capacity);

    // Construct a table from a vector of RecordBatches read from a file.
    Table(std::string name, std::vector<std::shared_ptr<arrow::RecordBatch>>,
          int block_capacity);

    // Create an empty block.
    std::shared_ptr<Block> create_block();

    // Return the block with the specified ID.
    std::shared_ptr<Block> get_block(int block_id) const;

    // Return a block from the insert_pool
    std::shared_ptr<Block> get_block_for_insert();

    // Add a block to the insert_pool
    void mark_block_for_insert(const std::shared_ptr<Block> &block);

    int get_num_blocks() const;

    // Insert a record into a block in the insert pool. Returns true if
    // insertion was successful, false otherwise.
    //
    // record: data to be inserted
    // byte_widths: width of each value to be inserted
    //
    // With the current implementation, if the first block we fetch from the
    // insert pool does not have enough space to hold the record, we simply
    // create a new block. In other words, there is no reasonable mechanism
    // in place to
    void insert_record(uint8_t *record, int32_t *byte_lengths);

    // Return the table's schema, excluding the valid column of the underlying
    // blocks.
    std::shared_ptr<arrow::Schema> get_schema() const;

    std::unordered_map<int, std::shared_ptr<Block>> get_blocks();

private:
    std::string table_name;

    // Excludes the valid column, which is only visible to the Block class.
    std::shared_ptr<arrow::Schema> schema;

    // Initialized to BLOCK_SIZE
    int block_capacity;

    std::unordered_map<int, std::shared_ptr<Block>> blocks;

    std::mutex blocks_mutex;

    // A map containing blocks that can hold at least one additional record.
    std::unordered_map<int, std::shared_ptr<Block>> insert_pool;

    std::mutex insert_pool_mutex;

    // Block ID of the next block to be created. Equal to the number of blocks
    // in the table.
    int block_counter;

    // The byte width of all fields with fixed length. If there are no
    // variable-length fields, then this is equal to the byte width of a full
    // record.
    //
    // Used to determine if we can fit another record into the block.
    int fixed_record_width;

    // Total number of rows in all blocks of the table.
    int num_rows;

    // Print the contents of all blocks in the table, including the valid
    // column.
    void print();

    // Compute the minimum number of bytes contained in each record.
    int compute_fixed_record_width();



};


#endif //HUSTLE_OFFLINE_TABLE_H
