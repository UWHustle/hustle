#ifndef HUSTLE_OFFLINE_BLOCK_H
#define HUSTLE_OFFLINE_BLOCK_H

#include <arrow/api.h>

#define BLOCK_SIZE 1024

// A Hustle Block is a wrapper for an Arrow RecordBatch. An Arrow RecordBatch
// is a table-like structure containing a vector of equal-length Arrow Arrays.
// An Arrow Array is a contiguous sequence of values of the same type. Both
// RecordBatch and Array are immutable. Data within an Array can be mutated by
// accessing the Array's underlying ArrayData, the mutable container holding
// the data buffer. ArrayData buffers are always padded to multiples of 64
// bytes.
//
// Because RecordBatch and Array are immutable, modifying the underlying
// ArrayData will not update internal variables the RecordBatch nor the Array,
// e.g. inserting an element into ArrayData and incrementing its length will
// not update the length of the Array.
//
// Currently, blocks are initialized with 0 rows, and memory is allocated upon
// insertion as needed.
//
// Currently, only BOOL, INT64, STRING, and FIXEDSIZEBINARY types are supported.
// However, support for any fixed-width type can be added by introducing
// additional case statements in each switch block.
//
// TODO(nicholas): In the future, blocks will "guess" how many records they cab
// hold and allocate enough memory for that many records. If this guess is too
// large/small, then we can allocate/deallocate memory accordingly.
//
// TODO(nicholas): It may be better to store a vector of mutable ArrayDatas
// instead of a RecordBatch. We could mutate the ArrayData without having to
// worry about internal inconsistencies. If we need to run a query on a block,
// we could construct a RecordBatch from the vector of ArrayData and return
// that instead. If we need to fetch just one column, we could construct an
// Array from the corresponding ArrayData. With this approach, we avoid
// internal consistencies and without having to reconstruct RecordBatches
// and Arrays.
class Block {
public:
    // Initialize an empty block.
    Block(int id, const std::shared_ptr<arrow::Schema> &schema, int capacity);

    // Initialize a Block from a RecordBatch read in from a file.
    Block(int id, std::shared_ptr<arrow::RecordBatch>, int capacity);

    int get_id() const;

    std::shared_ptr<arrow::Array> get_column(int column_index) const;

    std::shared_ptr<arrow::Array> get_column_by_name(const std::string &name) const;

    // Determine the first available row that can be used to store the data. If
    // no such index exists, return -1.
    //
    // This function is not used right now, since memory is allocated upon
    // insertion as necessary.
    int get_free_row_index() const;

    // Return true is a the row at row_index contains valid data, false
    // otherwise.
    bool get_valid(unsigned int row_index) const;

    // Set the valid bit to val at row_index
    void set_valid(unsigned int row_index, bool val);

    void increment_num_rows();

    void decrement_num_rows();


    void increment_num_bytes(unsigned int);

    std::shared_ptr<arrow::RecordBatch> get_records();

    int get_bytes_left();

    // Print the contents of the block delimited by tabs, including the valid
    // column.
    void print();

    int get_num_rows() const;

    // Insert a record into the Block
    //
    // record: data to be inserted
    // byte_widths: width of each value to be inserted
    //
    // If there is enough space to insert the record,
    bool insert_record(uint8_t *record, int32_t *byte_widths);


private:

    // Block ID
    int id;

    std::shared_ptr<arrow::RecordBatch> records;
    std::shared_ptr<arrow::Schema> schema;

    // Compute the number of bytes in the block. This function is only called
    // when a block is initialized from a RecordBatch, i.e. when we read in a
    // block from a file.
    // TODO(nicholas): Instead of computing num_bytes, we can write num_bytes
    // at the beginning of each Block file and read it in before hand.
    int compute_num_bytes();

    // Total number of data bytes stored in the block, excluding the valid column data.
    int num_bytes;

    // Initialized to BLOCK_SIZE
    int capacity;

    // Number of rows in the Block, including valid and invalid rows.
    int num_rows;


};

#endif //HUSTLE_OFFLINE_BLOCK_H
