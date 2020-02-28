#ifndef HUSTLE_OFFLINE_BLOCK_H
#define HUSTLE_OFFLINE_BLOCK_H

#include <arrow/api.h>

#define BLOCK_SIZE 1024

/**
 * A Hustle Block is a wrapper for an Arrow RecordBatch. An Arrow RecordBatch
 * is a table-like structure containing a vector of equal-length Arrow Arrays
 * An Arrow Array is a contiguous sequence of values of the same type. Both
 * RecordBatch and Array are immutable. Data within an Array can be mutated by
 * accessing the Array's underlying ArrayData, the mutable container holding
 * the data buffer. ArrayData buffers are always padded to multiples of 64
 * bytes.
 *
 * Because RecordBatch and Array are immutable, modifying the underlying
 * ArrayData will not update internal variables the RecordBatch nor the Array
 * e.g. inserting an element into ArrayData and incrementing its length will
 * not update the length of the Array.
 *
 * Currently, blocks are initialized with 0 rows, and memory is allocated upon
 * insertion as needed.
 *
 * Currently, only BOOL, INT64, STRING, and FIXEDSIZEBINARY types are support
 * However, support for any fixed-width type can be added by introducing
 * additional case statements in each switch block.
 *
 * TODO(nicholas): In the future, blocks will "guess" how many records they can
 * hold and allocate enough memory for that many records. If this guess is too
 * large/small, then we can allocate/deallocate memory accordingly.
 *
 * TODO(nicholas): It may be better to store a vector of mutable ArrayDatas
 * instead of a RecordBatch. We could mutate the ArrayData without having to
 * worry about internal inconsistencies. If we need to run a query on a block
 * we could construct a RecordBatch from the vector of ArrayData and return
 * that instead. If we need to fetch just one column, we could construct a
 * Array from the corresponding ArrayData. With this approach, we avoid
 * internal consistencies and without having to reconstruct RecordBatches
 */
class Block {
public:
    //
    /**
     * Initialize an empty block.
     *
     * @param id Block ID
     * @param schema Block schema, excluding the valid column
     * @param capacity Maximum number of date bytes to be stored in the Block
     */
    Block(int id, const std::shared_ptr<arrow::Schema> &schema, int capacity);

    /**
     * Initialize a Block from a RecordBatch read in from a file. This will
     * eventually be removed. The constructor that uses a vector of ArrayData
     * should be used instead.
     *
     * @param id Block ID
     * @param record_batch RecordBatch read from a file
     * @param capacity Maximum number of date bytes to be stored in the Block
     */
    Block(int id, std::shared_ptr<arrow::RecordBatch> record_batch, int
    capacity);

    /**
     * Initialize a Block from a vector of ArrayData
     *
     * @param id Block ID
     * @param schema Block schema, excluding the valid column
     * @param column_data ArrayData for each column
     * @param capacity Maximum number of date bytes to be stored in the Block
     */
    Block(int id, const std::shared_ptr<arrow::Schema> &schema,
          std::vector<std::shared_ptr<arrow::ArrayData>> column_data,
          int capacity);

    /**
     * Get the Block's ID
     *
     * @return The Block's ID
     */
    int get_id() const;

    /**
     * Return the block's schema, including the valid column.
     * @return the block's schema
     */
    std::shared_ptr<arrow::Schema> get_schema();

    /**
     * Get a column from the Block by index. The indexing of columns is defined
     * by the schema definition.
     *
     * @param column_index Index of the column to be returned.
     * @return A read-only pointer to the column.
     */
    std::shared_ptr<arrow::Array> get_column(int column_index) const;

    /**
     *
     *
     * @return
     */
    std::shared_ptr<arrow::Array> get_valid_column() const;

    /**
     * Get a column from the Block by name. Column names are defined by the
     * schema definition.
     *
     * @param name Name of the column
     * @return A read-only pointer to the column.
     */
    std::shared_ptr<arrow::Array>
    get_column_by_name(const std::string &name) const;

    /**
     * Determine the first available row that can be used to store the data.
     * This function is not used right now, since memory is allocated upon
     * insertion as necessary.
     *
     * @return If a free row exists, return it's index. Otherwise, return -1.
     */
    int get_free_row_index() const;

    /**
     *  Return the valid bit of a particular row.
     *
     * @param row_index Row index
     * @return True is a the row at row_index contains valid data, false
     * otherwise.
     */
    bool get_valid(unsigned int row_index) const;

    //
    /**
     * Set the valid bit to val at row_index
     *
     * @param row_index Row index
     * @param val Value to set the valid bit at row_index
     */
    void set_valid(unsigned int row_index, bool val);

    /**
     * Increment the number of rows stored in the Block. Note that this does NOT
     * increment the number of rows in the RecordBatch nor the length of its
     * Arrays.
     */
    void increment_num_rows();

    /**
     * Decrement the number of rows stored in the Block. Note that this does NOT
     * decrement the number of rows in the RecordBatch now the length of its
     * Arrays.
     */
    void decrement_num_rows();

    /**
     * Increment the number of bytes stored in the Block by bytes.
     *
     * @param bytes Number of bytes added to the Block.
     */
    void increment_num_bytes(unsigned int bytes);

    /**
     * Get the Block's RecordBatch
     *
     * @return A pointer to the Block's RecordBatch
     */
    std::shared_ptr<arrow::RecordBatch> get_records();

    /**
     * Get the number of bytes that can still be added to the Block without
     * exceeding its capacity.
     *
     * @return Number of bytes that can still be added to the Block
     */
    int get_bytes_left();

    /**
     * Print the contents of the block delimited by tabs, including the valid
     * column.
     */
    void print();

    /**
     * Get the number of rows in the Block.
     *
     * @return Number of rows in the Block
     */
    int get_num_rows() const;

    /**
     * Insert a record into the Block.
     *
     * @param record Values to be inserted into each column. Values should be
     * listed in the same order as they appear in the Block's schema. Values
     * should not be separated by e.g. null characters.
     * @param byte_widths Byte width of each value to be inserted. Byte widths
     * should be listed in the same order as they appear in the Block's schema.
     * @return True if insertion was successful, false otherwise.
     */
    bool insert_record(uint8_t *record, int32_t *byte_widths);

    /**
     * Insert one or more records into the Block as a vector of ArrayData.
     * This insertion method would be used to insert the results of a query,
     * since query results are returned as Arrays.
     *
     * @param column_data Values to be inserted into each column, including
     * the valid column. Columns should be listed in the same order as they
     * appear in the Block's schema. The length of column_data must match the
     * length of the Block's schema. All ArrayData must contain the same
     * number of elements.
     * @return True if insertion was successful, false otherwise.
     */
    bool insert_records(std::vector<std::shared_ptr<arrow::ArrayData>>
                        column_data);


private:

    // Block ID
    int id;

    std::shared_ptr<arrow::Schema> schema;
    std::shared_ptr<arrow::ArrayData> valid;
    std::vector<std::shared_ptr<arrow::ArrayData>> columns;

    /**
     * Compute the number of bytes in the block. This function is only called
     * when a block is initialized from a RecordBatch, i.e. when we read in a
     * block from a file.
     *
     * @return The number of data bytes stored in the RecordBatch, excluding
     * the valid column.
     *
     * TODO(nicholas): Instead of computing num_bytes, we can write num_bytes
     * at the beginning of each Block file and read it in before hand.
     */
    int compute_num_bytes();

    // Total number of data bytes stored in the block, excluding the valid
    // column data.
    int num_bytes;

    // Initialized to BLOCK_SIZE
    int capacity;

    // Number of rows in the Block, including valid and invalid rows.
    int num_rows;
};

#endif //HUSTLE_OFFLINE_BLOCK_H
