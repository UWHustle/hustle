#ifndef HUSTLE_OFFLINE_BLOCK_H
#define HUSTLE_OFFLINE_BLOCK_H

#include <arrow/api.h>

#define BLOCK_SIZE 1024

class Block {
public:
    Block(int id, const std::shared_ptr<arrow::Schema> &schema, int capacity);
    Block(int id, std::shared_ptr<arrow::RecordBatch>, int capacity);
    int get_id() const;
    std::shared_ptr<arrow::Array> get_column(int column_index) const;
    std::shared_ptr<arrow::Array> get_column_by_name(const std::string &name) const;
    int get_free_row_index() const;
    bool get_valid(unsigned int row_index) const;
    void set_valid(unsigned int row_index, bool val);
    void increment_num_rows();
    void decrement_num_rows();
    void increment_num_bytes(unsigned int);
    std::shared_ptr<arrow::RecordBatch> get_records();
    int get_bytes_left();

    void print();
    int get_num_rows() const;
    bool insert_record(uint8_t* record, int32_t* byte_widths);



private:
    int id;
    std::shared_ptr<arrow::RecordBatch> records;
    std::shared_ptr<arrow::Schema> schema;
    int compute_num_bytes();

    int num_bytes;
    int capacity;
    int num_rows;



};

#endif //HUSTLE_OFFLINE_BLOCK_H
