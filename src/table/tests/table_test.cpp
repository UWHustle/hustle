//
// Created by Nicholas Corrado on 1/8/20.
//


#include <iostream>
#include "../table.h"
#include "../util.h"

void test_from_empty_table() {

    std::shared_ptr<arrow::Field> field1 = arrow::field("A", arrow::int64());
    std::shared_ptr<arrow::Field> field2 = arrow::field("B", arrow::fixed_size_binary(43));
    std::shared_ptr<arrow::Field> field3 = arrow::field("C", arrow::fixed_size_binary(60));
    std::shared_ptr<arrow::Field> field4 = arrow::field("D", arrow::int64());
    std::shared_ptr<arrow::Schema> schema = arrow::schema({field1, field2, field3, field4});

    std::cout << "Loading empty table..." << std::endl;
    Table table("table", schema, BLOCK_SIZE);
    table.print();

    std::string record_string;
    uint8_t* record_bytes;
    uint64_t f1, f4;

    record_string = "00000042Mon dessin ne representait pas un chapeau. Il representait un serpent boa qui digerait un elephant.    37373737";
    f1 = 4242;
    f4 = 37373737;

    record_bytes = (uint8_t*) record_string.data();

    std::memcpy(&record_bytes[0], &f1, sizeof(f1));
    std::memcpy(&record_bytes[8+43+60], &f4, sizeof(f4));

    std::cout << "Inserting one record..." << std::endl;
    table.insert_record(record_bytes);
    std::cout << std::endl;
    table.print();
    std::cout << std::endl;

    record_string = "00000000Twice two makes four is an excellent thing.Twice two makes five is sometimes a very charming thing too.00000000";
    f1 = 1776;
    f4 = 1789;
    record_bytes = (uint8_t*) record_string.data();

    std::memcpy(&record_bytes[0], &f1, sizeof(f1));
    std::memcpy(&record_bytes[8+43+60], &f4, sizeof(f4));

    std::cout << "Inserting a second record..." << std::endl;
    table.insert_record(record_bytes);
    std::cout << "Printing table with two records..." << std::endl;
    std::cout << std::endl;
    table.print();
    std::cout << std::endl;

    record_string = "00000000Nullius in verba                           Premature optimization is the root of all evil              00000000";
    f1 = 481516;
    f4 = 2342;
    record_bytes = (uint8_t*) record_string.data();

    std::memcpy(&record_bytes[0], &f1, sizeof(f1));
    std::memcpy(&record_bytes[8+43+60], &f4, sizeof(f4));

    std::cout << "Inserting a third record..." << std::endl;
    table.insert_record(record_bytes);
    std::cout << "Printing table with three records..." << std::endl;
    std::cout << std::endl;
    table.print();
    std::cout << std::endl;

    std::cout << "Writing table to file..." << std::endl;
    write_to_file("./output.arrow", table);

    std::cout << "Reading table from file..." << std::endl;
    Table table_from_file = read_from_file("./output.arrow");
    std::cout << std::endl;
    table_from_file.print();
    std::cout << std::endl;


    record_string = "00000000When you add verbiage to a page,           you can assume that customers will read 18% of it.          00000000";
    f1 = 12020;
    f4 = 7;
    record_bytes = (uint8_t*) record_string.data();

    std::memcpy(&record_bytes[0], &f1, sizeof(f1));
    std::memcpy(&record_bytes[8+43+60], &f4, sizeof(f4));

    std::cout << "Inserting the another record 17 times so that we allocate another block..." << std::endl;
    for(int i=0; i<17; i++) table.insert_record(record_bytes);
    std::cout << "Printing table 20 records..." << std::endl;
    std::cout << std::endl;
    table.print();
    std::cout << std::endl;


}

