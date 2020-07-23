#include "ssb_workload.h"
#include "../table/util.h"
using namespace hustle::operators;
using namespace std::chrono;

void read_from_csv() {

    SSB workload(1, false);
    
    std::shared_ptr<Table> t;

    t = read_from_csv_file("/mydata/SQL-benchmark-data-generator/ssbgencustomer.tbl", workload.c_schema, BLOCK_SIZE);
    write_to_file("/mydata/SQL-benchmark-data-generator/ssbgen/customer.hsl", *t);
    std::cout << "c" << std::endl;

    t = read_from_csv_file("/mydata/SQL-benchmark-data-generator/ssbgen/supplier.tbl", workload.s_schema, BLOCK_SIZE);
    write_to_file("/mydata/SQL-benchmark-data-generator/ssbgen/supplier.hsl", *t);
    std::cout << "s" << std::endl;

    t = read_from_csv_file("/mydata/SQL-benchmark-data-generator/ssbgen/date.tbl", workload.d_schema, BLOCK_SIZE);
    write_to_file("/mydata/SQL-benchmark-data-generator/ssbgen/date.hsl", *t);
    std::cout << "d" << std::endl;

    t = read_from_csv_file("/mydata/SQL-benchmark-data-generator/ssbgen/part.tbl", workload.p_schema, BLOCK_SIZE);
    write_to_file("/mydata/SQL-benchmark-data-generator/ssbgen/part.hsl", *t);
    std::cout << "p" << std::endl;

    t = read_from_csv_file("/mydata/SQL-benchmark-data-generator/ssbgenlineorder.tbl", workload.lo_schema, 20*BLOCK_SIZE);
    write_to_file("/mydata/SQL-benchmark-data-generator/ssbgen/lineorder.hsl", *t);
    std::cout << "lo" << std::endl;

//    auto t = read_from_csv_file("/Users/corrado/hustle/src/ssb/data/ssb-01/customer.tbl", c_schema, 20*BLOCK_SIZE);
//    write_to_file("/Users/corrado/hustle/src/ssb/data/ssb-01-20MB/customer.hsl", *t);
//    std::cout << "c" << std::endl;
//
//    t = read_from_csv_file("/Users/corrado/hustle/src/ssb/data/ssb-01/supplier.tbl", s_schema, 20*BLOCK_SIZE);
//    write_to_file("/Users/corrado/hustle/src/ssb/data/ssb-01-20MB/supplier.hsl", *t);
//    std::cout << "s" << std::endl;
//
//    t = read_from_csv_file("/Users/corrado/hustle/src/ssb/data/ssb-01/date.tbl", d_schema, 20*BLOCK_SIZE);
//    write_to_file("/Users/corrado/hustle/src/ssb/data/ssb-01-20MB/date.hsl", *t);
//    std::cout << "d" << std::endl;
//
//    t = read_from_csv_file("/Users/corrado/hustle/src/ssb/data/ssb-01/part.tbl", p_schema, 20*BLOCK_SIZE);
//    write_to_file("/Users/corrado/hustle/src/ssb/data/ssb-01-20MB/part.hsl", *t);
//    std::cout << "p" << std::endl;
//
//    t = read_from_csv_file("/Users/corrado/hustle/src/ssb/data/ssb-10/lineorder.tbl", lo_schema, 20*BLOCK_SIZE);
//    write_to_file("/Users/corrado/hustle/src/ssb/data/ssb-10-20MB/lineorder.hsl", *t);
//    std::cout << "lo" << std::endl;
}

void run_experiment(int num_trials) {

//    read_from_csv();
//    SSB workload(10, false);
    SSB workload(1, true);
    std::cout << "sleeping..." << std::endl;
    sleep(0);
    for (int i = 0; i < num_trials; i++) {
        std::cout << "batch start" << std::endl;

        workload.q11();
        workload.q12();
        workload.q13();

//        workload.q21();
//        workload.q22();
//        workload.q23();
//
//        workload.q31();
//        workload.q32();
//        workload.q33();
//        workload.q34();
//
//        workload.q41();
//        workload.q42();
//        workload.q43();

        workload.q21_lip();
        workload.q22_lip();
        workload.q23_lip();

        workload.q31_lip();
        workload.q32_lip();
        workload.q33_lip();
        workload.q34_lip();

        workload.q41_lip();
        workload.q42_lip();
        workload.q43_lip();
    }

}

int main(int argc, char *argv[]) {

//    read_from_csv();
//    return 0;

    run_experiment(20);

//    SSB workload(1, true);
//    SSB workload(1, false);
//    SSB workload(10, false);

//    std::string input;
//    std::cout << ">> ";
//    std::getline(std::cin, input);

//    while (input != "exit") {
//        auto t1 = high_resolution_clock::now();



//        auto t2 = high_resolution_clock::now();
//        std::cout << duration_cast<milliseconds>(t2 - t1).count() << std::endl;
//
//        std::cout << ">> ";
//        std::getline(std::cin, input);
//    }
}