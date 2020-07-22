#include "ssb_workload.h"
#include "../table/util.h"
using namespace hustle::operators;
using namespace std::chrono;

void read_from_csv() {

    std::shared_ptr<Table> lo, c, s, p, d;
    std::shared_ptr<arrow::Schema> lo_schema, c_schema, s_schema, p_schema, d_schema;


    auto field1 = arrow::field("order key", arrow::uint32());
    auto field2 = arrow::field("line number", arrow::int64());
    auto field3 = arrow::field("cust key", arrow::int64());
    auto field4 = arrow::field("part key", arrow::int64());
    auto field5 = arrow::field("supp key", arrow::int64());
    auto field6 = arrow::field("order date", arrow::int64());
    auto field7 = arrow::field("ord priority", arrow::utf8());
    auto field8 = arrow::field("ship priority", arrow::int64());
    auto field9 = arrow::field("quantity", arrow::uint8());
    auto field10 = arrow::field("extended price", arrow::int64());
    auto field11 = arrow::field("ord total price", arrow::int64());
    auto field12 = arrow::field("discount", arrow::uint8());
    auto field13 = arrow::field("revenue", arrow::int64());
    auto field14 = arrow::field("supply cost", arrow::int64());
    auto field15 = arrow::field("tax", arrow::int64());
    auto field16 = arrow::field("commit date", arrow::int64());
    auto field17 = arrow::field("ship mode", arrow::utf8());
    lo_schema=arrow::schema({field1,field2,field3,field4,field5,
            field6,field7,field8,field9,field10,
            field11,field12,field13,field14,field15,
            field16,field17});


    std::shared_ptr<arrow::Field>s_field1=arrow::field("s supp key",
    arrow::int64());
    std::shared_ptr<arrow::Field>s_field2=arrow::field("s name",
    arrow::utf8());
    std::shared_ptr<arrow::Field>s_field3=arrow::field("s address",
    arrow::utf8());
    std::shared_ptr<arrow::Field>s_field4=arrow::field("s city",
    arrow::utf8());
    std::shared_ptr<arrow::Field>s_field5=arrow::field("s nation",
    arrow::utf8());
    std::shared_ptr<arrow::Field>s_field6=arrow::field("s region",
    arrow::utf8());
    std::shared_ptr<arrow::Field>s_field7=arrow::field("s phone",
    arrow::utf8());

    s_schema=arrow::schema({s_field1,s_field2,s_field3,s_field4,
            s_field5,
            s_field6,s_field7});


    std::shared_ptr<arrow::Field>c_field1=arrow::field("c cust key",
    arrow::int64());
    std::shared_ptr<arrow::Field>c_field2=arrow::field("c name",
    arrow::utf8());
    std::shared_ptr<arrow::Field>c_field3=arrow::field("c address",
    arrow::utf8());
    std::shared_ptr<arrow::Field>c_field4=arrow::field("c city",
    arrow::utf8());
    std::shared_ptr<arrow::Field>c_field5=arrow::field("c nation",
    arrow::utf8());
    std::shared_ptr<arrow::Field>c_field6=arrow::field("c region",
    arrow::utf8());
    std::shared_ptr<arrow::Field>c_field7=arrow::field("c phone",
    arrow::utf8());
    std::shared_ptr<arrow::Field>c_field8=arrow::field("c mkt segment",
    arrow::utf8());

    c_schema=arrow::schema({c_field1,c_field2,c_field3,c_field4,
            c_field5,c_field6,c_field7,c_field8});

    std::shared_ptr<arrow::Field>d_field1=arrow::field("date key",
    arrow::int64());
    std::shared_ptr<arrow::Field>d_field2=arrow::field("date",
    arrow::utf8());
    std::shared_ptr<arrow::Field>d_field3=arrow::field("day of week",
    arrow::utf8());
    std::shared_ptr<arrow::Field>d_field4=arrow::field("month",
    arrow::utf8());
    std::shared_ptr<arrow::Field>d_field5=arrow::field("year",
    arrow::int64());
    std::shared_ptr<arrow::Field>d_field6=arrow::field("year month num",
    arrow::int64());
    std::shared_ptr<arrow::Field>d_field7=arrow::field("year month",
    arrow::utf8());
    std::shared_ptr<arrow::Field>d_field8=arrow::field("day num in week",
    arrow::int64());
    std::shared_ptr<arrow::Field>d_field9=arrow::field("day num in "
    "month",
    arrow::int64());
    std::shared_ptr<arrow::Field>d_field10=arrow::field("day num in "
    "year",
    arrow::int64());
    std::shared_ptr<arrow::Field>d_field11=arrow::field("month num in "
    "year",
    arrow::int64());
    std::shared_ptr<arrow::Field>d_field12=arrow::field("week num in "
    "year",
    arrow::int64());
    std::shared_ptr<arrow::Field>d_field13=arrow::field("selling season",
    arrow::utf8());
    std::shared_ptr<arrow::Field>d_field14=arrow::field("last day in "
    "week fl",
    arrow::int64());
    std::shared_ptr<arrow::Field>d_field15=arrow::field("last day in "
    "month fl",
    arrow::int64());
    std::shared_ptr<arrow::Field>d_field16=arrow::field("holiday fl",
    arrow::int64());
    std::shared_ptr<arrow::Field>d_field17=arrow::field("weekday fl",
    arrow::int64());

    d_schema=arrow::schema({d_field1,d_field2,d_field3,d_field4,d_field5,
            d_field6,d_field7,d_field8,d_field9,d_field10,
            d_field11,d_field12,d_field13,d_field14,d_field15,
            d_field16,d_field17});




    std::shared_ptr<arrow::Field>p_field1=arrow::field("part key",
    arrow::int64());
    std::shared_ptr<arrow::Field>p_field2=arrow::field("name",
    arrow::utf8());
    std::shared_ptr<arrow::Field>p_field3=arrow::field("mfgr",
    arrow::utf8());
    std::shared_ptr<arrow::Field>p_field4=arrow::field("category",
    arrow::utf8());
    std::shared_ptr<arrow::Field>p_field5=arrow::field("brand1",
    arrow::utf8());
    std::shared_ptr<arrow::Field>p_field6=arrow::field("color",
    arrow::utf8());
    std::shared_ptr<arrow::Field>p_field7=arrow::field("type",
    arrow::utf8());
    std::shared_ptr<arrow::Field>p_field8=arrow::field("size",
    arrow::int64());
    std::shared_ptr<arrow::Field>p_field9=arrow::field("container",
    arrow::utf8());

    p_schema=arrow::schema({p_field1,p_field2,p_field3,p_field4,
            p_field5,
            p_field6,p_field7,p_field8,
            p_field9});


    std::shared_ptr<Table> t;
    t = read_from_csv_file("/Users/corrado/hustle/src/ssb/data/ssb-01/customer.tbl", c_schema, BLOCK_SIZE);
    write_to_file("/Users/corrado/hustle/src/ssb/data/ssb-01/customer.hsl", *t);
    std::cout << "c" << std::endl;
//
//    t = read_from_csv_file("/Users/corrado/hustle/src/ssb/data/ssb-01-10MB/supplier.tbl", s_schema, BLOCK_SIZE);
//    write_to_file("/Users/corrado/hustle/src/ssb/data/ssb-01/supplier.hsl", *t);
//    std::cout << "s" << std::endl;
//
//    t = read_from_csv_file("/Users/corrado/hustle/src/ssb/data/ssb-01-10MB/date.tbl", d_schema, BLOCK_SIZE);
//    write_to_file("/Users/corrado/hustle/src/ssb/data/ssb-01/date.hsl", *t);
//    std::cout << "d" << std::endl;
//
//    t = read_from_csv_file("/Users/corrado/hustle/src/ssb/data/ssb-01-10MB/part.tbl", p_schema, BLOCK_SIZE);
//    write_to_file("/Users/corrado/hustle/src/ssb/data/ssb-01/part.hsl", *t);
//    std::cout << "p" << std::endl;

//    t = read_from_csv_file("/Users/corrado/hustle/src/ssb/data/ssb-01/lineorder.tbl", lo_schema, BLOCK_SIZE);
//    write_to_file("/Users/corrado/hustle/src/ssb/data/ssb-01/lineorder.hsl", *t);
//    std::cout << "lo" << std::endl;

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

    SSB workload(10, false);
    sleep(20);
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