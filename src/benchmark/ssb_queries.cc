//
// Created by SURYADEV on 14/02/21.
//
#include "ssb_queries.h"
#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <string.h>

namespace hustle::operators {
    SSBQueries::SSBQueries() {
        std::filesystem::remove_all("db_directory");
        hustle_db = std::make_shared<HustleDB>("db_directory");
        // it will only start if it is not running.
        CreateTable();
        hustle::HustleDB::startScheduler();
    }

    SSBQueries::~SSBQueries() { hustle::HustleDB::stopScheduler(); }

    void SSBQueries::CreateTable() {
        // Create table part
        hustle::catalog::TableSchema part("part");
        hustle::catalog::ColumnSchema p_partkey(
                "p_partkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema p_name(
                "p_name", {hustle::catalog::HustleType::CHAR, 22}, true, false);
        hustle::catalog::ColumnSchema p_mfgr(
                "p_mfgr", {hustle::catalog::HustleType::CHAR, 6}, true, false);
        hustle::catalog::ColumnSchema p_category(
                "p_category", {hustle::catalog::HustleType::CHAR, 7}, true, false);
        hustle::catalog::ColumnSchema p_brand1(
                "p_brand1", {hustle::catalog::HustleType::CHAR, 9}, true, false);
        hustle::catalog::ColumnSchema p_color(
                "p_color", {hustle::catalog::HustleType::CHAR, 11}, true, false);
        hustle::catalog::ColumnSchema p_type(
                "p_type", {hustle::catalog::HustleType::CHAR, 25}, true, false);
        hustle::catalog::ColumnSchema p_size(
                "p_size", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema p_container(
                "p_container", {hustle::catalog::HustleType::CHAR, 10}, true, false);
        part.addColumn(p_partkey);
        part.addColumn(p_name);
        part.addColumn(p_mfgr);
        part.addColumn(p_category);
        part.addColumn(p_brand1);
        part.addColumn(p_color);
        part.addColumn(p_type);
        part.addColumn(p_size);
        part.addColumn(p_container);
        part.setPrimaryKey({});
        p_schema = part.getArrowSchema();

        // Create table supplier
        hustle::catalog::TableSchema supplier("supplier");
        hustle::catalog::ColumnSchema s_suppkey(
                "s_suppkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema s_name(
                "s_name", {hustle::catalog::HustleType::CHAR, 25}, true, false);
        hustle::catalog::ColumnSchema s_address(
                "s_address", {hustle::catalog::HustleType::CHAR, 25}, true, false);
        hustle::catalog::ColumnSchema s_city(
                "s_city", {hustle::catalog::HustleType::CHAR, 10}, true, false);
        hustle::catalog::ColumnSchema s_nation(
                "s_nation", {hustle::catalog::HustleType::CHAR, 15}, true, false);
        hustle::catalog::ColumnSchema s_region(
                "s_region", {hustle::catalog::HustleType::CHAR, 12}, true, false);
        hustle::catalog::ColumnSchema s_phone(
                "s_phone", {hustle::catalog::HustleType::CHAR, 15}, true, false);
        supplier.addColumn(s_suppkey);
        supplier.addColumn(s_name);
        supplier.addColumn(s_address);
        supplier.addColumn(s_city);
        supplier.addColumn(s_nation);
        supplier.addColumn(s_region);
        supplier.addColumn(s_phone);
        supplier.setPrimaryKey({});

        s_schema = supplier.getArrowSchema();

        // Create table customer
        hustle::catalog::TableSchema customer("customer");
        hustle::catalog::ColumnSchema c_suppkey(
                "c_custkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema c_name(
                "c_name", {hustle::catalog::HustleType::CHAR, 25}, true, false);
        hustle::catalog::ColumnSchema c_address(
                "c_address", {hustle::catalog::HustleType::CHAR, 25}, true, false);
        hustle::catalog::ColumnSchema c_city(
                "c_city", {hustle::catalog::HustleType::CHAR, 10}, true, false);
        hustle::catalog::ColumnSchema c_nation(
                "c_nation", {hustle::catalog::HustleType::CHAR, 15}, true, false);
        hustle::catalog::ColumnSchema c_region(
                "c_region", {hustle::catalog::HustleType::CHAR, 12}, true, false);
        hustle::catalog::ColumnSchema c_phone(
                "c_phone", {hustle::catalog::HustleType::CHAR, 15}, true, false);
        hustle::catalog::ColumnSchema c_mktsegment(
                "c_mktsegment", {hustle::catalog::HustleType::CHAR, 10}, true, false);
        customer.addColumn(c_suppkey);
        customer.addColumn(c_name);
        customer.addColumn(c_address);
        customer.addColumn(c_city);
        customer.addColumn(c_nation);
        customer.addColumn(c_region);
        customer.addColumn(c_phone);
        customer.addColumn(c_mktsegment);
        customer.setPrimaryKey({});
        c_schema = customer.getArrowSchema();

        // Create table ddate
        hustle::catalog::TableSchema ddate("ddate");
        hustle::catalog::ColumnSchema d_datekey(
                "d_datekey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema d_date(
                "d_date", {hustle::catalog::HustleType::CHAR, 19}, true, false);
        hustle::catalog::ColumnSchema d_dayofweek(
                "d_dayofweek", {hustle::catalog::HustleType::CHAR, 10}, true, false);
        hustle::catalog::ColumnSchema d_month(
                "d_month", {hustle::catalog::HustleType::CHAR, 10}, true, false);
        hustle::catalog::ColumnSchema d_year(
                "d_year", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema d_yearmonthnum(
                "d_yearmonthnum", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema d_yearmonth(
                "d_yearmonth", {hustle::catalog::HustleType::CHAR, 8}, true, false);
        hustle::catalog::ColumnSchema d_daynuminweek(
                "d_daynuminweek", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema d_daynuminmonth(
                "d_daynuminmonth", {hustle::catalog::HustleType::INTEGER, 0}, true,
                false);
        hustle::catalog::ColumnSchema d_daynuminyear(
                "d_daynuminyear", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema d_monthnuminyear(
                "d_monthnuminyear", {hustle::catalog::HustleType::INTEGER, 0}, true,
                false);
        hustle::catalog::ColumnSchema d_weeknuminyear(
                "d_weeknuminyear", {hustle::catalog::HustleType::INTEGER, 0}, true,
                false);
        hustle::catalog::ColumnSchema d_sellingseason(
                "d_sellingseason", {hustle::catalog::HustleType::CHAR, 13}, true, false);
        hustle::catalog::ColumnSchema d_lastdayinweekfl(
                "d_lastdayinweekfl", {hustle::catalog::HustleType::CHAR, 1}, true, false);
        hustle::catalog::ColumnSchema d_lastdayinmonthfl(
                "d_lastdayinmonthfl", {hustle::catalog::HustleType::CHAR, 1}, true,
                false);
        hustle::catalog::ColumnSchema d_holidayfl(
                "d_holidayfl", {hustle::catalog::HustleType::CHAR, 1}, true, false);
        hustle::catalog::ColumnSchema d_weekdayfl(
                "d_weekdayfl", {hustle::catalog::HustleType::CHAR, 1}, true, false);
        ddate.addColumn(d_datekey);
        ddate.addColumn(d_date);
        ddate.addColumn(d_dayofweek);
        ddate.addColumn(d_month);
        ddate.addColumn(d_year);
        ddate.addColumn(d_yearmonthnum);
        ddate.addColumn(d_yearmonth);
        ddate.addColumn(d_daynuminweek);
        ddate.addColumn(d_daynuminmonth);
        ddate.addColumn(d_daynuminyear);
        ddate.addColumn(d_monthnuminyear);
        ddate.addColumn(d_weeknuminyear);
        ddate.addColumn(d_sellingseason);
        ddate.addColumn(d_lastdayinweekfl);
        ddate.addColumn(d_lastdayinmonthfl);
        ddate.addColumn(d_holidayfl);
        ddate.addColumn(d_weekdayfl);
        ddate.setPrimaryKey({});
        d_schema = ddate.getArrowSchema();

        // Create table lineorder
        hustle::catalog::TableSchema lineorder("lineorder");
        hustle::catalog::ColumnSchema lo_orderkey(
                "lo_orderkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_linenumber(
                "lo_linenumber", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_custkey(
                "lo_custkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_partkey(
                "lo_partkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_suppkey(
                "lo_suppkey", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_orderdate(
                "lo_orderdate", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_orderpriority(
                "lo_orderpriority", {hustle::catalog::HustleType::CHAR, 15}, true, false);
        hustle::catalog::ColumnSchema lo_shippriority(
                "lo_shippriority", {hustle::catalog::HustleType::CHAR, 1}, true, false);
        hustle::catalog::ColumnSchema lo_quantity(
                "lo_quantity", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_extendedprice(
                "lo_extendedprice", {hustle::catalog::HustleType::INTEGER, 0}, true,
                false);
        hustle::catalog::ColumnSchema lo_ordertotalprice(
                "lo_ordertotalprice", {hustle::catalog::HustleType::INTEGER, 0}, true,
                false);
        hustle::catalog::ColumnSchema lo_discount(
                "lo_discount", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_revenue(
                "lo_revenue", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_supplycost(
                "lo_supplycost", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_tax(
                "lo_tax", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_commitdate(
                "lo_commitdate", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
        hustle::catalog::ColumnSchema lo_shipmode(
                "lo_shipmode", {hustle::catalog::HustleType::CHAR, 10}, true, false);
        lineorder.addColumn(lo_orderkey);
        lineorder.addColumn(lo_linenumber);
        lineorder.addColumn(lo_custkey);
        lineorder.addColumn(lo_partkey);
        lineorder.addColumn(lo_suppkey);
        lineorder.addColumn(lo_orderdate);
        lineorder.addColumn(lo_orderpriority);
        lineorder.addColumn(lo_shippriority);
        lineorder.addColumn(lo_quantity);
        lineorder.addColumn(lo_extendedprice);
        lineorder.addColumn(lo_ordertotalprice);
        lineorder.addColumn(lo_discount);
        lineorder.addColumn(lo_revenue);
        lineorder.addColumn(lo_supplycost);
        lineorder.addColumn(lo_tax);
        lineorder.addColumn(lo_commitdate);
        lineorder.addColumn(lo_shipmode);
        lineorder.setPrimaryKey({});
        lo_schema = lineorder.getArrowSchema();
        /*
        DBTable::TablePtr t;

        t = read_from_csv_file("../../../ssb/data/customer.tbl", c_schema,
                               20 * BLOCK_SIZE);
        write_to_file("../../../ssb/data/customer.hsl", *t);

        t = read_from_csv_file("../../../ssb/data/supplier.tbl", s_schema,
                               20 * BLOCK_SIZE);
        write_to_file("../../../ssb/data/supplier.hsl", *t);

        t = read_from_csv_file("../../../ssb/data/date.tbl", d_schema,
                               20 * BLOCK_SIZE);
        write_to_file("../../../ssb/data/date.hsl", *t);

        t = read_from_csv_file("../../../ssb/data/part.tbl", p_schema,
                               20 * BLOCK_SIZE);
        write_to_file("../../../ssb/data/part.hsl", *t);

        t = read_from_csv_file("../../../ssb/data/lineorder.tbl", lo_schema,
                               20 * BLOCK_SIZE);
        write_to_file("../../../ssb/data/lineorder.hsl", *t);*/

        std::cout << "read the table files..." << std::endl;
        DBTable::TablePtr lo, c, s, p, d;
        lo = read_from_file("../../../ssb/data/lineorder.hsl", true, "lineorder");
        d = read_from_file("../../../ssb/data/date.hsl", true, "ddate");
        p = read_from_file("../../../ssb/data/part.hsl", true, "part");
        c = read_from_file("../../../ssb/data/customer.hsl", true, "customer");
        s = read_from_file("../../../ssb/data/supplier.hsl", true, "supplier");

        hustle_db->createTable(supplier, s);
        hustle_db->createTable(customer, c);
        hustle_db->createTable(ddate, d);
        hustle_db->createTable(part, p);
        hustle_db->createTable(lineorder, lo);

        FILE* stream = fopen("../../../ssb/data/lineorder.tbl", "r");
        char line[2048];
        std::string query = "BEGIN TRANSACTION;";
        while (fgets(line, 2048, stream)) {
            char* tmp = strdup(line);
            char** fields = getfields(tmp, 17);
            query += "INSERT INTO lineorder VALUES ("+std::string(fields[0])+", "+std::string(fields[1])+", "+std::string(fields[2])+", "+std::string(fields[3])+ ","\
        ""+std::string(fields[4])+", "+std::string(fields[5])+", '"+std::string(fields[6])+"', '"+std::string(fields[7])+"', "+std::string(fields[8])+", "+std::string(fields[9])+", "+std::string(fields[10])+", "+std::string(fields[11])+", "+std::string(fields[12])+", "+std::string(fields[13])+", "+std::string(fields[14])+", "+std::string(fields[15])+", '"+std::string(fields[16])+"');\n";
        }
        query += "COMMIT;";
        hustle_db->executeQuery(query);

        stream = fopen("../../../ssb/data/part.tbl", "r");
        query = "BEGIN TRANSACTION;";
        while (fgets(line, 2048, stream)) {
            char* tmp = strdup(line);
            char** fields = getfields(tmp, 17);
            query +=  "INSERT INTO part VALUES ("+std::string(fields[0])+", "+std::string(fields[1])+", "+std::string(fields[2])+", "+std::string(fields[3])+", "\
        ""+std::string(fields[4])+", "+std::string(fields[5])+", '"+std::string(fields[6])+"', '"+std::string(fields[7])+"', "+std::string(fields[8])+");\n";
        }
        query += "COMMIT;";
        hustle_db->executeQuery(query);

        stream = fopen("../../../ssb/data/supplier.tbl", "r");
        query = "BEGIN TRANSACTION;";
        while (fgets(line, 2048, stream)) {
            char* tmp = strdup(line);
            char** fields = getfields(tmp, 17);
            std::string query = "INSERT INTO supplier VALUES ("+std::string(fields[0])+", '"+ std::string(fields[1])+"', '"+ std::string(fields[2])+"', '"+ std::string(fields[3])+"', "\
        "'"+ std::string(fields[4])+"', '"+ std::string(fields[5])+"', '"+std::string(fields[6])+"', '"+std::string(fields[7])+"');\n";
        }
        query += "COMMIT;";
        hustle_db->executeQuery(query);

        stream = fopen("../../../ssb/data/customer.tbl", "r");
        query = "BEGIN TRANSACTION;";
        while (fgets(line, 2048, stream)) {
            char* tmp = strdup(line);
            char** fields = getfields(tmp, 17);
            std::string query = "INSERT INTO customer VALUES ("+std::string(fields[0])+", '"+ std::string(fields[1])+"', '"+ std::string(fields[2]) +"', '"+ std::string(fields[3]) +"', "\
        "'"+ std::string(fields[4]) +"', '"+std::string(fields[5]) +"', '"+std::string(fields[6])+"', '"+std::string(fields[7])+"');\n";
            hustle_db->executeQuery(query);
        }
        query += "COMMIT;";
        hustle_db->executeQuery(query);

        stream = fopen("../../../ssb/data/date.tbl", "r");
        query = "BEGIN TRANSACTION;";
        while (fgets(line, 2048, stream)) {
            char* tmp = strdup(line);
            char** fields = getfields(tmp, 17);
            std::string query = "INSERT INTO ddate VALUES ("+std::string(fields[0])+", '"+ std::string(fields[1])+"', '"+ std::string(fields[2]) +"', '"+ std::string(fields[3]) + "',"\
        ""+std::string(fields[4])+", "+std::string(fields[5])+", '"+std::string(fields[6])+"', "+std::string(fields[7])+", "+std::string(fields[8])+", "+std::string(fields[9])+", "+std::string(fields[10])+", "+std::string(fields[11])+", '"+ std::string(fields[12]) +"', "+std::string(fields[13])+", "+std::string(fields[14])+", "+std::string(fields[15])+", "+ std::string(fields[16])+");\n";
        }
        query += "COMMIT;";
        hustle_db->executeQuery(query);
    }


    char** SSBQueries::getfields (char* line, int num){
        char **fields = (char **)malloc(num * sizeof(char*));
        char* field;
        int index = 0;
        for (field = strtok(line, "|");
             field && *field;
             field = strtok(NULL, "|\n")) {
            fields[index++] = field;
        }
        return fields;
    }

    void SSBQueries::q11() {
        std::cout << "q11" << std::endl;
        std::string query =
                "select sum(lo_extendedprice) as "
                "revenue "
                "from lineorder, ddate "
                "where lo_orderdate = d_datekey and d_year = 1993 and ((lo_discount "
                "between 1 and 3) and lo_quantity < 25);";
        query =
                "select sum(lo_extendedprice) as "
                "revenue "
                "from lineorder "
                "where  lo_quantity < 25;";
        hustle_db->executeQuery(query);
    }

    void SSBQueries::q12() {
        std::cout << "q12" << std::endl;
        std::string query =
                "select sum(lo_extendedprice * lo_discount) as "
                "revenue\n"
                "from lineorder, ddate\n"
                "where lo_orderdate = d_datekey\n"
                "and d_yearmonthnum = 199401\n"
                "and (lo_discount BETWEEN 4 and 6\n"
                "and lo_quantity BETWEEN 26 and 35);";
        hustle_db->executeQuery(query);
    }

    void SSBQueries::q13() {
        std::cout << "q13" << std::endl;
        std::string query =
                "select sum(lo_extendedprice * lo_discount) as "
                "revenue\n"
                "from lineorder, ddate\n"
                "where lo_orderdate = d_datekey\n"
                "and d_weeknuminyear = 6 and d_year = 1994\n"
                "and (lo_discount BETWEEN 5 and 7\n"
                "and lo_quantity BETWEEN 36 and 40);";
        hustle_db->executeQuery(query);
    }

    void SSBQueries::q21() {
        std::cout << "q21" << std::endl;
        std::string query =
                "select sum(lo_revenue), d_year, p_brand1\n"
                "from lineorder, ddate, part, supplier\n"
                "where lo_partkey = p_partkey\n"
                "and lo_suppkey = s_suppkey\n"
                "and lo_orderdate = d_datekey\n"
                "and p_category = 'MFGR#12'\n"
                "and s_region = 'AMERICA'\n"
                "group by d_year, p_brand1\n"
                "order by d_year, p_brand1;";
        hustle_db->executeQuery(query);
    }

    void SSBQueries::q22() {
        std::string query =
                "select sum(lo_revenue), d_year, p_brand1\n"
                "\tfrom lineorder, ddate, part, supplier\n"
                "\twhere lo_partkey = p_partkey\n"
                "\t\tand lo_suppkey = s_suppkey\n"
                "\t\tand lo_orderdate = d_datekey\n"
                "\t\tand p_brand1 > 'MFGR#2221'\n" /*and p_brand1 < 'MFGR#2228'\n"*/
                "\t\tand s_region = 'ASIA'\n"
                "\tgroup by d_year, p_brand1\n"
                "\torder by d_year, p_brand1;";
        hustle_db->executeQuery(query);
    }

    void SSBQueries::q23() {
        std::string query =
                "select sum(lo_revenue), d_year, p_brand1\n"
                "\tfrom lineorder, ddate, part, supplier\n"
                "\twhere lo_partkey = p_partkey\n"
                "\t\tand lo_suppkey = s_suppkey\n"
                "\t\tand lo_orderdate = d_datekey\n"
                "\t\tand p_brand1 = 'MFGR#2221'\n"
                "\t\tand s_region = 'EUROPE'\n"
                "\tgroup by d_year, p_brand1\n"
                "\torder by d_year, p_brand1;";
        hustle_db->executeQuery(query);
    }

    void SSBQueries::q31() {
        std::string query =
                "select c_nation, s_nation, d_year, sum(lo_revenue) "
                "as revenue\n"
                "\tfrom customer, lineorder, supplier, ddate\n"
                "\twhere lo_custkey = c_custkey\n"
                "\t\tand lo_suppkey = s_suppkey\n"
                "\t\tand lo_orderdate = d_datekey\n"
                "\t\tand c_region = 'ASIA'\n"
                "\t\tand s_region = 'ASIA'\n"
                "\t\tand d_year >= 1992 and d_year <= 1997\n"
                "\tgroup by c_nation, s_nation, d_year\n"
                "\torder by d_year, revenue;";
        hustle_db->executeQuery(query);
    }

    void SSBQueries::q32() {
        std::string query =
                "select c_city, s_city, d_year, sum(lo_revenue) as "
                "revenue\n"
                "\tfrom customer, lineorder, supplier, ddate\n"
                "\twhere lo_custkey = c_custkey\n"
                "\t\tand lo_suppkey = s_suppkey\n"
                "\t\tand lo_orderdate = d_datekey\n"
                "\t\tand c_nation = 'UNITED STATES'\n"
                "\t\tand s_nation = 'UNITED STATES'\n"
                "\t\tand d_year >= 1992 and d_year <= 1997\n"
                "\tgroup by c_city, s_city, d_year\n"
                "\torder by d_year, revenue;";
        hustle_db->executeQuery(query);
    }

    void SSBQueries::q33() {
        std::string query =
                "select c_city, s_city, d_year, sum(lo_revenue) as "
                "revenue\n"
                "\tfrom customer, lineorder, supplier, ddate\n"
                "\twhere lo_custkey = c_custkey\n"
                "\t\tand lo_suppkey = s_suppkey\n"
                "\t\tand lo_orderdate = d_datekey\n"
                "\t\tand (c_nation = 'UNITED KINGDOM'\n"
                "\t\tand (c_city='UNITED KI1' or c_city='UNITED KI5'))\n"
                "\t\tand ((s_city='UNITED KI1' or s_city='UNITED KI5')\n"
                "\t\tand s_nation = 'UNITED_KINGDOM')\n"
                "\t\tand d_year >= 1992 and d_year <= 1997\n"
                "\tgroup by c_city, s_city, d_year\n"
                "\torder by d_year, revenue;";
        hustle_db->executeQuery(query);
    }

    void SSBQueries::q34() {
        std::string query =
                "select c_city, s_city, d_year, sum(lo_revenue) as "
                "revenue\n"
                "\tfrom customer, lineorder, supplier, ddate\n"
                "\twhere lo_custkey = c_custkey\n"
                "\t\tand lo_suppkey = s_suppkey\n"
                "\t\tand lo_orderdate = d_datekey\n"
                "\t\tand (c_nation = 'UNITED KINGDOM'\n"
                "\t\tand (c_city='UNITED KI1' or c_city='UNITED KI5'))\n"
                "\t\tand ((s_city='UNITED KI1' or s_city='UNITED KI5')\n"
                "\t\tand s_nation = 'UNITED KINGDOM')"
                "\t\tand d_yearmonth = 'Dec1997'\n"
                "\tgroup by c_city, s_city, d_year\n"
                "\torder by d_year, revenue;";
        hustle_db->executeQuery(query);
    }

    void SSBQueries::q41() {
        std::string query =
                "select d_year, c_nation, "
                "sum(lo_revenue-lo_supplycost) as profit1\n"
                "\tfrom ddate, customer, supplier, part, lineorder\n"
                "\twhere lo_partkey = p_partkey\n"
                "\t\tand lo_suppkey = s_suppkey\n"
                "\t\tand lo_custkey = c_custkey\n"
                "\t\tand lo_orderdate = d_datekey\n"
                "\t\tand c_region = 'AMERICA'\n"
                "\t\tand s_region = 'AMERICA'\n"
                "\t\tand (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')\n"
                "\tgroup by d_year, c_nation\n"
                "\torder by d_year, c_nation;";
        hustle_db->executeQuery(query);
    }

    void SSBQueries::q42() {
        std::string query =
                "select d_year, s_nation, p_category, sum(lo_revenue - lo_supplycost) "
                "as profit1\n"
                "\tfrom ddate, customer, supplier, part, lineorder\n"
                "\twhere lo_partkey = p_partkey\n"
                "\t\tand lo_suppkey = s_suppkey\n"
                "\t\tand lo_custkey = c_custkey\n"
                "\t\tand lo_orderdate = d_datekey\n"
                "\t\tand c_region = 'AMERICA'\n"
                "\t\tand s_region = 'AMERICA'\n"
                "\t\tand (d_year = 1997 or d_year = 1998)\n"
                "\t\tand (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')\n"
                "\tgroup by d_year, s_nation, p_category\n"
                "\torder by d_year, s_nation, p_category;";
        hustle_db->executeQuery(query);
    }

    void SSBQueries::q43() {
        std::string query =
                "select d_year, s_city, p_brand1, sum(lo_revenue-lo_supplycost) as "
                "profit1\n"
                "\tfrom ddate, customer, supplier, part, lineorder\n"
                "\twhere lo_partkey = p_partkey\n"
                "\t\tand lo_suppkey = s_suppkey\n"
                "\t\tand lo_custkey = c_custkey\n"
                "\t\tand lo_orderdate = d_datekey\n"
                "\t\tand c_region = 'AMERICA'\n"
                "\t\tand s_nation = 'UNITED STATES'\n"
                "\t\tand (d_year = 1997 or d_year = 1998)\n"
                "\t\tand p_category = 'MFGR#14'\n"
                "\tgroup by d_year, s_city, p_brand1\n"
                "\torder by d_year, s_city, p_brand1;";
        hustle_db->executeQuery(query);
    }
}
