#include "tatp_hustle_loader.h"

#include <algorithm>
#include <climits>
#include <iostream>
#include <random>

#include "tatp_hustle_benchmark.h"
#include "txbench/benchmarks/tatp/tatp_util.h"
#include "txbench/random_generator.h"

txbench::TATPHustleLoader::TATPHustleLoader(int n_rows, const std::string &host,
                                            int port, const std::string &user)
    : n_rows_(n_rows) {}

void txbench::TATPHustleLoader::load() {
  std::random_device rd;
  std::mt19937 mt(rd());

  RandomGenerator rg;
  hustle::HustleDB::startScheduler();
  std::shared_ptr<arrow::Schema> s_schema, ai_schema, sf_schema, cf_schema;
  std::shared_ptr<hustle::HustleDB> hustle_db =
      txbench::TATPHustleBenchmark::getHustleDB();
  hustle::catalog::TableSchema subscriber("Subscriber");
  hustle::catalog::ColumnSchema s_id(
      "s_id", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema sub_nbr(
      "sub_nbr", {hustle::catalog::HustleType::CHAR, 15}, true, false);
  hustle::catalog::ColumnSchema bit_1(
      "bit_1", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema bit_2(
      "bit_2", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema bit_3(
      "bit_3", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema bit_4(
      "bit_4", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema bit_5(
      "bit_5", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema bit_6(
      "bit_6", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema bit_7(
      "bit_7", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema bit_8(
      "bit_8", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema bit_9(
      "bit_9", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema bit_10(
      "bit_10", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema hex_1(
      "hex_1", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema hex_2(
      "hex_2", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema hex_3(
      "hex_3", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema hex_4(
      "hex_4", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema hex_5(
      "hex_5", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema hex_6(
      "hex_6", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema hex_7(
      "hex_7", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema hex_8(
      "hex_8", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema hex_9(
      "hex_9", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema hex_10(
      "hex_10", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema byte2_1(
      "byte2_1", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema byte2_2(
      "byte2_2", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema byte2_3(
      "byte2_3", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema byte2_4(
      "byte2_4", {hustle::catalog::HustleType::INTEGER, 0}, true, false);

  hustle::catalog::ColumnSchema byte2_5(
      "byte2_5", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema byte2_6(
      "byte2_6", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema byte2_7(
      "byte2_7", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema byte2_8(
      "byte2_8", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema byte2_9(
      "byte2_9", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema byte2_10(
      "byte2_10", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema msc_location(
      "msc_location", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema vlr_location(
      "vlr_location", {hustle::catalog::HustleType::INTEGER, 0}, true, false);

  subscriber.addColumn(s_id);
  subscriber.addColumn(sub_nbr);
  subscriber.addColumn(bit_1);
  subscriber.addColumn(bit_2);
  subscriber.addColumn(bit_3);
  subscriber.addColumn(bit_4);
  subscriber.addColumn(bit_5);
  subscriber.addColumn(bit_6);
  subscriber.addColumn(bit_7);
  subscriber.addColumn(bit_8);
  subscriber.addColumn(bit_9);
  subscriber.addColumn(bit_10);
  subscriber.addColumn(hex_1);
  subscriber.addColumn(hex_2);
  subscriber.addColumn(hex_3);
  subscriber.addColumn(hex_4);
  subscriber.addColumn(hex_5);
  subscriber.addColumn(hex_6);
  subscriber.addColumn(hex_7);
  subscriber.addColumn(hex_8);
  subscriber.addColumn(hex_9);
  subscriber.addColumn(hex_10);
  subscriber.addColumn(byte2_1);
  subscriber.addColumn(byte2_2);
  subscriber.addColumn(byte2_3);
  subscriber.addColumn(byte2_4);
  subscriber.addColumn(byte2_5);
  subscriber.addColumn(byte2_6);
  subscriber.addColumn(byte2_7);
  subscriber.addColumn(byte2_8);
  subscriber.addColumn(byte2_9);
  subscriber.addColumn(byte2_10);
  subscriber.addColumn(msc_location);
  subscriber.addColumn(vlr_location);
  subscriber.setPrimaryKey({});
  s_schema = subscriber.getArrowSchema();

  hustle::catalog::TableSchema access_info("Access_Info");
  /* hustle::catalog::ColumnSchema s_id(
       "s_id", {hustle::catalog::HustleType::INTEGER, 0}, true, false);*/
  hustle::catalog::ColumnSchema ai_type(
      "ai_type", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema data1(
      "data1", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema data2(
      "data2", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema data3(
      "data3", {hustle::catalog::HustleType::CHAR, 3}, true, false);
  hustle::catalog::ColumnSchema data4(
      "data4", {hustle::catalog::HustleType::CHAR, 5}, true, false);
  access_info.addColumn(s_id);
  access_info.addColumn(ai_type);
  access_info.addColumn(data1);
  access_info.addColumn(data2);
  access_info.addColumn(data3);
  access_info.addColumn(data4);

  access_info.setPrimaryKey({});
  ai_schema = access_info.getArrowSchema();

  hustle::catalog::TableSchema special_facility("Special_Facility");
  hustle::catalog::ColumnSchema sf_s_id(
      "sf_s_id", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema sf_sf_type(
      "sf_sf_type", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema is_active(
      "is_active", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema error_cntrl(
      "error_cntrl", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema data_a(
      "data_a", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema data_b(
      "data_b", {hustle::catalog::HustleType::CHAR, 5}, true, false);
  special_facility.addColumn(sf_s_id);
  special_facility.addColumn(sf_sf_type);
  special_facility.addColumn(is_active);
  special_facility.addColumn(error_cntrl);
  special_facility.addColumn(data_a);
  special_facility.addColumn(data_b);
  special_facility.setPrimaryKey({});
  sf_schema = special_facility.getArrowSchema();

  hustle::catalog::TableSchema call_forwarding("Call_Forwarding");
  hustle::catalog::ColumnSchema cf_s_id(
      "cf_s_id", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema cf_sf_type(
      "cf_sf_type", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema start_time(
      "start_time", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema end_time(
      "end_time", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema numberx(
      "numberx", {hustle::catalog::HustleType::CHAR, 15}, true, false);
  call_forwarding.addColumn(cf_s_id);
  call_forwarding.addColumn(cf_sf_type);
  call_forwarding.addColumn(start_time);
  call_forwarding.addColumn(end_time);
  call_forwarding.addColumn(numberx);
  call_forwarding.setPrimaryKey({});
  cf_schema = call_forwarding.getArrowSchema();

  std::shared_ptr<DBTable> s, ai, sf, cf;

  s = std::make_shared<hustle::storage::DBTable>("Subscriber", s_schema, BLOCK_SIZE);
  ai = std::make_shared<hustle::storage::DBTable>("Access_Info", ai_schema,
                                                  BLOCK_SIZE);
  sf = std::make_shared<hustle::storage::DBTable>("Special_Facility", sf_schema,
                                                  BLOCK_SIZE);
  cf = std::make_shared<hustle::storage::DBTable>("Call_Forwarding", cf_schema,
                                                  BLOCK_SIZE);

  hustle_db->createTable(subscriber, s);

  hustle_db->createTable(access_info, ai);

  hustle_db->createTable(special_facility, sf);
  hustle_db->createTable(call_forwarding, cf);

  std::vector<int> subscriber_ids(n_rows_);
  std::iota(subscriber_ids.begin(), subscriber_ids.end(), 1);
  std::shuffle(subscriber_ids.begin(), subscriber_ids.end(), mt);

  std::string s_query = "BEGIN TRANSACTION; ";
  std::string ac_query = "BEGIN TRANSACTION; ";
  std::string sf_query = "BEGIN TRANSACTION; ";
  std::string cf_query = "BEGIN TRANSACTION; ";
  for (int subscriber_id : subscriber_ids) {
    s_query += "INSERT INTO Subscriber VALUES (";
    s_query +=  std::to_string(subscriber_id) + ",";
    s_query += leading_zero_pad(15, std::to_string(subscriber_id)) + ",";

    for (int i = 0; i < 10; ++i) {
      s_query += std::to_string(rg.random_bool()) + ",";
    }

    for (int i = 0; i < 10; ++i) {
      s_query +=  std::to_string(rg.random_int(0, 15)) + ",";
    }

    for (int i = 0; i < 10; ++i) {
      s_query +=  std::to_string(rg.random_int(0, 255)) + ",";
    }

    s_query +=  std::to_string(rg.random_int(INT_MIN, INT_MAX)) + ",";
    s_query +=  std::to_string(rg.random_int(INT_MIN, INT_MAX));
    s_query += ");";

    int access_info_rows = rg.random_int(1, 4);
    std::vector<int> ai_types_possible = {1, 2, 3, 4};
    std::vector<int> ai_types;
    std::sample(ai_types_possible.begin(), ai_types_possible.end(),
                std::back_inserter(ai_types), access_info_rows, mt);

    for (int ai_type : ai_types) {
      ac_query += "INSERT INTO Access_Info VALUES (" +
                  std::to_string(subscriber_id) + ", " + std::to_string(ai_type) +
                  ","
                  "" +
                  std::to_string(rg.random_int(0, 255)) + "," + std::to_string(rg.random_int(0, 255)) +
                  ","
                  "'" +
                  uppercase_string(3, rg) + "', '" + uppercase_string(5, rg) +
                  "');";
    }

    int special_facility_rows = rg.random_int(1, 4);
    std::vector<int> sf_types_possible = {1, 2, 3, 4};
    std::vector<int> sf_types;
    std::sample(sf_types_possible.begin(), sf_types_possible.end(),
                std::back_inserter(sf_types), special_facility_rows, mt);

    for (int sf_type : sf_types) {
      sf_query += "INSERT INTO Special_Facility VALUES (" +
                  std::to_string(subscriber_id) + ", " + std::to_string(sf_type) + ", " +
                  std::to_string((int)(rg.random_int(0, 99) < 85)) + ", " +
                  std::to_string(rg.random_int(0, 255)) + ", " + std::to_string(rg.random_int(0, 255)) + ", '" +
                  uppercase_string(5, rg) + "');";

      int call_forwarding_rows = rg.random_int(0, 3);
      std::vector<int> start_times_possible = {0, 8, 16};
      std::vector<int> start_times;
      std::sample(start_times_possible.begin(), start_times_possible.end(),
                  std::back_inserter(start_times), call_forwarding_rows, mt);

      for (int start_time : start_times) {
        cf_query += "INSERT INTO Call_Forwarding VALUES (" +
                    std::to_string(subscriber_id) + ", " + std::to_string(sf_type) + ", " +
                    std::to_string(start_time) + ", " + std::to_string(start_time + rg.random_int(1, 8)) +
                    ", '" + uppercase_string(15, rg) + "');";
      }
    }
  }

  s_query += "COMMIT;";
  ac_query += "COMMIT;";
  sf_query += "COMMIT;";
  cf_query += "COMMIT;";
  hustle_db->executeQuery(s_query);
  hustle_db->executeQuery(ac_query);
  hustle_db->executeQuery(sf_query);
  hustle_db->executeQuery(cf_query);

  //hustle::HustleDB::stopScheduler();
  std::cout << "loaded the values" << std::endl;
}
