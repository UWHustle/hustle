#include "tatp_mysql_loader.h"
#include "benchmarks/tatp/tatp_util.h"
#include "random_generator.h"
#include <algorithm>
#include <climits>
#include <random>
#include <iostream>

txbench::TATPMySQLLoader::TATPMySQLLoader(int n_rows, const std::string &host,
                                          int port, const std::string &user)
    : n_rows_(n_rows), session_(host, port, user) {}

void txbench::TATPMySQLLoader::load() {
  std::random_device rd;
  std::mt19937 mt(rd());

  RandomGenerator rg;

  session_.startTransaction();

  mysqlx::Schema schema = session_.createSchema("tatp", true);

  session_.sql("DROP TABLE IF EXISTS tatp.Call_Forwarding;").execute();
  session_.sql("DROP TABLE IF EXISTS tatp.Special_Facility;").execute();
  session_.sql("DROP TABLE IF EXISTS tatp.Access_Info;").execute();
  session_.sql("DROP TABLE IF EXISTS tatp.Subscriber;").execute();

  session_
      .sql("CREATE TABLE tatp.Subscriber (s_id INTEGER, sub_nbr CHAR(15), "
           "bit_1 BOOLEAN, bit_2 BOOLEAN, bit_3 BOOLEAN, bit_4 BOOLEAN, "
           "bit_5 BOOLEAN, bit_6 BOOLEAN, bit_7 BOOLEAN, bit_8 BOOLEAN, "
           "bit_9 BOOLEAN, bit_10 BOOLEAN, "
           "hex_1 TINYINT, hex_2 TINYINT, hex_3 TINYINT, hex_4 TINYINT, "
           "hex_5 TINYINT, hex_6 TINYINT, hex_7 TINYINT, hex_8 TINYINT, "
           "hex_9 TINYINT, hex_10 TINYINT, "
           "byte2_1 SMALLINT, byte2_2 SMALLINT, byte2_3 SMALLINT, "
           "byte2_4 SMALLINT, byte2_5 SMALLINT, byte2_6 SMALLINT, "
           "byte2_7 SMALLINT, byte2_8 SMALLINT, byte2_9 SMALLINT, "
           "byte2_10 SMALLINT, "
           "msc_location INTEGER, vlr_location INTEGER, "
           "PRIMARY KEY (s_id));")
      .execute();

  session_
      .sql("CREATE TABLE tatp.Access_Info (s_id INTEGER, ai_type TINYINT, "
           "data1 SMALLINT, data2 SMALLINT, data3 CHAR(3), data4 CHAR(5), "
           "PRIMARY KEY (s_id, ai_type), "
           "FOREIGN KEY (s_id) REFERENCES tatp.Subscriber (s_id));")
      .execute();

  session_
      .sql("CREATE TABLE tatp.Special_Facility (s_id INTEGER, sf_type TINYINT, "
           "is_active BOOLEAN, error_cntrl SMALLINT, "
           "data_a SMALLINT, data_b CHAR(5), "
           "PRIMARY KEY (s_id, sf_type), "
           "FOREIGN KEY (s_id) REFERENCES tatp.Subscriber (s_id));")
      .execute();

  session_
      .sql("CREATE TABLE tatp.Call_Forwarding (s_id INTEGER, sf_type TINYINT, "
           "start_time TINYINT, end_time TINYINT, numberx CHAR(15), "
           "PRIMARY KEY (s_id, sf_type, start_time), "
           "FOREIGN KEY (s_id, sf_type) "
           "REFERENCES tatp.Special_Facility(s_id, sf_type));")
      .execute();

  std::vector<int> subscriber_ids(n_rows_);
  std::iota(subscriber_ids.begin(), subscriber_ids.end(), 1);
  std::shuffle(subscriber_ids.begin(), subscriber_ids.end(), mt);

  for (int subscriber_id : subscriber_ids) {
    mysqlx::Row row;

    row.set(0, subscriber_id);
    row.set(1, leading_zero_pad(15, std::to_string(subscriber_id)));

    for (int i = 0; i < 10; ++i) {
      row.set(2 + i, rg.random_bool());
    }

    for (int i = 0; i < 10; ++i) {
      row.set(12 + i, rg.random_int(0, 15));
    }

    for (int i = 0; i < 10; ++i) {
      row.set(22 + i, rg.random_int(0, 255));
    }

    row.set(32, rg.random_int(INT_MIN, INT_MAX));
    row.set(33, rg.random_int(INT_MIN, INT_MAX));

    schema.getTable("Subscriber").insert().values(row).execute();

    int access_info_rows = rg.random_int(1, 4);
    std::vector<int> ai_types_possible = {1, 2, 3, 4};
    std::vector<int> ai_types;
    std::sample(ai_types_possible.begin(), ai_types_possible.end(),
                std::back_inserter(ai_types), access_info_rows, mt);

    for (int ai_type : ai_types) {
      schema.getTable("Access_Info")
          .insert()
          .values(subscriber_id, ai_type, rg.random_int(0, 255),
                  rg.random_int(0, 255), uppercase_string(3, rg),
                  uppercase_string(5, rg))
          .execute();
    }

    int special_facility_rows = rg.random_int(1, 4);
    std::vector<int> sf_types_possible = {1, 2, 3, 4};
    std::vector<int> sf_types;
    std::sample(sf_types_possible.begin(), sf_types_possible.end(),
                std::back_inserter(sf_types), special_facility_rows, mt);

    for (int sf_type : sf_types) {
      schema.getTable("Special_Facility")
          .insert()
          .values(subscriber_id, sf_type, (int)(rg.random_int(0, 99) < 85),
                  rg.random_int(0, 255), rg.random_int(0, 255),
                  uppercase_string(5, rg))
          .execute();

      int call_forwarding_rows = rg.random_int(0, 3);
      std::vector<int> start_times_possible = {0, 8, 16};
      std::vector<int> start_times;
      std::sample(start_times_possible.begin(), start_times_possible.end(),
                  std::back_inserter(start_times), call_forwarding_rows, mt);

      for (int start_time : start_times) {
        schema.getTable("Call_Forwarding")
            .insert()
            .values(subscriber_id, sf_type, start_time,
                    start_time + rg.random_int(1, 8), uppercase_string(15, rg))
            .execute();
      }
    }
  }

  session_.commit();
  session_.close();
}
