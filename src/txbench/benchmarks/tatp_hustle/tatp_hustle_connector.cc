#include "txbench/benchmarks/tatp_hustle/tatp_hustle_connector.h"

#include <iostream>
#include <memory>
#include <chrono>
#include "storage/table.h"

int txbench::TATPHustleConnector::query_type = -1;

txbench::TATPHustleConnector::TATPHustleConnector(const std::string &host,
                                                  int port,
                                                  const std::string &user) {
  hustle_db = txbench::TATPHustleBenchmark::getHustleDB();
}

using clock1 = std::chrono::system_clock;
using sec = std::chrono::duration<double>;

double txbench::TATPHustleConnector::getSubscriberData(
    int s_id, std::string *sub_nbr, std::array<bool, 10> &bit,
    std::array<int, 10> &hex, std::array<int, 10> &byte2, int *msc_location,
    int *vlr_location) {
  std::string query =
      "SELECT s_id, sub_nbr,"
      "bit_1, bit_2, bit_3, bit_4, bit_5, bit_6, bit_7,"
      "bit_8, bit_9, bit_10,"
      "hex_1, hex_2, hex_3, hex_4, hex_5, hex_6, hex_7,"
      "hex_8, hex_9, hex_10,"
      "byte2_1, byte2_2, byte2_3, byte2_4, byte2_5,"
      "byte2_6, byte2_7, byte2_8, byte2_9, byte2_10,"
      "msc_location, vlr_location "
      "FROM Subscriber "
      "WHERE s_id=" +
      std::to_string(s_id) + ";";
  const auto before = clock1::now();
    DBTable::query_type = 1;
  //std::cout << "Execute: " << query << std::endl;
  if(!hustle_db->execute_query(query)) {
    return -1;
  }
  const sec duration = clock1::now() - before;
  return duration.count();
}

double txbench::TATPHustleConnector::getNewDestination(int s_id, int sf_type,
                                                     int start_time,
                                                     int end_time,
                                                     std::string *numberx) {
  std::string query =
      "SELECT numberx "
      "FROM Special_Facility, Call_Forwarding "
      "WHERE "
      "cf_s_id=sf_s_id "
      "AND (sf_s_id=" +
      std::to_string(s_id) +
      " "
     "AND sf_sf_type=" +
      std::to_string(sf_type) +
      " "
      "AND is_active=1) "
      "AND (start_time<=" +
      std::to_string(start_time) +
      " "
      "AND end_time>" +
      std::to_string(end_time) + ");";
  const auto before = clock1::now();
   // std::cout << "Execute: " << query << std::endl;
    DBTable::query_type = 2;

    if(!hustle_db->execute_query(query)) {
    return -1;
  }
  const sec duration = clock1::now() - before;
  return duration.count();
}

double txbench::TATPHustleConnector::getAccessData(int s_id, int ai_type,
                                                 int *data1, int *data2,
                                                 std::string *data3,
                                                 std::string *data4) {
  std::string query =
      "SELECT data1, data2, data3, data4 "
      "FROM Access_Info "
      "WHERE s_id=" +
      std::to_string(s_id) + " AND ai_type=" + std::to_string(ai_type) + ";";
  const auto before = clock1::now();
    //std::cout << "Execute: " << query << std::endl;
    DBTable::query_type = 3;

    if(!hustle_db->execute_query(query)) {
    return -1;
  }
  const sec duration = clock1::now() - before;
  return duration.count();
}

double txbench::TATPHustleConnector::updateSubscriberData(int s_id, bool bit_1,
                                                        int sf_type,
                                                   int data_a) {
  std::string query = "BEGIN;";
  query +=
      "UPDATE Subscriber "
      "SET bit_1=" +
      std::to_string(bit_1) +
      " "
      "WHERE s_id=" +
      std::to_string(s_id) + ";";
   query  +=
      "UPDATE Special_Facility "
      "SET data_a=" +
      std::to_string(data_a) +
      " "
      "WHERE sf_s_id=" +
      std::to_string(s_id) +
      " "
      "AND sf_sf_type=" +
      std::to_string(sf_type) + ";";
  query += "COMMIT;";
  const auto before = clock1::now();
    DBTable::query_type = 4;

    if(!hustle_db->execute_query(query)) {
    return -1;
  }
  const sec duration = clock1::now() - before;
  return duration.count();
}

double txbench::TATPHustleConnector::updateLocation(const std::string &sub_nbr,
                                                  int vlr_location) {
  std::string query =
      "UPDATE Subscriber "
      "SET vlr_location=" +
      std::to_string(vlr_location) +
      " "
      "WHERE sub_nbr='" +
      sub_nbr + "';";
  const auto before = clock1::now();
    DBTable::query_type = 5;

    if(!hustle_db->execute_query(query)) {
    return -1;
  }
  const sec duration = clock1::now() - before;
  return duration.count();
}

double txbench::TATPHustleConnector::insertCallForwarding(
    const std::string &sub_nbr, int sf_type, int start_time, int end_time,
    const std::string &numberx) {
  std::string query =
      "INSERT INTO Call_Forwarding "
      "VALUES ('" +
      sub_nbr + "', " + std::to_string(sf_type) + "," +
      std::to_string(start_time) + "," + std::to_string(end_time) + "," +
      std::to_string(end_time) + ");";
  const auto before = clock1::now();
    DBTable::query_type = 6;

    if(!hustle_db->execute_query(query)) {
    return -1;
  }
  const sec duration = clock1::now() - before;
  return duration.count();
}

double txbench::TATPHustleConnector::deleteCallForwarding(int s_id,
                                                          const std::string &sub_nbr, int sf_type,
                                                          int start_time) {
    std::string query = "BEGIN;";
  //  query +="SELECT s_id FROM Subscriber "
  //          "WHERE sub_nbr = '" + sub_nbr + "';";
    query +=
      "DELETE FROM Call_Forwarding "
      "WHERE cf_s_id=" + std::to_string(s_id) + " "
      "AND cf_sf_type=" +
      std::to_string(sf_type) +
      ""
      " AND start_time=" +
      std::to_string(start_time) + ";";
    query += "COMMIT;";

  const auto before = clock1::now();
    DBTable::query_type = 7;

    if(!hustle_db->execute_query(query)) {
    return -1;
  }
  const sec duration = clock1::now() - before;
  return duration.count();
}


