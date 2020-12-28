#include "txbench/benchmarks/tatp_hustle/tatp_hustle_connector.h"

#include <iostream>
#include <memory>

txbench::TATPHustleConnector::TATPHustleConnector(const std::string &host,
                                                  int port,
                                                  const std::string &user) {
  hustle_db = txbench::TATPHustleBenchmark::getHustleDB();
}

void txbench::TATPHustleConnector::getSubscriberData(
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
   hustle_db->executeQuery(query);
   std::cout << "query done - select - 1" << std::endl;
}

void txbench::TATPHustleConnector::getNewDestination(int s_id, int sf_type,
                                                     int start_time,
                                                     int end_time,
                                                     std::string *numberx) {
  std::string query =
      "SELECT numberx "
      "FROM Special_Facility, Call_Forwarding "
      "WHERE "
      "cf_s_id=sf_s_id "
      "AND cf_sf_type=sf_sf_type "
      "AND sf_s_id=" +
      std::to_string(s_id) +
      " "
      "AND sf_sf_type=" +
      std::to_string(sf_type) +
      " "
      "AND is_active=1 "
      "AND start_time<=" +
      std::to_string(start_time) +
      " "
      "AND end_time>" +
      std::to_string(end_time) + ";";
     //hustle_db->executeQuery(query);
}

void txbench::TATPHustleConnector::getAccessData(int s_id, int ai_type,
                                                 int *data1, int *data2,
                                                 std::string *data3,
                                                 std::string *data4) {
  std::string query =
      "SELECT data1, data2, data3, data4 "
      "FROM Access_Info "
      "WHERE s_id=" +
      std::to_string(s_id) + " AND ai_type=" + std::to_string(ai_type) + ";";
   hustle_db->executeQuery(query);
  std::cout << "query done - select - 3" << std::endl;
}

void txbench::TATPHustleConnector::updateSubscriberData(int s_id, bool bit_1,
                                                        int sf_type,
                                                        int data_a) {
  std::string query1 =
      "UPDATE Subscriber "
      "SET bit_1=" +
      std::to_string(bit_1) +
      " "
      "WHERE s_id=" +
      std::to_string(s_id) + ";";
   hustle_db->executeQuery(query1);
  std::string query2 =
      "UPDATE Special_Facility "
      "SET data_a=" +
      std::to_string(data_a) +
      " "
      "WHERE sf_s_id=" +
      std::to_string(s_id) +
      " "
      "AND sf_type=" +
      std::to_string(sf_type) + ";";
       hustle_db->executeQuery(query2);
       std::cout << "query done - update - 4" << std::endl;
}

void txbench::TATPHustleConnector::updateLocation(const std::string &sub_nbr,
                                                  int vlr_location) {
  std::string query =
      "UPDATE Subscriber "
      "SET vlr_location=" +
      std::to_string(vlr_location) +
      " "
      "WHERE sub_nbr='" +
      sub_nbr + "';";
   hustle_db->executeQuery(query);
   std::cout << "query done - update" << std::endl;
}

void txbench::TATPHustleConnector::insertCallForwarding(
    const std::string &sub_nbr, int sf_type, int start_time, int end_time,
    const std::string &numberx) {
  std::string query =
      "INSERT INTO Call_Forwarding "
      "VALUES ('" +
      sub_nbr + "', " + std::to_string(sf_type) + "," +
      std::to_string(start_time) + "," + std::to_string(end_time) + "," +
      std::to_string(end_time) + ");";
   hustle_db->executeQuery(query);
   std::cout << "query done - insert" << std::endl;
}

void txbench::TATPHustleConnector::deleteCallForwarding(
    const std::string &sub_nbr, int sf_type, int start_time) {
  std::string query =
      "DELETE FROM Call_Forwarding "
      "WHERE cf_s_id=10 "
      "AND cf_sf_type=" +
      std::to_string(sf_type) +
      ""
      " AND start_time=" +
      std::to_string(start_time) + ";";
   hustle_db->executeQuery(query);
   std::cout << "query done - delete" << std::endl;
}
