#include "tatp_mysql_connector.h"
#include <iostream>

txbench::TATPMySQLConnector::TATPMySQLConnector(const std::string &host,
                                                int port,
                                                const std::string &user)
    : session_(host, port, user),

      select_subscriber_data_(session_.sql("SELECT * "
                                           "FROM tatp.Subscriber "
                                           "WHERE s_id = ?;")),

      select_call_forwarding_number_(
          session_.sql("SELECT cf.numberx "
                       "FROM tatp.Special_Facility sf, tatp.Call_Forwarding cf "
                       "WHERE sf.s_id = ? "
                       "AND sf.sf_type = ? "
                       "AND sf.is_active = 1 "
                       "AND cf.s_id = sf.s_id "
                       "AND cf.sf_type = sf.sf_type "
                       "AND cf.start_time <= ? "
                       "AND cf.end_time > ?;")),

      select_access_data_(session_.sql("SELECT data1, data2, data3, data4 "
                                       "FROM tatp.Access_Info "
                                       "WHERE s_id = ? AND ai_type = ?;")),

      update_subscriber_bit_(session_.sql("UPDATE tatp.Subscriber "
                                          "SET bit_1 = ? "
                                          "WHERE s_id = ?;")),

      update_special_facility_data_(session_.sql("UPDATE tatp.Special_Facility "
                                                 "SET data_a = ? "
                                                 "WHERE s_id = ? "
                                                 "AND sf_type = ?;")),

      update_subscriber_location_(session_.sql("UPDATE tatp.Subscriber "
                                               "SET vlr_location = ? "
                                               "WHERE s_id = ?;")),

      select_subscriber_id_(session_.sql("SELECT s_id "
                                         "FROM tatp.Subscriber "
                                         "WHERE sub_nbr = ?;")),

      select_special_facility_type_(session_.sql("SELECT sf_type "
                                                 "FROM tatp.Special_Facility "
                                                 "WHERE s_id = ?;")),

      insert_into_call_forwarding_(
          session_.sql("INSERT INTO tatp.Call_Forwarding "
                       "VALUES (?, ?, ?, ?, ?);")),

      delete_from_call_forwarding_(
          session_.sql("DELETE FROM tatp.Call_Forwarding "
                       "WHERE s_id = ? "
                       "AND sf_type = ? "
                       "AND start_time = ?;")) {}

void txbench::TATPMySQLConnector::getSubscriberData(
    int s_id, std::string *sub_nbr, std::array<bool, 10> &bit,
    std::array<int, 10> &hex, std::array<int, 10> &byte2, int *msc_location,
    int *vlr_location) {
  session_.startTransaction();

  mysqlx::SqlResult result = select_subscriber_data_.bind(s_id).execute();
  mysqlx::Row row = result.fetchOne();

  *sub_nbr = row[1].get<std::string>();

  for (int i = 0; i < 10; ++i) {
    bit[i] = row[2 + i].get<bool>();
  }

  for (int i = 0; i < 10; ++i) {
    hex[i] = row[12 + i].get<int>();
  }

  for (int i = 0; i < 10; ++i) {
    byte2[i] = row[22 + i].get<int>();
  }

  *msc_location = row[32].get<int>();
  *vlr_location = row[33].get<int>();

  session_.commit();
}

void txbench::TATPMySQLConnector::getNewDestination(int s_id, int sf_type,
                                                    int start_time,
                                                    int end_time,
                                                    std::string *numberx) {
  session_.startTransaction();

  mysqlx::SqlResult result =
      select_call_forwarding_number_.bind(s_id, sf_type, start_time, end_time)
          .execute();

  if (mysqlx::Row row = result.fetchOne()) {
    *numberx = row[0].get<std::string>();
  }

  session_.commit();
}

void txbench::TATPMySQLConnector::getAccessData(int s_id, int ai_type,
                                                int *data1, int *data2,
                                                std::string *data3,
                                                std::string *data4) {
  session_.startTransaction();

  mysqlx::SqlResult result = select_access_data_.bind(s_id, ai_type).execute();

  if (mysqlx::Row row = result.fetchOne()) {
    *data1 = row[0].get<int>();
    *data2 = row[1].get<int>();
    *data3 = row[2].get<std::string>();
    *data4 = row[3].get<std::string>();
  }

  session_.commit();
}

void txbench::TATPMySQLConnector::updateSubscriberData(int s_id, bool bit_1,
                                                       int sf_type,
                                                       int data_a) {
  session_.startTransaction();

  update_subscriber_bit_.bind(bit_1, s_id).execute();
  update_special_facility_data_.bind(data_a, s_id, sf_type).execute();

  session_.commit();
}

void txbench::TATPMySQLConnector::updateLocation(const std::string &sub_nbr,
                                                 int vlr_location) {
  session_.startTransaction();

  update_subscriber_location_.bind(vlr_location, sub_nbr).execute();

  session_.commit();
}

void txbench::TATPMySQLConnector::insertCallForwarding(
    const std::string &sub_nbr, int sf_type, int start_time, int end_time,
    const std::string &numberx) {
  session_.startTransaction();

  int s_id =
      select_subscriber_id_.bind(sub_nbr).execute().fetchOne()[0].get<int>();
  select_special_facility_type_.bind(s_id).execute();

  try {
    insert_into_call_forwarding_
        .bind(s_id, sf_type, start_time, end_time, numberx)
        .execute();

    session_.commit();
  } catch (mysqlx::Error &e) {
    session_.rollback();
  }
}

void txbench::TATPMySQLConnector::deleteCallForwarding(
    const std::string &sub_nbr, int sf_type, int start_time) {
  session_.startTransaction();

  int s_id =
      select_subscriber_id_.bind(sub_nbr).execute().fetchOne()[0].get<int>();
  delete_from_call_forwarding_.bind(s_id, sf_type, start_time).execute();

  session_.commit();
}
