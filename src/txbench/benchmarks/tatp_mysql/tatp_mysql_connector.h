#ifndef TXBENCH__TATP_MYSQL_CONNECTOR_H
#define TXBENCH__TATP_MYSQL_CONNECTOR_H

#include "benchmarks/tatp/tatp_connector.h"
#include <mysqlx/xdevapi.h>

namespace txbench {

class TATPMySQLConnector : public TATPConnector {
public:
  TATPMySQLConnector(const std::string &host, int port,
                     const std::string &user);

  void getSubscriberData(int s_id, std::string *sub_nbr,
                         std::array<bool, 10> &bit, std::array<int, 10> &hex,
                         std::array<int, 10> &byte2, int *msc_location,
                         int *vlr_location) override;

  void getNewDestination(int s_id, int sf_type, int start_time, int end_time,
                         std::string *numberx) override;

  void getAccessData(int s_id, int ai_type, int *data1, int *data2,
                     std::string *data3, std::string *data4) override;

  void updateSubscriberData(int s_id, bool bit_1, int sf_type,
                            int data_a) override;

  void updateLocation(const std::string &sub_nbr, int vlr_location) override;

  void insertCallForwarding(const std::string &sub_nbr, int sf_type,
                            int start_time, int end_time,
                            const std::string &numberx) override;

  void deleteCallForwarding(const std::string &sub_nbr, int sf_type,
                            int start_time) override;

private:
  mysqlx::Session session_;

  mysqlx::SqlStatement select_subscriber_data_;
  mysqlx::SqlStatement select_call_forwarding_number_;
  mysqlx::SqlStatement select_access_data_;
  mysqlx::SqlStatement update_subscriber_bit_;
  mysqlx::SqlStatement update_special_facility_data_;
  mysqlx::SqlStatement update_subscriber_location_;
  mysqlx::SqlStatement select_subscriber_id_;
  mysqlx::SqlStatement select_special_facility_type_;
  mysqlx::SqlStatement insert_into_call_forwarding_;
  mysqlx::SqlStatement delete_from_call_forwarding_;
};

} // namespace txbench

#endif // TXBENCH__TATP_MYSQL_CONNECTOR_H
