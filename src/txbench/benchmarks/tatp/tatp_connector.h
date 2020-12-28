#ifndef TXBENCH__TATP_CONNECTOR_H
#define TXBENCH__TATP_CONNECTOR_H

#include <array>
#include <string>

namespace txbench {

class TATPConnector {
public:
  virtual ~TATPConnector() = default;

  virtual void getSubscriberData(int s_id, std::string *sub_nbr,
                                 std::array<bool, 10> &bit,
                                 std::array<int, 10> &hex,
                                 std::array<int, 10> &byte2, int *msc_location,
                                 int *vlr_location) = 0;

  virtual void getNewDestination(int s_id, int sf_type, int start_time,
                                 int end_time, std::string *numberx) = 0;

  virtual void getAccessData(int s_id, int ai_type, int *data1, int *data2,
                             std::string *data3, std::string *data4) = 0;

  virtual void updateSubscriberData(int s_id, bool bit_1, int sf_type,
                                    int data_a) = 0;

  virtual void updateLocation(const std::string &sub_nbr, int vlr_location) = 0;

  virtual void insertCallForwarding(const std::string &sub_nbr, int sf_type,
                                    int start_time, int end_time,
                                    const std::string &numberx) = 0;

  virtual void deleteCallForwarding(const std::string &sub_nbr, int sf_type,
                                    int start_time) = 0;

};

} // namespace txbench

#endif // TXBENCH__TATP_CONNECTOR_H
