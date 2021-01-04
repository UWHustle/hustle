#include "tatp_worker.h"

#include <climits>
#include <iostream>
#include <memory>

#include "tatp_util.h"

txbench::TATPWorker::TATPWorker(int n_rows,
                                std::unique_ptr<TATPConnector> connector)
    : n_rows_(n_rows),
      a_val_(n_rows <= 1000000 ? 65535
                               : n_rows <= 10000000 ? 1048575 : 2097151),
      connector_(std::move(connector)) {
  w_trans_count = 0;
  query_time_ = (double *)calloc(NUM_QUERIES, sizeof(double));
  s_trans_count_ = (int *)calloc(NUM_QUERIES, sizeof(int));
}

void txbench::TATPWorker::run(std::atomic_bool &terminate,
                              std::atomic_int &commit_count) {
  while (!terminate) {
    int s_id =
        (rg_.random_int(0, a_val_) | rg_.random_int(1, n_rows_)) % n_rows_ + 1;

    int transaction_type = rg_.random_int(0, 99);
    // std::cout << "Query executing " << transaction_type << std::endl;
    if (transaction_type < 35) {
      // GET_SUBSCRIBER_DATA
      // Probability: 35%
      std::string sub_nbr;
      std::array<bool, 10> bit{};
      std::array<int, 10> hex{};
      std::array<int, 10> byte2{};
      int msc_location, vlr_location;
      double time = connector_->getSubscriberData(
          s_id, &sub_nbr, bit, hex, byte2, &msc_location, &vlr_location);
      if (time != -1) {
        query_time_[0] += time;
        s_trans_count_[0]++;
      }
    } else if (transaction_type < 45) {
      // GET_NEW_DESTINATION
      // Probability: 10%
      int sf_type = rg_.random_int(1, 4);
      int start_times_possible[] = {0, 8, 16};
      int start_time = start_times_possible[rg_.random_int(0, 2)];
      int end_time = rg_.random_int(1, 24);
      std::string numberx;
      double time = connector_->getNewDestination(s_id, sf_type, start_time,
                                                  end_time, &numberx);
      if (time != -1) {
        query_time_[1] += time;
        s_trans_count_[1]++;
      }
    } else if (transaction_type < 80) {
      // GET_ACCESS_DATA
      // Probability: 35%
      int ai_type = rg_.random_int(1, 4);
      int data1, data2;
      std::string data3, data4;
      double time = connector_->getAccessData(s_id, ai_type, &data1, &data2,
                                              &data3, &data4);
      if (time != -1) {
        query_time_[2] += time;
        s_trans_count_[2]++;
      }

    } else if (transaction_type < 82) {
      // UPDATE_SUBSCRIBER_DATA
      // Probability: 2%
      bool bit_1 = rg_.random_bool();
      int sf_type = rg_.random_int(1, 4);
      int data_a = rg_.random_int(0, 255);
      double time =
          connector_->updateSubscriberData(s_id, bit_1, sf_type, data_a);

      if (time != -1) {
        query_time_[3] += time;
        s_trans_count_[3]++;
      }
    } else if (transaction_type < 96) {
      // UPDATE_LOCATION
      // Probability: 14%
      std::string sub_nbr = leading_zero_pad(15, std::to_string(s_id));
      int vlr_location = rg_.random_int(INT_MIN, INT_MAX);
      double time = connector_->updateLocation(sub_nbr, vlr_location);

      if (time != -1) {
        query_time_[4] += time;
        s_trans_count_[4]++;
      }
    } else if (transaction_type < 98) {
      // INSERT_CALL_FORWARDING
      // Probability: 2%
      std::string sub_nbr = leading_zero_pad(15, std::to_string(s_id));
      int sf_type = rg_.random_int(1, 4);
      int start_times_possible[] = {0, 8, 16};
      int start_time = start_times_possible[rg_.random_int(0, 2)];
      int end_time = rg_.random_int(1, 24);
      std::string numberx =
          leading_zero_pad(15, std::to_string(rg_.random_int(1, n_rows_)));
      double time = connector_->insertCallForwarding(
          sub_nbr, sf_type, start_time, end_time, numberx);
      if (time != -1) {
        query_time_[5] += time;
        s_trans_count_[5]++;
      }
    } else {
      // DELETE_CALL_FORWARDING
      // Probability: 2%
      std::string sub_nbr = leading_zero_pad(15, std::to_string(s_id));
      int sf_type = rg_.random_int(1, 4);
      int start_times_possible[] = {0, 8, 16};
      int start_time = start_times_possible[rg_.random_int(0, 2)];
      double time =
          connector_->deleteCallForwarding(sub_nbr, sf_type, start_time);

      if (time != -1) {
        query_time_[6] += time;
        s_trans_count_[6]++;
      }
    }
    w_trans_count++;
    commit_count++;
    if (w_trans_count == 1000) {
      break;
    }
  }
 
  this->print(w_trans_count);
}

void txbench::TATPWorker::print(int commit_count) {
  std::cout << "Worker time " << std::endl;
  int total_s_trans_count = 0;
  for (int i = 0; i < NUM_QUERIES; i++) {
    std::cout << "Time " << (i + 1) << " : "
              << 1.0 * (query_time_[i] * 1.0 / s_trans_count_[i]) << std::endl;
    total_s_trans_count += s_trans_count_[i];
  }
  std::cout << "transaction count: " << w_trans_count
            << " successful transaction count: " << total_s_trans_count
            << std::endl;
}
