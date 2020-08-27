//
// Created by Nicholas Corrado on 7/25/20.
//

#ifndef HUSTLE_HISTOGRAM_H
#define HUSTLE_HISTOGRAM_H

#include <algorithm>
#include <cstdint>
#include <cstdlib>

class Histogram {
 public:
  explicit Histogram(int num_bins, int min, int max) {
    num_bins_ = num_bins;
    min_ = min;
    max_ = max;
    range_ = max - min;

    counts_ = (int*)malloc(sizeof(int) * num_bins_);
    cumulative_ = (int*)malloc(sizeof(int) * num_bins_);
    for (int i = 0; i < num_bins; ++i) {
      counts_[i] = 0;
      cumulative_[i] = 0;
    }

    num_values_ = 0;

    q_ = 75;
    r_ = ((double)q_) / 100;
  }

  void insert(double value) {
    auto bin = find_bin(value);
    counts_[bin]++;
    //        for (int i=0; i<bin; ++i){
    //            cumulative_[i]++;
    //        }
    ++num_values_;
  }

  double get_quantile(int q) {
    // bin corresponding to quantile q
    //        auto q_bin = (std::upper_bound(cumulative_, cumulative_ +
    //        num_bins_, ((double) q)/100 * num_values_) - cumulative_);
    int q_bin = -1;
    double cut = (1 - r_) * num_values_;
    // Scan from right to left to find the rightmost element larger than cut.
    // Note that cumulative_ is always reverse sorted.
    //        for (int i=num_bins_-1; i>=0; --i) {
    //            if (cumulative_[i] > cut) {
    //                q_bin = i;
    //                break;
    //            }
    //        }
    int sum = 0;
    for (int i = num_bins_ - 1; i >= 0; --i) {
      sum += counts_[i];
      if (sum > cut) {
        q_bin = i + 1;
        break;
      }
    }
    // return the fraction of values in the histogram in bins [0, bin_q]
    // inclusive. rank q selectivity estimate.
    auto y = (((double)(q_bin)) / num_bins_);
    return (((double)(q_bin)) / num_bins_);
  }

 private:
  int find_bin(double value) {
    // - 1 because bins are indexed from 0
    auto bin = (value / range_) * num_bins_ - 1;
    if (bin < 0) bin = 0;
    return bin;
  }

  int num_bins_;
  int min_;
  int max_;
  double range_;

  int* counts_;
  int* cumulative_;
  int num_values_;

  int q_;
  double r_;
};

#endif  // HUSTLE_HISTOGRAM_H
