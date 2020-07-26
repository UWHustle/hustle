//
// Created by Nicholas Corrado on 7/25/20.
//

#ifndef HUSTLE_HISTOGRAM_H
#define HUSTLE_HISTOGRAM_H


#include <cstdint>
#include <cstdlib>
#include <algorithm>

class Histogram {

public:

    explicit Histogram(int num_bins, int min, int max) {
        num_bins_ = num_bins;
        min_ = min;
        max_ = max;
        range_ = max - min;

//        counts_ = (int*) malloc(sizeof(int)*num_bins_);
        cumulative_ = (int*) malloc(sizeof(int)*num_bins_);
        for (int i=0; i<=num_bins; ++i) {
//            counts_[i] = 0;
            cumulative_[i] = 0;
        }

        num_values_ = 0;
    }

    void insert(double value) {
        auto bin = find_bin(value);
//        ++counts_[bin];
        for (int i=0; i<=bin; ++i){
            cumulative_[i]++;
        }
        ++num_values_;
    }

    double get_risk(int q) {
        // bin corresponding to quantile q
        auto q_bin = *(std::upper_bound(cumulative_, cumulative_ + num_bins_, ((double) q)/100 * num_values_)-1);
        // return the fraction of values in the histogram in bins [0, bin_q] inclusive.
        return (1 - ((double) q)/100)*(((double) q_bin)/num_bins_);
        return ((double) cumulative_[q_bin])/num_values_;
    }
    int* get_cumulative() {
        return cumulative_;
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

//    int* counts_;
    int* cumulative_;
    int num_values_;
};


#endif //HUSTLE_HISTOGRAM_H
