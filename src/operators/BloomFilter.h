#ifndef HUSTLE_BLOOMFILTER_H
#define HUSTLE_BLOOMFILTER_H

#include <cstdlib>
#include <vector>
#include <arrow/api.h>
#include "EventProfiler.hpp"

class BloomFilter {
public:
    
    explicit BloomFilter(int num_vals);
    void insert(long long val);
    bool probe(long long val);
    double get_hit_rate();
    void set_memory(int memory);
    void update();
    static bool compare(std::shared_ptr<BloomFilter> lhs, std::shared_ptr<BloomFilter> rhs);
    void set_fact_fk_name(std::string fk_name);
    std::string get_fact_fk_name();


private:

    std::string fk_name_;
    double eps_;
    
    int num_cells_;
    int num_bytes_;
    int num_hash_;
    int* seeds_;
    uint8_t *cells_;

    // LIP-specific data members
    int* hit_count_queue_;
    int* probe_count_queue_;
    int queue_index_;

    int hit_count_;
    int probe_count_;
    int hit_count_queue_sum_;
    int probe_count_queue_sum_;

    int memory_;



    unsigned int hash(long long val, int seed);
//    unsigned int hash(const std::string& x, int seed);

    void Reset();


};


#endif //HUSTLE_BLOOMFILTER_H
