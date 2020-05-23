#ifndef HUSTLE_BLOOMFILTER_H
#define HUSTLE_BLOOMFILTER_H

#include<stdlib.h>
#include <vector>
#include <arrow/api.h>

class BloomFilter {
public:
    
//    explicit BloomFilter(int num_vals);
    explicit BloomFilter(std::shared_ptr<arrow::ChunkedArray> col);
    void insert(std::shared_ptr<arrow::ChunkedArray> col);
    bool probe(long long val);
    double get_hit_rate();
    void set_memory(int memory);
    void update();


private:

    double eps_;
    
    int num_cells_;
    int num_hash_;
    int* seeds_;
    std::vector<bool> cells_;

    // LIP-specific data members
    int* hit_count_queue_;
    int* probe_count_queue_;
    int queue_index_;

    int hit_count_;
    int probe_count_;
    int hit_count_queue_sum_;
    int probe_count_queue_sum_;

    int memory_;



    static bool compare(BloomFilter *lhs, BloomFilter *rhs);

    unsigned int hash(long long val, int seed);
//    unsigned int hash(const std::string& x, int seed);

    void Reset();
};


#endif //HUSTLE_BLOOMFILTER_H
