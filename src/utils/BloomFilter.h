#ifndef HUSTLE_BLOOMFILTER_H
#define HUSTLE_BLOOMFILTER_H

#include <cstdlib>
#include <utility>
#include <vector>
#include <arrow/api.h>
#include "EventProfiler.hpp"

#define MAX_SEED 65535

class BloomFilter {
public:

    /**
     * Construct a Bloom filter
     *
     * @param num_vals number of values we will insert into the Bloom filter
     */
    explicit BloomFilter(int num_vals) {
        eps_ = 1e-2;

        int n = num_vals;
        num_hash_ = int (round( - log(eps_) / log(2)));
        num_cells_ = int (n * num_hash_ / log(2));
        int num_bytes_ = sizeof(uint8_t) * num_cells_ / 8 + 1;

        num_hash_ = 3;

        cells_ = (uint8_t *) malloc(num_bytes_);

        for (int i=0; i<num_bytes_; i++) {
            cells_[i] = 0;
        }

        seeds_ = (int*)malloc(num_hash_ * sizeof(int));
        for(int i = 0; i < num_hash_; ++i) {
            seeds_[i] = rand() % MAX_SEED;
        }

        hit_count_ = 0;
        probe_count_ = 0;
        hit_count_queue_sum_ = 0;
        probe_count_queue_sum_ = 0;
    }

    /**
     * Insert a value into the Bloom filter
     *
     * @param val Value to be inserted
     */
    inline void insert(long long val) {
        // Hash the value using all hash functions
        int index;
        for (int k=0; k<num_hash_; k++) {
            index = hash(val, seeds_[k]) % num_cells_;
            cells_[index/8] |= (1u << (index % 8u));
        }
    }

    /**
     * Probe the Bloom filter with a value
     *
     * @param val probe value
     * @return true if the probe was successful (i.e. val is likely in the filter),
     * false otherwise.
     */
    inline bool probe(long long val) {
//        probe_count_++;
        uint64_t index;
        for(int i=0; i<num_hash_; i++){
            index = hash(val, seeds_[i]) % num_cells_;
            if (!(cells_[index/8] & (1u << (index % 8u)))) {
                return false;
            }
        }
//    hit_count_++;
        return true;
    }
    /**
     * Return the ratio of the total of hits to the total number of probes
     * (from the memory_ previous batches).
     *
     * @return hit to probe ratio
     */
    inline double get_hit_rate() {
        if (probe_count_queue_sum_ > 0)
            return 1.0 * hit_count_queue_sum_ / probe_count_queue_sum_;
        else
            return 1;
    }

    /**
     * Set the number of batches the Bloom filter should "remember" while keeping
     * hit/probe statistics.
     *
     * @param memory Number of batches to remember.
     */
    inline void set_memory(int memory) {
        memory_ = memory;

        hit_count_queue_ = (int *) malloc(memory_*sizeof(int));
        probe_count_queue_ = (int *) malloc(memory_*sizeof(int));

        for (int i=0; i<memory_; i++) {
            hit_count_queue_[i] = 0;
            probe_count_queue_[i] = 0;
        }

        queue_index_ = memory_-1;
    }

    /**
     * Update the filter's queues and counts
     */
    inline void update() {

        hit_count_queue_sum_  -= hit_count_queue_[queue_index_];
        probe_count_queue_sum_ -= probe_count_queue_[queue_index_];

        hit_count_queue_[queue_index_] = hit_count_;
        probe_count_queue_[queue_index_] = probe_count_;

        queue_index_--;
        if(queue_index_ < 0) queue_index_ = memory_-1;

        hit_count_queue_sum_ += hit_count_;
        probe_count_queue_sum_ += probe_count_;

        hit_count_ = 0;
        probe_count_ = 0;
    }

    /**
     * Compare the filter rates of two filters.
     *
     * @param lhs A Bloom filter
     * @param rhs another Bloom filter
     * @return true if the filter rate of lhs is smaller than that of rhs, false
     * otherwise.
     */
    static inline bool compare(const std::shared_ptr<BloomFilter>& lhs, const std::shared_ptr<BloomFilter>& rhs) {
        return lhs->get_hit_rate() < rhs->get_hit_rate();
    }

    /**
     * Set the name of the foreign key column associated with this filter.
     *
     * @param fk_name foreign key column name
     */
    inline void set_fact_fk_name(std::string fk_name) { fk_name_ = std::move(fk_name); }

    /**
     * Get the name of the foreign key column associated with this filter.
     *
     * @return foreign key column name
     */
    inline std::string get_fact_fk_name() { return fk_name_;}

private:

    // Foreign key column name associated with the filter
    std::string fk_name_;
    // False positive rate
    double eps_;
    uint64_t t;
    // Number of bits in the Bloom filter
    int num_cells_;
    // Number of hash functions
    int num_hash_;
    // Hash function seeds
    int* seeds_;
    // The Bloom filter
    uint8_t *cells_;

    // Keeps track of how many hits occured in each batch processed
    int* hit_count_queue_;
    // Keeps track of how many probes occured in each batch processed
    int* probe_count_queue_;
    // The index of the oldest queue member
    int queue_index_;

    // Number of times a probe passed in a given batch
    int hit_count_;
    // Total number of probes in a given batch
    int probe_count_;
    // Total number of times a probe passed across all batches processed
    int hit_count_queue_sum_;
    // Total number of probes across all batches processed
    int probe_count_queue_sum_;

    // The number of batches the Bloom filter "remembers"
    int memory_;

    /**
     *
     * @param val value to hash
     * @param seed random seed
     * @return a 32-bit hash value
     */
    inline unsigned int hash(unsigned long long x, int seed) {
        x = (x << 32) ^ seed;
        x = ((x >> 16) ^ x) * 0x45d9f3b;
        x = ((x >> 16) ^ x) * 0x45d9f3b;
        x = (x >> 16) ^ x;
        return x;
    }

    inline void Reset() {
        hit_count_ = 0;
        probe_count_ = 0;
        hit_count_queue_sum_ = 0;
        probe_count_queue_sum_ = 0;
    }

};


#endif //HUSTLE_BLOOMFILTER_H
