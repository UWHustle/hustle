#ifndef HUSTLE_BLOOMFILTER_H
#define HUSTLE_BLOOMFILTER_H

#include <cstdlib>
#include <vector>
#include <arrow/api.h>
#include "EventProfiler.hpp"


class BloomFilter {
public:

    /**
     * Construct a Bloom filter
     *
     * @param num_vals number of values we will insert into the Bloom filter
     */
    explicit BloomFilter(int num_vals);

    /**
     * Insert a value into the Bloom filter
     *
     * @param val Value to be inserted
     */
    void insert(long long val);

    /**
     * Probe the Bloom filter with a value
     *
     * @param val probe value
     * @return true if the probe was successful (i.e. val is likely in the filter),
     * false otherwise.
     */
    bool probe(long long val);
    void probe(const uint64_t *vals, uint64_t num_vals, std::vector<uint64_t> &out, uint64_t offset, bool store_val);
    void probe(const uint64_t *vals, std::vector<uint64_t> &indices, uint64_t num_vals, std::vector<uint64_t> &out,
               uint64_t offset, bool store_val);
    /**
     * Return the ratio of the total of hits to the total number of probes
     * (from the memory_ previous batches).
     *
     * @return hit to probe ratio
     */
    double get_hit_rate();

    /**
     * Set the number of batches the Bloom filter should "remember" while keeping
     * hit/probe statistics.
     *
     * @param memory Number of batches to remember.
     */
    void set_memory(int memory);

    /**
     * Update the filter's queues and counts
     */
    void update();

    /**
     * Compare the filter rates of two filters.
     *
     * @param lhs A Bloom filter
     * @param rhs another Bloom filter
     * @return true if the filter rate of lhs is smaller than that of rhs, false
     * otherwise.
     */
    static bool compare(std::shared_ptr<BloomFilter> lhs, std::shared_ptr<BloomFilter> rhs);

    /**
     * Set the name of the foreign key column associated with this filter.
     *
     * @param fk_name foreign key column name
     */
    void set_fact_fk_name(std::string fk_name);

    /**
     * Get the name of the foreign key column associated with this filter.
     *
     * @return foreign key column name
     */
    std::string get_fact_fk_name();

    // Number of bits in the Bloom filter
    int num_cells_;
    // Number of hash functions
    int num_hash_;
    // Hash function seeds
    int* seeds_;
    // The Bloom filter
    uint8_t *cells_;
private:

    // Foreign key column name associated with the filter
    std::string fk_name_;
    // False positive rate
    double eps_;

//    // Number of bits in the Bloom filter
//    int num_cells_;
//    // Number of hash functions
//    int num_hash_;
//    // Hash function seeds
//    int* seeds_;
//    // The Bloom filter
//    uint8_t *cells_;

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
    unsigned int hash(long long val, int seed);

    void Reset();



};


#endif //HUSTLE_BLOOMFILTER_H
