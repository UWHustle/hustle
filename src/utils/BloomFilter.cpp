#include <cmath>
#include "BloomFilter.h"
#include "MurmurHash3.h"

#define MAX_SEED 65535

BloomFilter::BloomFilter(int num_vals) {
    eps_ = 1e-2;

    int n = num_vals;
    num_hash_ = int (round( - log(eps_) / log(2)));
    num_cells_ = int (n * num_hash_ / log(2));
    num_bytes_ = sizeof(uint8_t) * num_cells_ / 8 + 1;

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

//    insert(col);
}

//BloomFilter::BloomFilter(int num_vals) {
//    int n = num_vals;
//    num_hash_ = int (round( - log(eps_) / log(2)));
//    num_cells_ = int (n * num_hash_ / log(2));
//
//    cells_ = std::vector<bool>(num_cells_, false);
//
//    seeds_ = (int*)malloc(num_hash_ * sizeof(int));
//    for(int i = 0; i < num_hash_; ++i) {
//        seeds_[i] = rand() % MAX_SEED;
//    }
//
//    hit_count_ = 0;
//    probe_count_ = 0;
//    hit_count_queue_sum_ = 0;
//    probe_count_queue_sum_ = 0;
//}

// Note: We assume that the queues are empty when we call this function.
void BloomFilter::set_memory(int memory) {

    memory_ = memory;
    
    hit_count_queue_ = (int *) malloc(memory_*sizeof(int));
    probe_count_queue_ = (int *) malloc(memory_*sizeof(int));

    for (int i=0; i<memory_; i++) {
        hit_count_queue_[i] = 0;
        probe_count_queue_[i] = 0;
    }
    
    queue_index_ = memory_-1;
}

void BloomFilter::insert(long long val) {
    // Hash the value using all hash functions
    for (int k=0; k<num_hash_; k++) {
        int index = hash(val, seeds_[k]) % num_cells_;
        cells_[index/8] |= (1u << (index % 8u));
    }

}

void BloomFilter::update(){

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

void BloomFilter::Reset() {
    hit_count_ = 0;
    probe_count_ = 0;
    hit_count_queue_sum_ = 0;
    probe_count_queue_sum_ = 0;
}

bool BloomFilter::probe(long long val){

    probe_count_++;
    for(int i=0; i<num_hash_; i++){
        int index = hash(val, seeds_[i]) % num_cells_;
        int bit = cells_[index/8] & (1u << (index % 8u));
        if (!bit) {
            return false;
        }
    }
    hit_count_++;
    return true;
}

double BloomFilter::get_hit_rate() {
    if (probe_count_queue_sum_ > 0)
        return 1.0 * hit_count_queue_sum_ / probe_count_queue_sum_;
    else
        return 1;
}

bool BloomFilter::compare( BloomFilter *lhs,  BloomFilter *rhs) {
    return lhs->get_hit_rate() < rhs->get_hit_rate();
}


//unsigned int BloomFilter::hash(long long val, int seed) {
//    uint32_t out;
//    MurmurHash3_x86_32(&val, sizeof(long long), seed, &out);
//    return out;
//}

unsigned int BloomFilter::hash(long long x, int seed) {
    x = (x<<32)^seed;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x;
}

//	Function to hash a string, with a seed.
//	returns a hash.
//unsigned int BloomFilter::hash(const std::string& x, int seed) {
//    int len = x.length();
//
//    char* tmp = (char*)malloc(len * sizeof(char));
//    strcpy(tmp, x.c_str());
//
//    unsigned int ret;
//    MurmurHash3_x86_32 (tmp, len, seed, &ret );
//
//    free(tmp);
//
//    return ret;
//}