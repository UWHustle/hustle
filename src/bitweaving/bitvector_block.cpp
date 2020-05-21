// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <memory.h>
#include <bitset>
#include <iostream>

#include "types.h"
#include "bitvector_block.h"
#include "status.h"
#include "bitwise.h"
#include "utility.h"

namespace hustle::bitweaving {

BitVectorBlock::BitVectorBlock() :
  num_(0)
{
  num_word_units_ = bceil(num_, NUM_BITS_PER_WORD);
  data_ = new WordUnit[bceil(NUM_CODES_PER_BLOCK, NUM_BITS_PER_WORD)];
}

BitVectorBlock::BitVectorBlock(size_t num) :
  num_(num)
{
  num_word_units_ = bceil(num_, NUM_BITS_PER_WORD);
  data_ = new WordUnit[bceil(NUM_CODES_PER_BLOCK, NUM_BITS_PER_WORD)];
}

BitVectorBlock::~BitVectorBlock()
{
  delete [] data_;
}

void BitVectorBlock::operator=(const BitVectorBlock & v)
{
  if (num_word_units_ < v.num_word_units_) {
    delete [] data_;
    data_ = new WordUnit[v.num_word_units_];
  }
  num_ = v.num_;
  num_word_units_ = v.num_word_units_;
  memcpy(data_, v.data_, sizeof(WordUnit) * num_word_units_);
}

bool BitVectorBlock::operator==(const BitVectorBlock & v)
{
  if (num_ != v.num_)
    return false;
  for (size_t i = 0; i < num_word_units_; i++) {
    if (data_[i] != v.data_[i])
      return false;
  }
  return true;
}

void BitVectorBlock::SetZeros()
{
  memset(data_, 0, sizeof(WordUnit) * num_word_units_);
  Finalize();
}

void BitVectorBlock::SetOnes()
{
  uint8_t mask = -1;
  memset(data_, mask, sizeof(WordUnit) * num_word_units_);
  Finalize();
}

size_t BitVectorBlock::Count()
{
  size_t count = 0;
  for (size_t i = 0; i < num_word_units_; i++) {
    count += Popcnt(data_[i]);
  }
  return count;
}

void BitVectorBlock::Complement()
{
  for (size_t i = 0; i < num_word_units_; i++) {
    data_[i] = ~data_[i];
  }
  Finalize();
}

Status BitVectorBlock::And(const BitVectorBlock & v)
{
  if (v.num_ != num_) {
    return Status::InvalidArgument("Logical AND: BitVector length does not match.");
  }
  for (size_t i = 0; i < num_word_units_; i++) {
    data_[i] = data_[i] & v.data_[i];
  }
  Finalize();
  return Status::OK();
}

Status BitVectorBlock::Or(const BitVectorBlock & v)
{
  if (v.num_ != num_) {
    return Status::InvalidArgument("Logical OR: BitVector length does not match.");
  }
  for (size_t i = 0; i < num_word_units_; i++) {
    data_[i] = data_[i] | v.data_[i];
  }
  Finalize();
  return Status::OK();
}

void BitVectorBlock::Finalize()
{
  size_t word = num_ / NUM_BITS_PER_WORD;
  size_t offset = NUM_BITS_PER_WORD - (num_ % NUM_BITS_PER_WORD);
  if (offset != NUM_BITS_PER_WORD)
    data_[word] &= (-1ULL << offset);
}

std::string BitVectorBlock::ToString()
{
  std::string str;
  for (size_t i = 0; i < num_word_units_; i++) {
    std::bitset<64> bitset(data_[i]);
    str.append(bitset.to_string());
//    for( size_t j=0; j<64; j++){
//        if((i*64 + j) < num_){
//            std::bitset<1> bitset(BitVectorBlock::GetBit(j));
//            str.append(bitset.to_string());
//        }
//    }
    str.append(" ");
  }
  return str;
}

} // namespace bitweaving
