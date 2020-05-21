// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <memory>
#include <vector>

#include "bitvector.h"
#include "bitvector_block.h"
#include "bitvector_iterator.h"
#include "status.h"
#include "bitwise.h"
#include "utility.h"

namespace hustle::bitweaving {

/**
 * @brief Structure for private members of Class BitVector.
 */
struct BitVector::Rep {
  /**
   * @brief The number of bits in this bit-vector.
   */
  size_t num;
  /**
   * @brief A list of bit-vector blocks.
   */
  std::vector<BitVectorBlock *> vector;
};

BitVector::BitVector(size_t num)
{
  rep_ = new Rep();
  rep_->num = num;
  for (size_t i = 0; i < num; i += NUM_CODES_PER_BLOCK) {
    size_t size = std::min(num - i, NUM_CODES_PER_BLOCK);
    rep_->vector.push_back(new BitVectorBlock(size));
  }
}

BitVector::~BitVector()
{
  for (uint32_t i = 0; i < rep_->vector.size(); i++) {
    delete rep_->vector[i];
  }
  delete rep_;
}

void BitVector::operator=(const BitVector & bit_vector)
{
  for (uint32_t i = 0; i < rep_->vector.size(); i++) {
    *rep_->vector[i] = *bit_vector.rep_->vector[i];
  }
}

bool BitVector::operator==(const BitVector & bit_vector) const
{
  for (uint32_t i = 0; i < rep_->vector.size(); i++) {
    if (!(*rep_->vector[i] == *bit_vector.rep_->vector[i]))
      return false;
  }
  return true;
}

size_t BitVector::GetNum() const
{
  return rep_->num;
}

size_t BitVector::GetNumBitVectorBlock() const
{
  return rep_->vector.size();
}

BitVectorBlock & BitVector::GetBitVectorBlock(size_t block_id) const
{
  assert(block_id < rep_->vector.size());
  return *rep_->vector[block_id];
}

void BitVector::SetZeros()
{
  for (uint32_t i = 0; i < rep_->vector.size(); i++) {
    rep_->vector[i]->SetZeros();
  }
}

void BitVector::SetOnes()
{
  for (uint32_t i = 0; i < rep_->vector.size(); i++) {
    rep_->vector[i]->SetOnes();
  }
}

size_t BitVector::Count() const
{
  size_t count = 0;
  for (uint32_t i = 0; i < rep_->vector.size(); i++) {
    count += rep_->vector[i]->Count();
  }
  return count;
}

void BitVector::Complement()
{
  for (uint32_t i = 0; i < rep_->vector.size(); i++) {
    rep_->vector[i]->Complement();
  }
}

Status BitVector::And(const BitVector & bit_vector)
{
  for (uint32_t i = 0; i < rep_->vector.size(); i++) {
    rep_->vector[i]->And(*bit_vector.rep_->vector[i]);
  }
  return Status::OK();
}

Status BitVector::Or(const BitVector & bit_vector)
{
  for (uint32_t i = 0; i < rep_->vector.size(); i++) {
    rep_->vector[i]->Or(*bit_vector.rep_->vector[i]);
  }
  return Status::OK();
}

std::string BitVector::ToString() const
{
  std::string str;
  for (uint32_t i = 0; i < rep_->vector.size(); i++) {
    if (i != 0)
      str += "-";
    str += rep_->vector[i]->ToString();
  }
  return str;
}




}// namespace bitweaving
