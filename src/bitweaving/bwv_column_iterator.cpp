// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <cassert>
#include <iostream>

#include "column.h"
#include "bwv_column_block.h"
#include "bwv_column_iterator.h"

namespace hustle::bitweaving {

template <size_t CODE_SIZE>
BwVColumnIterator<CODE_SIZE>::BwVColumnIterator(Column * const column)
  : ColumnIterator(column)
{
  assert(column_->GetType() == kBitWeavingV);

  last_tuple_id_ = 0;
  group_word_id_ = 0;
  last_group_word_id_ = 0;
  code_id_in_segment_ = 0;
  code_id_in_block_ = 0;
  column_block_id_ = 0;
  column_block_ = static_cast<BwVColumnBlock<CODE_SIZE> *>(
      column_->GetColumnBlock(column_block_id_));
}

template <size_t CODE_SIZE>
BwVColumnIterator<CODE_SIZE>::~BwVColumnIterator()
{
}

template <size_t CODE_SIZE>
void BwVColumnIterator<CODE_SIZE>::Seed(TupleId tuple_id)
{
  size_t delta = tuple_id - last_tuple_id_;
  last_tuple_id_ = tuple_id;
  code_id_in_block_ += delta;
  if (code_id_in_block_ < NUM_CODES_PER_BLOCK) {
    if (delta + code_id_in_segment_ < NUM_CODES_PER_SEGMENT) {
      // Move within the same segment
      code_id_in_segment_ += delta;
    } else {
      code_id_in_segment_ = code_id_in_block_ % NUM_CODES_PER_SEGMENT;

      group_word_id_ = (code_id_in_block_ / NUM_CODES_PER_SEGMENT) * NUM_BITS_PER_GROUP;
      last_group_word_id_ = (code_id_in_block_ / NUM_CODES_PER_SEGMENT) * NUM_BITS_LAST_GROUP;
    }
  } else {
    // Move to the next block
    size_t num_blocks = code_id_in_block_ / NUM_CODES_PER_BLOCK;
    column_block_id_ += num_blocks;
    column_block_ = static_cast<BwVColumnBlock<CODE_SIZE> *>(
        column_->GetColumnBlock(column_block_id_));

    code_id_in_block_ = code_id_in_block_ % NUM_CODES_PER_BLOCK;
    code_id_in_segment_ = code_id_in_block_ % NUM_CODES_PER_SEGMENT;
    group_word_id_ = (code_id_in_block_ / NUM_CODES_PER_SEGMENT) * NUM_BITS_PER_GROUP;
    last_group_word_id_ = (code_id_in_block_ / NUM_CODES_PER_SEGMENT) * NUM_BITS_LAST_GROUP;
  }
}

template <size_t CODE_SIZE>
Status BwVColumnIterator<CODE_SIZE>::GetCode(TupleId tuple_id, Code & code)
{
  Seed(tuple_id);

  size_t offset_in_segment = NUM_CODES_PER_SEGMENT - 1 - code_id_in_segment_;
  WordUnit mask = 1ULL << offset_in_segment;
  WordUnit code_word = 0;

  size_t bit_id = 0;
  for (size_t group_id = 0; group_id < NUM_FULL_GROUPS; group_id++) {
    size_t word_id = group_word_id_;
    WordUnit * data = column_block_->data_[group_id];
    for (size_t bit_id_in_group = 0;
        bit_id_in_group < NUM_BITS_PER_GROUP;
        bit_id_in_group++) {
      code_word |= ((data[word_id] & mask) >> offset_in_segment) << (NUM_BITS_PER_CODE - 1 - bit_id);
      word_id++;
      bit_id++;
    }
  }
  size_t word_id = last_group_word_id_;
  WordUnit * data = column_block_->data_[LAST_GROUP];
  for (size_t bit_id_in_group = 0;
      bit_id_in_group < NUM_BITS_LAST_GROUP;
      bit_id_in_group++) {
    code_word |= ((data[word_id] & mask) >> offset_in_segment) << (NUM_BITS_PER_CODE - 1 - bit_id);
    word_id++;
    bit_id++;
  }
  code = (Code)code_word;
  return Status::OK();
}

template <size_t CODE_SIZE>
Status BwVColumnIterator<CODE_SIZE>::SetCode(TupleId tuple_id, Code code)
{
  Seed(tuple_id);

  size_t offset_in_segment = NUM_CODES_PER_SEGMENT - 1 - code_id_in_segment_;
  WordUnit mask = 1ULL << offset_in_segment;
  WordUnit code_word = (WordUnit)code;

  size_t bit_id = 0;
  for (size_t group_id = 0; group_id < NUM_FULL_GROUPS; group_id++) {
    size_t word_id = group_word_id_;
    WordUnit * data = column_block_->data_[group_id];
    for (size_t bit_id_in_group = 0;
        bit_id_in_group < NUM_BITS_PER_GROUP;
        bit_id_in_group++) {
      data[word_id] &= ~mask;
      data[word_id] |= ((code_word >> (NUM_BITS_PER_CODE - 1 - bit_id)) << offset_in_segment) & mask;
      word_id++;
      bit_id++;
    }
  }

  size_t word_id = last_group_word_id_;
  WordUnit * data = column_block_->data_[LAST_GROUP];
  for (size_t bit_id_in_group = 0;
      bit_id_in_group < NUM_BITS_LAST_GROUP;
      bit_id_in_group++) {
    data[word_id] &= ~mask;
    data[word_id] |= ((code_word >> (NUM_BITS_PER_CODE - 1 - bit_id)) << offset_in_segment) & mask;
    word_id++;
    bit_id++;
  }

  return column_->SetCode(tuple_id, code);
}

//explicit instantiations
template class BwVColumnIterator<1>;
template class BwVColumnIterator<2>;
template class BwVColumnIterator<3>;
template class BwVColumnIterator<4>;
template class BwVColumnIterator<5>;
template class BwVColumnIterator<6>;
template class BwVColumnIterator<7>;
template class BwVColumnIterator<8>;
template class BwVColumnIterator<9>;
template class BwVColumnIterator<10>;
template class BwVColumnIterator<11>;
template class BwVColumnIterator<12>;
template class BwVColumnIterator<13>;
template class BwVColumnIterator<14>;
template class BwVColumnIterator<15>;
template class BwVColumnIterator<16>;
template class BwVColumnIterator<17>;
template class BwVColumnIterator<18>;
template class BwVColumnIterator<19>;
template class BwVColumnIterator<20>;
template class BwVColumnIterator<21>;
template class BwVColumnIterator<22>;
template class BwVColumnIterator<23>;
template class BwVColumnIterator<24>;
template class BwVColumnIterator<25>;
template class BwVColumnIterator<26>;
template class BwVColumnIterator<27>;
template class BwVColumnIterator<28>;
template class BwVColumnIterator<29>;
template class BwVColumnIterator<30>;
template class BwVColumnIterator<31>;
template class BwVColumnIterator<32>;

} // namespace bitweaving


