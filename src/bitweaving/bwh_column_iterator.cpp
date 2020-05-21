// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <cassert>

#include "column.h"
#include "bwh_column_block.h"
#include "bwh_column_iterator.h"

namespace hustle::bitweaving {

template <size_t CODE_SIZE>
BwHColumnIterator<CODE_SIZE>::BwHColumnIterator(Column * const column)
  : ColumnIterator(column)
{
  assert(column_->GetType() == kBitWeavingH);

  last_tuple_id_ = 0;
  segment_word_id_ = 0;
  code_id_in_segment_ = 0;
  code_id_in_block_ = 0;
  column_block_id_ = 0;
  column_block_ = static_cast<BwHColumnBlock<CODE_SIZE> *>(
      column_->GetColumnBlock(column_block_id_));

  // Precomputing data
  for (size_t i = 0; i < NUM_CODES_PER_SEGMENT; i++) {
    pre_data_[i].word_id_in_segment = i % NUM_WORDS_PER_SEGMENT;
    pre_data_[i].shift_in_word = (NUM_CODES_PER_WORD - 1 -
        i / NUM_WORDS_PER_SEGMENT) * NUM_BITS_PER_CODE;
  }
}

template <size_t CODE_SIZE>
BwHColumnIterator<CODE_SIZE>::~BwHColumnIterator()
{
}

template <size_t CODE_SIZE>
void BwHColumnIterator<CODE_SIZE>::Seek(TupleId tuple_id)
{
  size_t delta = tuple_id - last_tuple_id_;
  last_tuple_id_ = tuple_id;
  code_id_in_block_ += delta;
  if (code_id_in_block_ < NUM_CODES_PER_BLOCK) {
    if (delta + code_id_in_segment_ < NUM_CODES_PER_SEGMENT) {
      // Move within the same segment
      code_id_in_segment_ += delta;
    } else if (delta + code_id_in_segment_ < NUM_CODES_PER_SEGMENT * 2) {
      // Move to the next segment
      segment_word_id_ += NUM_WORDS_PER_SEGMENT;
      code_id_in_segment_ += delta - NUM_CODES_PER_SEGMENT;
    } else if (delta + code_id_in_segment_ < NUM_CODES_PER_SEGMENT * 3){
      // Move to the next next segment
      segment_word_id_ += NUM_WORDS_PER_SEGMENT * 2;
      code_id_in_segment_ += delta - NUM_CODES_PER_SEGMENT * 2;
    } else {
      segment_word_id_ = (code_id_in_block_ / NUM_CODES_PER_SEGMENT) * NUM_WORDS_PER_SEGMENT;
      code_id_in_segment_ = code_id_in_block_ % NUM_CODES_PER_SEGMENT;
    }
  } else {
    // Move to the next block
    size_t num_blocks = code_id_in_block_ / NUM_CODES_PER_BLOCK;
    column_block_id_ += num_blocks;
    column_block_ = static_cast<BwHColumnBlock<CODE_SIZE> *>(
        column_->GetColumnBlock(column_block_id_));

    code_id_in_block_ = code_id_in_block_ % NUM_CODES_PER_BLOCK;
    segment_word_id_ = (code_id_in_block_ / NUM_CODES_PER_SEGMENT) * NUM_WORDS_PER_SEGMENT;
    code_id_in_segment_ = code_id_in_block_ % NUM_CODES_PER_SEGMENT;
  }
}

template <size_t CODE_SIZE>
Status BwHColumnIterator<CODE_SIZE>::GetCode(TupleId tuple_id, Code & code)
{
  Seek(tuple_id);

  size_t word_id = segment_word_id_ + pre_data_[code_id_in_segment_].word_id_in_segment;
  code = (column_block_->data_[word_id] >> pre_data_[code_id_in_segment_].shift_in_word) & CODE_MASK;
  return Status::OK();
}

template <size_t CODE_SIZE>
Status BwHColumnIterator<CODE_SIZE>::SetCode(TupleId tuple_id, Code code)
{
  Seek(tuple_id);

  size_t word_id = segment_word_id_ + pre_data_[code_id_in_segment_].word_id_in_segment;
  column_block_->data_[word_id] &= ~(CODE_MASK << pre_data_[code_id_in_segment_].shift_in_word);
  column_block_->data_[word_id] |= (WordUnit)code << pre_data_[code_id_in_segment_].shift_in_word;
  return Status::OK();
}

//explicit instantiations
template class BwHColumnIterator<1>;
template class BwHColumnIterator<2>;
template class BwHColumnIterator<3>;
template class BwHColumnIterator<4>;
template class BwHColumnIterator<5>;
template class BwHColumnIterator<6>;
template class BwHColumnIterator<7>;
template class BwHColumnIterator<8>;
template class BwHColumnIterator<9>;
template class BwHColumnIterator<10>;
template class BwHColumnIterator<11>;
template class BwHColumnIterator<12>;
template class BwHColumnIterator<13>;
template class BwHColumnIterator<14>;
template class BwHColumnIterator<15>;
template class BwHColumnIterator<16>;
template class BwHColumnIterator<17>;
template class BwHColumnIterator<18>;
template class BwHColumnIterator<19>;
template class BwHColumnIterator<20>;
template class BwHColumnIterator<21>;
template class BwHColumnIterator<22>;
template class BwHColumnIterator<23>;
template class BwHColumnIterator<24>;
template class BwHColumnIterator<25>;
template class BwHColumnIterator<26>;
template class BwHColumnIterator<27>;
template class BwHColumnIterator<28>;
template class BwHColumnIterator<29>;
template class BwHColumnIterator<30>;
template class BwHColumnIterator<31>;
template class BwHColumnIterator<32>;

} // namespace bitweaving


