// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <cassert>
#include <iostream>
#include <arrow/array.h>

#include "bitvector_block.h"
#include "bitvector_iterator.h"
#include "bitwise.h"

namespace hustle::bitweaving {

    BitVectorIterator::BitVectorIterator(const BitVector &bitvector)
            : bitvector_(bitvector) {
        Begin();
    }

    BitVectorIterator::~BitVectorIterator() {

    }

    void BitVectorIterator::Begin() {
        num_word_units_ = 0;
        block_id_ = 0;
        block_offset_ = 0;
        word_unit_pos_ = 0;
        pos_ = 0;
        stack_size_ = 0;
        cur_block_ = NULL;
        curr_bit_pos_ = 0;
    }

    bool BitVectorIterator::Next() {
        if (stack_size_ == 0) {
            // Stack is empty, load next word unit.
            WordUnit word = 0;
            // Scan to next non-zero word
            while (word == 0) {
                if (word_unit_pos_ >= num_word_units_) {
                    if (block_id_ >= bitvector_.GetNumBitVectorBlock())
                        return false;
                    if (cur_block_ != NULL)
                        block_offset_ += cur_block_->GetNum();
                    // cur_block_ = bitvector_.rep_->vector[block_id_++];
                    cur_block_ = &bitvector_.GetBitVectorBlock(block_id_++);
                    num_word_units_ = cur_block_->GetNumWordUnits();
                    word_unit_pos_ = 0;
                }
                word = cur_block_->GetWordUnit(word_unit_pos_++);
            }
            // Extract all 1-bit in this word unit and push their position to stack
            size_t offset = block_offset_ +
                            (word_unit_pos_ - 1) * NUM_BITS_PER_WORD;
            while (word != 0) {
                assert(stack_size_ < NUM_BITS_PER_WORD);
                // Calculate the offset of the rightmost 1 in the word
                stack_[stack_size_++] = offset + Popcnt(RemoveSmearRightmostOne(word));
                assert(stack_[stack_size_ - 1] < bitvector_.GetNum());
                // Remove the rightmost 1
                word = RemoveRightmostOne(word);
            }
        }

        // If there are remaining positions in the stack buffer
        // Pop the position at the head of the stack
        pos_ = stack_[--stack_size_];
        return true;
    }

/**
 * Custom Next() iterator function to retrieve bit by bit instead of the next '1' bit
 */

//    bool BitVectorIterator::Next()
//    {
//        if (stack_size_ == 0) {
//            // Stack is empty, load next word unit.
//            WordUnit word = 0;
//            // Scan to next word
//           // while (word == 0) {
//                if (word_unit_pos_ >= num_word_units_) {
//                    if (block_id_ >= bitvector_.GetNumBitVectorBlock())
//                        return false;
//                    if (cur_block_ != NULL)
//                        block_offset_ += cur_block_->GetNum();
//                    // cur_block_ = bitvector_.rep_->vector[block_id_++];
//                    cur_block_ = &bitvector_.GetBitVectorBlock(block_id_++);
//                    num_word_units_ = cur_block_->GetNumWordUnits();
//                    word_unit_pos_ = 0;
//                }
//                word = cur_block_->GetWordUnit(word_unit_pos_++);
//           // }
//            // Extract all bits in this word unit and push their position to stack
//            size_t offset = block_offset_ +
//                            (word_unit_pos_ - 1) * NUM_BITS_PER_WORD;
//            size_t i = 0;
//            while (i < NUM_BITS_PER_WORD && stack_[stack_size_-1] < bitvector_.GetNum()-1) {
//                assert(stack_size_ < NUM_BITS_PER_WORD);
//                // Calculate the offset of each bit in the word
//                stack_[stack_size_++] = offset + i;
//                assert(stack_[stack_size_ - 1] < bitvector_.GetNum());
//                i++;
//            }
//        }
//        std::cout << "stack size: " << stack_size_ << std::endl;
//
//        // If there are remaining positions in the stack buffer
//        //pos_ = stack_[--stack_size_];
//        pos_ = stack_[curr_bit_pos_++];
//        --stack_size_;
//        return cur_block_->GetBit(pos_) == 1;
//    }

/**
 *
 * @param out - The output ArrayData to be filled with the bitmap values
 */
    void BitVectorIterator::FillDataIntoArrowFormat(std::shared_ptr<arrow::ArrayData> out) {
        auto out_bitmap = out->buffers[1]->mutable_data();
        BitVectorBlock *cur_block = NULL;
        size_t block_id = 0;
        size_t block_offset = 0;
        size_t num_word_units = 0;
        size_t word_unit_pos = 0;
        WordUnit word = 0;
        while (block_id < bitvector_.GetNumBitVectorBlock()) {
            if (cur_block != NULL)
                block_offset += cur_block->GetNum();
            cur_block = &bitvector_.GetBitVectorBlock(block_id++);
            num_word_units = cur_block->GetNumWordUnits();
            word_unit_pos = 0;
            size_t offset;
            while (word_unit_pos < num_word_units - 1) {
                word = cur_block->GetWordUnit(word_unit_pos++);
                /**
                * Reverse the word before reading from it since the bitvector has its output
                * from MSB->LSB while the expected arrow output format is from LSB->MSB
                */
                word = __builtin_bitreverse64(word);
                offset = block_offset +
                         (word_unit_pos - 1) * NUM_BITS_PER_WORD;
                assert(uint64_t(out->length) >= offset + NUM_BITS_PER_WORD);
                std::memcpy(&out_bitmap[offset / 8], &word, sizeof(WordUnit));
            }

            // When copying the last word in a block, check for the actual number of bits in this word as these may be
            // less than the NUM_BITS_PER_WORD. See finalize() method of bitvector_block.cpp for the reason.
            word = cur_block->GetWordUnit(word_unit_pos++);
            offset = block_offset +
                     (word_unit_pos - 1) * NUM_BITS_PER_WORD;

            size_t num_bits_in_last_word = cur_block->GetNum() % NUM_BITS_PER_WORD;
            if (num_bits_in_last_word != 0) {
                word = (word >> (NUM_BITS_PER_WORD - num_bits_in_last_word));
            } else {
                num_bits_in_last_word = NUM_BITS_PER_WORD; // 0 means there were actually NUM_BITS_PER_WORD in there
                word = __builtin_bitreverse64(word);
            }
            assert(uint64_t(out->length) >= offset + num_bits_in_last_word);
            std::memcpy(&out_bitmap[offset / 8], &word, bceil(num_bits_in_last_word, NUM_BITS_PER_BYTE));
        }
    }


}// namespace bitweaving


