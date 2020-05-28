// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <cassert>
#include <cstring>
#include <iostream>
#include <arrow/array.h>

#include "bwv_column_block.h"

namespace hustle::bitweaving {

    template<size_t CODE_SIZE>
    BwVColumnBlock<CODE_SIZE>::BwVColumnBlock() :
            ColumnBlock(kBitWeavingV, CODE_SIZE), num_used_words_(0) {
        memset(data_, 0, sizeof(WordUnit *) * MAX_NUM_GROUPS);
        for (size_t group_id = 0; group_id < NUM_GROUPS; group_id++) {
            data_[group_id] = new WordUnit[NUM_WORDS];
            assert(data_[group_id] != NULL);
            memset(data_[group_id], 0, sizeof(WordUnit) * NUM_WORDS);
        }
        max_code_ = 0;
    }

    template<size_t CODE_SIZE>
    BwVColumnBlock<CODE_SIZE>::~BwVColumnBlock() {
        for (size_t group_id = 0; group_id < NUM_GROUPS; group_id++) {
            delete[] data_[group_id];
        }
    }

    template<size_t CODE_SIZE>
    Status BwVColumnBlock<CODE_SIZE>::GetCode(TupleId pos, Code &code) {
        if (pos >= num_)
            return Status::InvalidArgument("GetCode: position overflow");

        size_t segment_id = pos / NUM_CODES_PER_SEGMENT;
        size_t offset_in_segment = NUM_CODES_PER_SEGMENT - 1 - (pos % NUM_CODES_PER_SEGMENT);
        WordUnit mask = 1ULL << offset_in_segment;
        WordUnit code_word = 0;

        size_t bit_id = 0;
        for (size_t group_id = 0; group_id < NUM_FULL_GROUPS; group_id++) {
            size_t word_id = segment_id * NUM_WORDS_PER_SEGMENT;
            for (size_t bit_id_in_group = 0;
                 bit_id_in_group < NUM_BITS_PER_GROUP;
                 bit_id_in_group++) {
                code_word |=
                        ((data_[group_id][word_id] & mask) >> offset_in_segment) << (NUM_BITS_PER_CODE - 1 - bit_id);
                word_id++;
                bit_id++;
            }
        }

        size_t word_id = segment_id * NUM_BITS_LAST_GROUP;
        for (size_t bit_id_in_group = 0;
             bit_id_in_group < NUM_BITS_LAST_GROUP;
             bit_id_in_group++) {
            code_word |= ((data_[LAST_GROUP][word_id] & mask) >> offset_in_segment) << (NUM_BITS_PER_CODE - 1 - bit_id);
            word_id++;
            bit_id++;
        }
        code = (Code) code_word;
        return Status::OK();
    }

    template<size_t CODE_SIZE>
    Status BwVColumnBlock<CODE_SIZE>::SetCode(TupleId pos, Code code) {
        if (pos >= num_)
            return Status::InvalidArgument("GetCode: position overflow");

        size_t segment_id = pos / NUM_CODES_PER_SEGMENT;
        size_t offset_in_segment = NUM_CODES_PER_SEGMENT - 1 - (pos % NUM_CODES_PER_SEGMENT);
        WordUnit mask = 1ULL << offset_in_segment;
        WordUnit code_word = (WordUnit) code;

        size_t bit_id = 0;
        for (size_t group_id = 0; group_id < NUM_FULL_GROUPS; group_id++) {
            size_t word_id = segment_id * NUM_WORDS_PER_SEGMENT;
            for (size_t bit_id_in_group = 0;
                 bit_id_in_group < NUM_BITS_PER_GROUP;
                 bit_id_in_group++) {
                data_[group_id][word_id] &= ~mask;
                data_[group_id][word_id] |=
                        ((code_word >> (NUM_BITS_PER_CODE - 1 - bit_id)) << offset_in_segment) & mask;
                word_id++;
                bit_id++;
            }
        }

        size_t word_id = segment_id * NUM_BITS_LAST_GROUP;
        for (size_t bit_id_in_group = 0;
             bit_id_in_group < NUM_BITS_LAST_GROUP;
             bit_id_in_group++) {
            data_[LAST_GROUP][word_id] &= ~mask;
            data_[LAST_GROUP][word_id] |= ((code_word >> (NUM_BITS_PER_CODE - 1 - bit_id)) << offset_in_segment) & mask;
            word_id++;
            bit_id++;
        }
        return Status::OK();
    }

    template<size_t CODE_SIZE>
    AppendResult* BwVColumnBlock<CODE_SIZE>::Append(const Code *codes, size_t num) {

        if (num_ + num > NUM_CODES_PER_BLOCK) {
            return new AppendResult(Status::InvalidArgument("Load too many codes into a column block."),
                    false,
                    CODE_SIZE);
        }

        num_ += num;
        num_used_words_ = bceil(num_, NUM_CODES_PER_SEGMENT) * NUM_WORDS_PER_SEGMENT;

        for (size_t i = 0; i < num; i++) {
            //std::cout << codes[i] << std::endl;
            max_code_ = std::max((Code)max_code_, codes[i]);
            SetCode(i + (num_ - num), codes[i]);
        }
        //((uint64_t)1 << CODE_SIZE works as long as CODE_SIZE is less than 64. Right now, with this bitweaving library,
        // we have max CODE_SIZE as 32.
        if(max_code_ >= ((uint64_t)1 << CODE_SIZE)){
            int suggested_bitwidth = ceil(log2(max_code_)) == floor(log2(max_code_)) ?
                    ceil(log2(max_code_)) + 1 : ceil(log2(max_code_));
            //std::cout << "suggested bitwidth " << suggested_bitwidth << std::endl;
            return new AppendResult(Status::InvalidBitwidth("Invalid Bitwidth. "
                                                            "Check for the suggested value"),
                                    false,
                                    suggested_bitwidth);
        }

        return new AppendResult(Status::OK(), true, CODE_SIZE);
    }

    template<size_t CODE_SIZE>
    Status BwVColumnBlock<CODE_SIZE>::Load(SequentialReadFile &file) {
        Status status;

        status = file.Read(reinterpret_cast<char *>(&num_), sizeof(size_t));
        if (!status.IsOk())
            return status;

        status = file.Read(reinterpret_cast<char *>(&num_used_words_), sizeof(size_t));
        if (!status.IsOk())
            return status;

        for (size_t group_id = 0; group_id < NUM_GROUPS; group_id++) {
            status = file.Read(reinterpret_cast<char *>(data_[group_id]),
                               sizeof(WordUnit) * NUM_WORDS);
            if (!status.IsOk())
                return status;
        }

        return Status::OK();
    }

    template<size_t CODE_SIZE>
    Status BwVColumnBlock<CODE_SIZE>::Save(SequentialWriteFile &file) {
        Status status;

        status = file.Append(reinterpret_cast<char *>(&num_), sizeof(size_t));
        if (!status.IsOk())
            return status;

        status = file.Append(reinterpret_cast<char *>(&num_used_words_), sizeof(size_t));
        if (!status.IsOk())
            return status;

        for (size_t group_id = 0; group_id < NUM_GROUPS; group_id++) {
            status = file.Append(reinterpret_cast<char *>(data_[group_id]),
                                 sizeof(WordUnit) * NUM_WORDS);
            if (!status.IsOk())
                return status;
        }

        return Status::OK();
    }

    template<size_t CODE_SIZE>
    template<Comparator CMP, size_t GROUP_ID>
    void BwVColumnBlock<CODE_SIZE>::ScanIteration(size_t segment_offset,
                                                  WordUnit &mask_equal, WordUnit &mask_less, WordUnit &mask_greater,
                                                  WordUnit *&literal_bit_ptr) {
        static const size_t NUM_BITS =
                GROUP_ID == LAST_GROUP ? NUM_BITS_LAST_GROUP : NUM_BITS_PER_GROUP;
        WordUnit *word_ptr = &data_[GROUP_ID][segment_offset];
        __builtin_prefetch(word_ptr + PREFETCH_DISTANCE);
        for (size_t bit_id = 0; bit_id < NUM_BITS; bit_id++) {
            switch (CMP) {
                case kEqual:
                case kInequal:
                    mask_equal = mask_equal & ~(*word_ptr ^ *literal_bit_ptr);
                    break;
                case kLess:
                case kLessEqual:
                    mask_less = mask_less | (mask_equal & ~*word_ptr & *literal_bit_ptr);
                    mask_equal = mask_equal & ~(*word_ptr ^ *literal_bit_ptr);
                    break;
                case kGreater:
                case kGreaterEqual:
                    mask_greater = mask_greater | (mask_equal & *word_ptr & ~*literal_bit_ptr);
                    mask_equal = mask_equal & ~(*word_ptr ^ *literal_bit_ptr);
                    break;
            }
            word_ptr++;
            literal_bit_ptr++;
        }
    }

    template<size_t CODE_SIZE>
    Status BwVColumnBlock<CODE_SIZE>::Scan(Comparator comparator, Code literal,
                                           BitVectorBlock &bitvector, BitVectorOpt opt) {
        if (bitvector.GetNum() != num_)
            return Status::InvalidArgument("Bit vector length does not match column length.");
        if (literal >= (1LL << bit_width_))
            return Status::InvalidArgument("The code in predicate overflows.");

        switch (comparator) {
            case kEqual:
                return Scan<kEqual>(literal, bitvector, opt);
            case kInequal:
                return Scan<kInequal>(literal, bitvector, opt);
            case kGreater:
                return Scan<kGreater>(literal, bitvector, opt);
            case kLess:
                return Scan<kLess>(literal, bitvector, opt);
            case kGreaterEqual:
                return Scan<kGreaterEqual>(literal, bitvector, opt);
            case kLessEqual:
                return Scan<kLessEqual>(literal, bitvector, opt);
        }
        return Status::InvalidArgument("Unknown comparator in BitWeaving/H scan.");
    }

    template<size_t CODE_SIZE>
    template<Comparator CMP>
    Status BwVColumnBlock<CODE_SIZE>::Scan(Code literal, BitVectorBlock &bitvector, BitVectorOpt opt) {
        switch (opt) {
            case kSet:
                return Scan<CMP, kSet>(literal, bitvector);
            case kAnd:
                return Scan<CMP, kAnd>(literal, bitvector);
            case kOr:
                return Scan<CMP, kOr>(literal, bitvector);
        }
        return Status::InvalidArgument("Unkown bit vector operator in BitWeaving/H scan.");
    }

    template<size_t CODE_SIZE>
    template<Comparator CMP, BitVectorOpt OPT>
    Status BwVColumnBlock<CODE_SIZE>::Scan(Code literal, BitVectorBlock &bitvector) {
        WordUnit literal_bits[NUM_BITS_PER_CODE];
        for (size_t bit_id = 0; bit_id < NUM_BITS_PER_CODE; bit_id++) {
            literal_bits[bit_id] = 0ULL -
                                   ((literal >> (NUM_BITS_PER_CODE - 1 - bit_id)) & 1ULL);
        }

        for (size_t segment_offset = 0;
             segment_offset < num_used_words_;
             segment_offset += NUM_WORDS_PER_SEGMENT) {
            size_t segment_id = segment_offset / NUM_WORDS_PER_SEGMENT;
            WordUnit mask_less = 0;
            WordUnit mask_greater = 0;
            WordUnit mask_equal;

            WordUnit mask_bitvector = bitvector.GetWordUnit(segment_id);
            switch (OPT) {
                case kSet:
                    mask_equal = -1ULL;
                    break;
                case kAnd:
                    mask_equal = mask_bitvector;
                    break;
                case kOr:
                    mask_equal = ~mask_bitvector;
                    break;
            }

            WordUnit *literal_bit_ptr = literal_bits;
            if (NUM_BITS_PER_CODE >= NUM_BITS_PER_GROUP && mask_equal != 0) {
                ScanIteration<CMP, 0>(segment_offset, mask_equal, mask_less, mask_greater, literal_bit_ptr);
                if (NUM_BITS_PER_CODE >= NUM_BITS_PER_GROUP * 2 && mask_equal != 0) {
                    ScanIteration<CMP, 1>(segment_offset, mask_equal, mask_less, mask_greater, literal_bit_ptr);
                    if (NUM_BITS_PER_CODE >= NUM_BITS_PER_GROUP * 3 && mask_equal != 0) {
                        ScanIteration<CMP, 2>(segment_offset, mask_equal, mask_less, mask_greater, literal_bit_ptr);
                        if (NUM_BITS_PER_CODE >= NUM_BITS_PER_GROUP * 4 && mask_equal != 0) {
                            ScanIteration<CMP, 3>(segment_offset, mask_equal, mask_less, mask_greater, literal_bit_ptr);
                            if (NUM_BITS_PER_CODE >= NUM_BITS_PER_GROUP * 5 && mask_equal != 0) {
                                ScanIteration<CMP, 4>(segment_offset, mask_equal, mask_less, mask_greater,
                                                      literal_bit_ptr);
                                if (NUM_BITS_PER_CODE >= NUM_BITS_PER_GROUP * 6 && mask_equal != 0) {
                                    ScanIteration<CMP, 5>(segment_offset, mask_equal, mask_less, mask_greater,
                                                          literal_bit_ptr);
                                    if (NUM_BITS_PER_CODE >= NUM_BITS_PER_GROUP * 7 && mask_equal != 0) {
                                        ScanIteration<CMP, 6>(segment_offset, mask_equal, mask_less, mask_greater,
                                                              literal_bit_ptr);
                                        if (NUM_BITS_PER_CODE >= NUM_BITS_PER_GROUP * 8 && mask_equal != 0) {
                                            ScanIteration<CMP, 7>(segment_offset, mask_equal, mask_less, mask_greater,
                                                                  literal_bit_ptr);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (NUM_BITS_LAST_GROUP != 0 && mask_equal != 0) {
                size_t last_group_segment_offset = segment_id * NUM_BITS_LAST_GROUP;
                ScanIteration<CMP, LAST_GROUP>(last_group_segment_offset, mask_equal, mask_less, mask_greater,
                                               literal_bit_ptr);
            }

            WordUnit mask;
            switch (CMP) {
                case kEqual:
                    mask = mask_equal;
                    break;
                case kInequal:
                    mask = ~mask_equal;
                    break;
                case kGreater:
                    mask = mask_greater;
                    break;
                case kLess:
                    mask = mask_less;
                    break;
                case kGreaterEqual:
                    mask = mask_greater | mask_equal;
                    break;
                case kLessEqual:
                    mask = mask_less | mask_equal;
                    break;
            }

            WordUnit result;
            switch (OPT) {
                case kSet:
                    bitvector.SetWordUnit(segment_id, mask);
                    result = mask;
                    break;
                case kAnd:
                    result = mask & mask_bitvector;
                    bitvector.SetWordUnit(segment_id, result);
                    break;
                case kOr:
                    result = mask | mask_bitvector;
                    bitvector.SetWordUnit(segment_id, result);
                    break;
            }
        }

        bitvector.Finalize();

        return Status::OK();
    }

    template<size_t CODE_SIZE>
    template<Comparator CMP, size_t GROUP_ID>
    void BwVColumnBlock<CODE_SIZE>::ScanIteration(size_t segment_offset,
                                                  WordUnit &mask_equal, WordUnit &mask_less, WordUnit &mask_greater,
                                                  WordUnit **other_data) {
        static const size_t NUM_BITS =
                GROUP_ID == LAST_GROUP ? NUM_BITS_LAST_GROUP : NUM_BITS_PER_GROUP;
        WordUnit *word_ptr = &data_[GROUP_ID][segment_offset];
        WordUnit *other_word_ptr = &other_data[GROUP_ID][segment_offset];
        __builtin_prefetch(word_ptr + PREFETCH_DISTANCE);
        __builtin_prefetch(other_word_ptr + PREFETCH_DISTANCE);
        for (size_t bit_id = 0; bit_id < NUM_BITS; bit_id++) {
            switch (CMP) {
                case kEqual:
                case kInequal:
                    mask_equal = mask_equal & ~(*word_ptr ^ *other_word_ptr);
                    break;
                case kLess:
                case kLessEqual:
                    mask_less = mask_less | (mask_equal & ~*word_ptr & *other_word_ptr);
                    mask_equal = mask_equal & ~(*word_ptr ^ *other_word_ptr);
                    break;
                case kGreater:
                case kGreaterEqual:
                    mask_greater = mask_greater | (mask_equal & *word_ptr & ~*other_word_ptr);
                    mask_equal = mask_equal & ~(*word_ptr ^ *other_word_ptr);
                    break;
            }
            word_ptr++;
            other_word_ptr++;
        }
    }

    template<size_t CODE_SIZE>
    Status BwVColumnBlock<CODE_SIZE>::Scan(Comparator comparator, ColumnBlock &column_block,
                                           BitVectorBlock &bitvector, BitVectorOpt opt) {
        if (bitvector.GetNum() != num_)
            return Status::InvalidArgument("Bit vector length does not match column length.");
        if (column_block.GetNumCodes() != num_)
            return Status::InvalidArgument("Column length does not match.");
        if (column_block.GetColumnType() != kBitWeavingV)
            return Status::InvalidArgument(
                    "Column type does not match. Input column should be in BitWeaving/H format.");
        if (column_block.GetBitWidth() != bit_width_)
            return Status::InvalidArgument("Code size does not match.");

        switch (comparator) {
            case kEqual:
                return Scan<kEqual>(column_block, bitvector, opt);
            case kInequal:
                return Scan<kInequal>(column_block, bitvector, opt);
            case kGreater:
                return Scan<kGreater>(column_block, bitvector, opt);
            case kLess:
                return Scan<kLess>(column_block, bitvector, opt);
            case kGreaterEqual:
                return Scan<kGreaterEqual>(column_block, bitvector, opt);
            case kLessEqual:
                return Scan<kLessEqual>(column_block, bitvector, opt);
        }
        return Status::InvalidArgument("Unknown comparator in BitWeaving/H scan.");
    }

    template<size_t CODE_SIZE>
    template<Comparator CMP>
    Status BwVColumnBlock<CODE_SIZE>::Scan(ColumnBlock &column_block, BitVectorBlock &bitvector, BitVectorOpt opt) {
        switch (opt) {
            case kSet:
                return Scan<CMP, kSet>(column_block, bitvector);
            case kAnd:
                return Scan<CMP, kAnd>(column_block, bitvector);
            case kOr:
                return Scan<CMP, kOr>(column_block, bitvector);
        }
        return Status::InvalidArgument("Unkown bit vector operator in BitWeaving/H scan.");
    }

    template<size_t CODE_SIZE>
    template<Comparator CMP, BitVectorOpt OPT>
    Status BwVColumnBlock<CODE_SIZE>::Scan(ColumnBlock &column_block, BitVectorBlock &bitvector) {
        assert(bitvector.GetNum() == num_);
        assert(column_block.GetNumCodes() == num_);
        assert(column_block.GetColumnType() == kBitWeavingV);
        assert(column_block.GetBitWidth() == bit_width_);

        // Now safe to cast to BwHColumnBlock
        BwVColumnBlock<CODE_SIZE> *other_block =
                static_cast<BwVColumnBlock<CODE_SIZE> *>(&column_block);
        WordUnit **other_data = other_block->data_;
        for (size_t segment_offset = 0;
             segment_offset < num_used_words_;
             segment_offset += NUM_WORDS_PER_SEGMENT) {
            size_t segment_id = segment_offset / NUM_WORDS_PER_SEGMENT;
            WordUnit mask_less = 0;
            WordUnit mask_greater = 0;
            WordUnit mask_equal;

            WordUnit mask_bitvector = bitvector.GetWordUnit(segment_id);
            switch (OPT) {
                case kSet:
                    mask_equal = -1ULL;
                    break;
                case kAnd:
                    mask_equal = mask_bitvector;
                    break;
                case kOr:
                    mask_equal = ~mask_bitvector;
                    break;
            }

            if (NUM_BITS_PER_CODE >= NUM_BITS_PER_GROUP && mask_equal != 0) {
                ScanIteration<CMP, 0>(segment_offset, mask_equal, mask_less, mask_greater, other_data);
                if (NUM_BITS_PER_CODE >= NUM_BITS_PER_GROUP * 2 && mask_equal != 0) {
                    ScanIteration<CMP, 1>(segment_offset, mask_equal, mask_less, mask_greater, other_data);
                    if (NUM_BITS_PER_CODE >= NUM_BITS_PER_GROUP * 3 && mask_equal != 0) {
                        ScanIteration<CMP, 2>(segment_offset, mask_equal, mask_less, mask_greater, other_data);
                        if (NUM_BITS_PER_CODE >= NUM_BITS_PER_GROUP * 4 && mask_equal != 0) {
                            ScanIteration<CMP, 3>(segment_offset, mask_equal, mask_less, mask_greater, other_data);
                            if (NUM_BITS_PER_CODE >= NUM_BITS_PER_GROUP * 5 && mask_equal != 0) {
                                ScanIteration<CMP, 4>(segment_offset, mask_equal, mask_less, mask_greater, other_data);
                                if (NUM_BITS_PER_CODE >= NUM_BITS_PER_GROUP * 6 && mask_equal != 0) {
                                    ScanIteration<CMP, 5>(segment_offset, mask_equal, mask_less, mask_greater,
                                                          other_data);
                                    if (NUM_BITS_PER_CODE >= NUM_BITS_PER_GROUP * 7 && mask_equal != 0) {
                                        ScanIteration<CMP, 6>(segment_offset, mask_equal, mask_less, mask_greater,
                                                              other_data);
                                        if (NUM_BITS_PER_CODE >= NUM_BITS_PER_GROUP * 8 && mask_equal != 0) {
                                            ScanIteration<CMP, 7>(segment_offset, mask_equal, mask_less, mask_greater,
                                                                  other_data);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (NUM_BITS_LAST_GROUP != 0 && mask_equal != 0) {
                size_t last_group_segment_offset = segment_id * NUM_BITS_LAST_GROUP;
                ScanIteration<CMP, LAST_GROUP>(last_group_segment_offset, mask_equal, mask_less, mask_greater,
                                               other_data);
            }

            WordUnit mask;
            switch (CMP) {
                case kEqual:
                    mask = mask_equal;
                    break;
                case kInequal:
                    mask = ~mask_equal;
                    break;
                case kGreater:
                    mask = mask_greater;
                    break;
                case kLess:
                    mask = mask_less;
                    break;
                case kGreaterEqual:
                    mask = mask_greater | mask_equal;
                    break;
                case kLessEqual:
                    mask = mask_less | mask_equal;
                    break;
            }

            switch (OPT) {
                case kSet:
                    bitvector.SetWordUnit(segment_id, mask);
                    break;
                case kAnd:
                    bitvector.SetWordUnit(segment_id, mask & mask_bitvector);
                    break;
                case kOr:
                    bitvector.SetWordUnit(segment_id, mask | mask_bitvector);
                    break;
            }
        }

        bitvector.Finalize();

        return Status::OK();
    }

/**
 * explicit instantiations
 */
    template
    class BwVColumnBlock<1>;

    template
    class BwVColumnBlock<2>;

    template
    class BwVColumnBlock<3>;

    template
    class BwVColumnBlock<4>;

    template
    class BwVColumnBlock<5>;

    template
    class BwVColumnBlock<6>;

    template
    class BwVColumnBlock<7>;

    template
    class BwVColumnBlock<8>;

    template
    class BwVColumnBlock<9>;

    template
    class BwVColumnBlock<10>;

    template
    class BwVColumnBlock<11>;

    template
    class BwVColumnBlock<12>;

    template
    class BwVColumnBlock<13>;

    template
    class BwVColumnBlock<14>;

    template
    class BwVColumnBlock<15>;

    template
    class BwVColumnBlock<16>;

    template
    class BwVColumnBlock<17>;

    template
    class BwVColumnBlock<18>;

    template
    class BwVColumnBlock<19>;

    template
    class BwVColumnBlock<20>;

    template
    class BwVColumnBlock<21>;

    template
    class BwVColumnBlock<22>;

    template
    class BwVColumnBlock<23>;

    template
    class BwVColumnBlock<24>;

    template
    class BwVColumnBlock<25>;

    template
    class BwVColumnBlock<26>;

    template
    class BwVColumnBlock<27>;

    template
    class BwVColumnBlock<28>;

    template
    class BwVColumnBlock<29>;

    template
    class BwVColumnBlock<30>;

    template
    class BwVColumnBlock<31>;

    template
    class BwVColumnBlock<32>;

} // namespace bitweaving
