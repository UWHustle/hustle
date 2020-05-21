// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <cstring>

#include "file.h"
#include "column_block.h"
#include "bwh_column_block.h"
#include "bitvector_block.h"
#include "utility.h"

namespace hustle::bitweaving {

    template<size_t CODE_SIZE>
    BwHColumnBlock<CODE_SIZE>::BwHColumnBlock() :
            ColumnBlock(kBitWeavingH, CODE_SIZE), num_used_words_(0) {
        data_ = new WordUnit[NUM_WORDS];
        assert(data_ != NULL);
        memset(data_, 0, sizeof(WordUnit) * NUM_WORDS);
        max_code_ = 0;
    }

    template<size_t CODE_SIZE>
    BwHColumnBlock<CODE_SIZE>::~BwHColumnBlock() {
        delete[] data_;
    }

    template<size_t CODE_SIZE>
    Status BwHColumnBlock<CODE_SIZE>::GetCode(TupleId pos, Code &code) {
        if (pos >= num_)
            return Status::InvalidArgument("GetCode: position overflow");

        size_t segment_id = pos / NUM_CODES_PER_SEGMENT;
        size_t code_id_in_segment = pos % NUM_CODES_PER_SEGMENT;
        size_t word_id = segment_id * NUM_WORDS_PER_SEGMENT + code_id_in_segment % NUM_WORDS_PER_SEGMENT;
        size_t code_id_in_word = code_id_in_segment / NUM_WORDS_PER_SEGMENT;

        static const WordUnit mask = (1ULL << CODE_SIZE) - 1;
        assert(word_id < NUM_WORDS);
        int shift = (NUM_CODES_PER_WORD - 1 - code_id_in_word) * NUM_BITS_PER_CODE;
        code = (data_[word_id] >> shift) & mask;
        return Status::OK();
    }

    template<size_t CODE_SIZE>
    Status BwHColumnBlock<CODE_SIZE>::SetCode(TupleId pos, Code code) {
        if (pos >= num_)
            return Status::InvalidArgument("GetCode: position overflow");

        size_t segment_id = pos / NUM_CODES_PER_SEGMENT;
        size_t code_id_in_segment = pos % NUM_CODES_PER_SEGMENT;
        size_t word_id = segment_id * NUM_WORDS_PER_SEGMENT + code_id_in_segment % NUM_WORDS_PER_SEGMENT;
        size_t code_id_in_word = code_id_in_segment / NUM_WORDS_PER_SEGMENT;

        assert(word_id < NUM_WORDS);
        int shift = (NUM_CODES_PER_WORD - 1 - code_id_in_word) * NUM_BITS_PER_CODE;
        data_[word_id] |= (WordUnit) code << shift;

        return Status::OK();
    }

    template<size_t CODE_SIZE>
    AppendResult *BwHColumnBlock<CODE_SIZE>::Append(const Code *codes, size_t num) {
        if (num_ + num > NUM_CODES_PER_BLOCK) {
            return new AppendResult(Status::InvalidArgument("Load too many codes into a column block."),
                                    false,
                                    CODE_SIZE);
        }
        num_ += num;
        num_used_words_ = bceil(num_, NUM_CODES_PER_SEGMENT) * NUM_WORDS_PER_SEGMENT;
        for (size_t i = 0; i < num; i++) {
            max_code_ = std::max((Code) max_code_, codes[i]);
            SetCode(i + (num_ - num), codes[i]);
        }
        if(max_code_ >= ((uint64_t)1 << CODE_SIZE)){
            int suggested_bitwidth = ceil(log2(max_code_)) == floor(log2(max_code_)) ?
                                     ceil(log2(max_code_)) + 1 : ceil(log2(max_code_));
            std::cout << "suggested bitwidth " << suggested_bitwidth << std::endl;
            return new AppendResult(Status::InvalidBitwidth("Invalid Bitwidth. "
                                                            "Check for the suggested value"),
                                    false,
                                    suggested_bitwidth);
        }
        return new AppendResult(Status::OK(), true, CODE_SIZE);
    }

    template<size_t CODE_SIZE>
    Status BwHColumnBlock<CODE_SIZE>::Load(SequentialReadFile &file) {
        Status status;

        status = file.Read(reinterpret_cast<char *>(&num_), sizeof(size_t));
        if (!status.IsOk())
            return status;

        status = file.Read(reinterpret_cast<char *>(&num_used_words_), sizeof(size_t));
        if (!status.IsOk())
            return status;

        status = file.Read(reinterpret_cast<char *>(data_),
                           sizeof(WordUnit) * NUM_WORDS);
        if (!status.IsOk())
            return status;

        return Status::OK();
    }

    template<size_t CODE_SIZE>
    Status BwHColumnBlock<CODE_SIZE>::Save(SequentialWriteFile &file) {
        Status status;

        status = file.Append(reinterpret_cast<char *>(&num_), sizeof(size_t));
        if (!status.IsOk())
            return status;

        status = file.Append(reinterpret_cast<char *>(&num_used_words_), sizeof(size_t));
        if (!status.IsOk())
            return status;

        status = file.Append(reinterpret_cast<char *>(data_),
                             sizeof(WordUnit) * NUM_WORDS);
        if (!status.IsOk())
            return status;

        return Status::OK();
    }

    template<size_t CODE_SIZE>
    Status BwHColumnBlock<CODE_SIZE>::Scan(Comparator comparator, Code literal,
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
    Status BwHColumnBlock<CODE_SIZE>::Scan(Code literal, BitVectorBlock &bitvector, BitVectorOpt opt) {
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
    Status BwHColumnBlock<CODE_SIZE>::Scan(Code literal, BitVectorBlock &bitvector) {
        assert(bitvector.GetNum() == num_);
        assert(literal < (1LL << bit_width_));

        // Generate masks
        // k = number of bits per code
        // base_mask: 0^k 1 0^k1 1 ... 0^k 1
        // predicate_mask: 0 code 0 code ... 0 code
        // complement_mask: 0 1^k 0 1^k 0 ... 0 1^k
        // result_mask: 1 0^k 1 0^k 1 ... 1 0^k
        WordUnit base_mask = 0;
        for (size_t i = 0; i < NUM_CODES_PER_WORD; i++) {
            base_mask = (base_mask << NUM_BITS_PER_CODE) | 1ULL;
        }
        WordUnit complement_mask = base_mask * ((1ULL << bit_width_) - 1ULL);
        WordUnit result_mask = base_mask << bit_width_;

        WordUnit less_than_mask = base_mask * literal;
        WordUnit greater_than_mask = (base_mask * literal) ^complement_mask;
        WordUnit equal_mask = base_mask * (~literal & (-1ULL >> (NUM_BITS_PER_WORD - bit_width_)));
        WordUnit inequal_mask = base_mask * literal;

        // Done for initialization

        WordUnit word_bitvector, output_bitvector = 0;
        int64_t output_offset = 0;
        size_t output_word_id = 0;
        for (size_t segment_offset = 0; segment_offset < num_used_words_; segment_offset += NUM_WORDS_PER_SEGMENT) {
            WordUnit segment_bitvector = 0;
            size_t word_id = 0;

            // A loop over all words inside a segment.
            // We break down the loop into several fixed-length small loops, making it easy to
            // unroll these loops.
            // This optimization provides about ~10% improvement
            for (size_t bit_group_id = 0; bit_group_id < NUM_WORD_GROUPS; bit_group_id++) {
                // Prefetch data into CPU data cache
                // This optimization provides about ~10% improvement
                __builtin_prefetch(data_ + segment_offset + word_id + PREFETCH_DISTANCE);

                for (size_t bit = 0; bit < NUM_WORDS_PER_WORD_GROUP; bit++) {
                    switch (CMP) {
                        case kEqual:
                            word_bitvector = ((data_[segment_offset + word_id] ^ equal_mask) + base_mask) & result_mask;
                            break;
                        case kInequal:
                            word_bitvector =
                                    ((data_[segment_offset + word_id] ^ inequal_mask) + complement_mask) & result_mask;
                            break;
                        case kGreater:
                            word_bitvector = (data_[segment_offset + word_id] + greater_than_mask) & result_mask;
                            break;
                        case kLess:
                            word_bitvector = (less_than_mask + (data_[segment_offset + word_id] ^ complement_mask)) &
                                             result_mask;
                            break;
                        case kGreaterEqual:
                            word_bitvector = ~(less_than_mask + (data_[segment_offset + word_id] ^ complement_mask)) &
                                             result_mask;
                            break;
                        case kLessEqual:
                            word_bitvector = ~(data_[segment_offset + word_id] + greater_than_mask) & result_mask;
                            break;
                        default:
                            return Status::InvalidArgument("Unknown comparator.");
                    }
                    segment_bitvector |= word_bitvector >> word_id++;
                }
            }

            // Prefetch
            __builtin_prefetch(data_ + segment_offset + word_id + PREFETCH_DISTANCE);

            // A small loop on the rest of bit positions
            for (size_t bit = 0; bit < NUM_UNGROUPED_WORDS; bit++) {
                switch (CMP) {
                    case kEqual:
                        word_bitvector = ((data_[segment_offset + word_id] ^ equal_mask) + base_mask) & result_mask;
                        break;
                    case kInequal:
                        word_bitvector =
                                ((data_[segment_offset + word_id] ^ inequal_mask) + complement_mask) & result_mask;
                        break;
                    case kGreater:
                        word_bitvector = (data_[segment_offset + word_id] + greater_than_mask) & result_mask;
                        break;
                    case kLess:
                        word_bitvector =
                                (less_than_mask + (data_[segment_offset + word_id] ^ complement_mask)) & result_mask;
                        break;
                    case kGreaterEqual:
                        word_bitvector =
                                ~(less_than_mask + (data_[segment_offset + word_id] ^ complement_mask)) & result_mask;
                        break;
                    case kLessEqual:
                        word_bitvector = ~(data_[segment_offset + word_id] + greater_than_mask) & result_mask;
                        break;
                    default:
                        return Status::InvalidArgument("Unknown comparator.");
                }
                segment_bitvector |= word_bitvector >> word_id++;
            }

            output_bitvector |= (segment_bitvector << NUM_EMPTY_BITS) >> output_offset;
            output_offset += NUM_CODES_PER_SEGMENT;

            if (output_offset >= (int64_t) NUM_BITS_PER_WORD) {
                switch (OPT) {
                    case kSet:
                        break;
                    case kAnd:
                        output_bitvector &= bitvector.GetWordUnit(output_word_id);
                        break;
                    case kOr:
                        output_bitvector |= bitvector.GetWordUnit(output_word_id);
                        break;
                    default:
                        return Status::InvalidArgument("Unknown bit vector operator.");
                }
                bitvector.SetWordUnit(output_word_id, output_bitvector);

                output_offset -= NUM_BITS_PER_WORD;
                size_t output_shift = NUM_BITS_PER_WORD - output_offset;
                output_bitvector = segment_bitvector << output_shift;
                //clear up outputBitvector if outputOffset = NUM_BITS_PER_WORD
                output_bitvector &= 0ULL - (output_shift != NUM_BITS_PER_WORD);
                output_word_id++;
            }
        }

        output_offset -= num_used_words_ * NUM_CODES_PER_WORD - num_;
        if (output_offset > 0) {
            switch (OPT) {
                case kSet:
                    break;
                case kAnd:
                    output_bitvector &= bitvector.GetWordUnit(output_word_id);
                    break;
                case kOr:
                    output_bitvector |= bitvector.GetWordUnit(output_word_id);
                    break;
                default:
                    return Status::InvalidArgument("Unknown bit vector operator.");
            }
            bitvector.SetWordUnit(output_word_id, output_bitvector);
        }

        bitvector.Finalize();
        return Status::OK();
    }

    template<size_t CODE_SIZE>
    Status BwHColumnBlock<CODE_SIZE>::Scan(Comparator comparator, ColumnBlock &column_block,
                                           BitVectorBlock &bitvector, BitVectorOpt opt) {
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
    Status BwHColumnBlock<CODE_SIZE>::Scan(ColumnBlock &column_block, BitVectorBlock &bitvector, BitVectorOpt opt) {
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
    Status BwHColumnBlock<CODE_SIZE>::Scan(ColumnBlock &column_block, BitVectorBlock &bitvector) {
        if (bitvector.GetNum() != num_)
            return Status::InvalidArgument("Bit vector length does not match column length.");
        if (column_block.GetNumCodes() != num_)
            return Status::InvalidArgument("Column length does not match.");
        if (column_block.GetColumnType() != kBitWeavingH)
            return Status::InvalidArgument(
                    "Column type does not match. Input column should be in BitWeaving/H format.");
        if (column_block.GetBitWidth() != bit_width_)
            return Status::InvalidArgument("Code size does not match.");

        // Now safe to cast to BwHColumnBlock
        BwHColumnBlock<CODE_SIZE> *other_block =
                static_cast<BwHColumnBlock<CODE_SIZE> *>(&column_block);

        size_t min_used_words = std::min(num_used_words_, other_block->num_used_words_);

        // Generate masks
        // k = number of bits per code
        // base_mask: 0^k 1 0^k1 1 ... 0^k 1
        // complement_mask: 0 1^k 0 1^k 0 ... 0 1^k
        // result_mask: 1 0^k 1 0^k 1 ... 1 0^k
        WordUnit base_mask = 0;
        for (size_t i = 0; i < NUM_CODES_PER_WORD; i++) {
            base_mask = (base_mask << NUM_BITS_PER_CODE) | 1ULL;
        }
        WordUnit complement_mask = base_mask * ((1ULL << bit_width_) - 1ULL);
        WordUnit result_mask = base_mask << bit_width_;

        // Done for initialization

        WordUnit word_bitvector, output_bitvector = 0;
        int64_t output_offset = 0;
        size_t output_word_id = 0;
        WordUnit *other_data = other_block->data_;
        for (size_t segment_offset = 0;
             segment_offset < min_used_words;
             segment_offset += NUM_WORDS_PER_SEGMENT) {
            WordUnit segment_bitvector = 0;
            size_t word_id = 0;

            // A loop over all words inside a segment.
            // We break down the loop into several fixed-length small loops, making it easy to
            // unroll these loops.
            // This optimization provides about ~10% improvement
            for (size_t bit_group_id = 0; bit_group_id < NUM_WORD_GROUPS; bit_group_id++) {
                // Prefetch data into CPU data cache
                // This optimization provides about ~10% improvement
                __builtin_prefetch(data_ + segment_offset + word_id + PREFETCH_DISTANCE);
                __builtin_prefetch(other_data + segment_offset + word_id + PREFETCH_DISTANCE);

                for (size_t bit = 0; bit < NUM_WORDS_PER_WORD_GROUP; bit++) {
                    size_t id = segment_offset + word_id;
                    switch (CMP) {
                        case kEqual:
                            word_bitvector = ~((data_[id] ^ other_data[id]) + complement_mask) & result_mask;
                            break;
                        case kInequal:
                            word_bitvector = ((data_[id] ^ other_data[id]) + complement_mask) & result_mask;
                            break;
                        case kGreater:
                            word_bitvector = (data_[id] + (other_data[id] ^ complement_mask)) & result_mask;
                            break;
                        case kLess:
                            word_bitvector = (other_data[id] + (data_[id] ^ complement_mask)) & result_mask;
                            break;
                        case kGreaterEqual:
                            word_bitvector = (~(other_data[id] + (data_[id] ^ complement_mask))) & result_mask;
                            break;
                        case kLessEqual:
                            word_bitvector = (~(data_[id] + (other_data[id] ^ complement_mask))) & result_mask;
                            break;
                        default:
                            return Status::InvalidArgument("Unknown comparator.");
                    }
                    segment_bitvector |= word_bitvector >> word_id++;
                }
            }

            // Prefetch
            __builtin_prefetch(data_ + segment_offset + word_id + PREFETCH_DISTANCE);
            __builtin_prefetch(other_data + segment_offset + word_id + PREFETCH_DISTANCE);

            // A small loop on the rest of bit positions
            for (size_t bit = 0; bit < NUM_UNGROUPED_WORDS; bit++) {
                size_t id = segment_offset + word_id;
                switch (CMP) {
                    case kEqual:
                        word_bitvector = ~((data_[id] ^ other_data[id]) + complement_mask) & result_mask;
                        break;
                    case kInequal:
                        word_bitvector = ((data_[id] ^ other_data[id]) + complement_mask) & result_mask;
                        break;
                    case kGreater:
                        word_bitvector = (data_[id] + (other_data[id] ^ complement_mask)) & result_mask;
                        break;
                    case kLess:
                        word_bitvector = (other_data[id] + (data_[id] ^ complement_mask)) & result_mask;
                        break;
                    case kGreaterEqual:
                        word_bitvector = (~(other_data[id] + (data_[id] ^ complement_mask))) & result_mask;
                        break;
                    case kLessEqual:
                        word_bitvector = (~(data_[id] + (other_data[id] ^ complement_mask))) & result_mask;
                        break;
                    default:
                        return Status::InvalidArgument("Unknown comparator.");
                }
                segment_bitvector |= word_bitvector >> word_id++;
            }

            output_bitvector |= (segment_bitvector << NUM_EMPTY_BITS) >> output_offset;
            output_offset += NUM_CODES_PER_SEGMENT;

            if (output_offset >= (int64_t) NUM_BITS_PER_WORD) {
                switch (OPT) {
                    case kSet:
                        break;
                    case kAnd:
                        output_bitvector &= bitvector.GetWordUnit(output_word_id);
                        break;
                    case kOr:
                        output_bitvector |= bitvector.GetWordUnit(output_word_id);
                        break;
                    default:
                        return Status::InvalidArgument("Unknown bit vector operator.");
                }
                bitvector.SetWordUnit(output_word_id, output_bitvector);

                output_offset -= NUM_BITS_PER_WORD;
                size_t output_shift = NUM_BITS_PER_WORD - output_offset;
                output_bitvector = segment_bitvector << output_shift;
                //clear up outputBitvector if outputOffset = NUM_BITS_PER_WORD
                output_bitvector &= 0ULL - (output_shift != NUM_BITS_PER_WORD);
                output_word_id++;
            }
        }

        output_offset -= num_used_words_ * NUM_CODES_PER_WORD - num_;
        if (output_offset > 0) {
            switch (OPT) {
                case kSet:
                    break;
                case kAnd:
                    output_bitvector &= bitvector.GetWordUnit(output_word_id);
                    break;
                case kOr:
                    output_bitvector |= bitvector.GetWordUnit(output_word_id);
                    break;
                default:
                    return Status::InvalidArgument("Unknown bit vector operator.");
            }
            bitvector.SetWordUnit(output_word_id, output_bitvector);
        }

        bitvector.Finalize();
        return Status::OK();
    }

/**
 * @brief explicit instantiations.
 */
    template
    class BwHColumnBlock<1>;

    template
    class BwHColumnBlock<2>;

    template
    class BwHColumnBlock<3>;

    template
    class BwHColumnBlock<4>;

    template
    class BwHColumnBlock<5>;

    template
    class BwHColumnBlock<6>;

    template
    class BwHColumnBlock<7>;

    template
    class BwHColumnBlock<8>;

    template
    class BwHColumnBlock<9>;

    template
    class BwHColumnBlock<10>;

    template
    class BwHColumnBlock<11>;

    template
    class BwHColumnBlock<12>;

    template
    class BwHColumnBlock<13>;

    template
    class BwHColumnBlock<14>;

    template
    class BwHColumnBlock<15>;

    template
    class BwHColumnBlock<16>;

    template
    class BwHColumnBlock<17>;

    template
    class BwHColumnBlock<18>;

    template
    class BwHColumnBlock<19>;

    template
    class BwHColumnBlock<20>;

    template
    class BwHColumnBlock<21>;

    template
    class BwHColumnBlock<22>;

    template
    class BwHColumnBlock<23>;

    template
    class BwHColumnBlock<24>;

    template
    class BwHColumnBlock<25>;

    template
    class BwHColumnBlock<26>;

    template
    class BwHColumnBlock<27>;

    template
    class BwHColumnBlock<28>;

    template
    class BwHColumnBlock<29>;

    template
    class BwHColumnBlock<30>;

    template
    class BwHColumnBlock<31>;

    template
    class BwHColumnBlock<32>;

}// namespace bitweaving

