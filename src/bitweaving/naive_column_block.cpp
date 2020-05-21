// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include "file.h"
#include "column_block.h"
#include "naive_column_block.h"
#include "bitvector_block.h"
#include "utility.h"

namespace hustle::bitweaving {

    NaiveColumnBlock::NaiveColumnBlock() :
            ColumnBlock(kNaive, sizeof(Code) * NUM_BITS_PER_BYTE) {
        data_ = new WordUnit[NUM_CODES_PER_BLOCK];
    }

    NaiveColumnBlock::~NaiveColumnBlock() {
        delete[] data_;
    }

    AppendResult *NaiveColumnBlock::Append(const Code *codes, size_t num) {
        if (num_ + num > NUM_CODES_PER_BLOCK) {
            return new AppendResult(Status::InvalidArgument("Load too many codes into a column block."),
                                    false,
                                    sizeof(Code) * NUM_BITS_PER_BYTE);
        }
        for (size_t i = 0; i < num; i++) {
            data_[num_ + i] = codes[i];
        }
        num_ += num;
        return new AppendResult(Status::OK(), true, sizeof(Code) * NUM_BITS_PER_BYTE);
    }

    Status NaiveColumnBlock::Load(SequentialReadFile &file) {
        Status status;

        status = file.Read(reinterpret_cast<char *>(&num_), sizeof(size_t));
        if (!status.IsOk())
            return status;

        status = file.Read(reinterpret_cast<char *>(data_),
                           sizeof(WordUnit) * NUM_CODES_PER_BLOCK);
        if (!status.IsOk())
            return status;

        return Status::OK();
    }

    Status NaiveColumnBlock::Save(SequentialWriteFile &file) {
        Status status;

        status = file.Append(reinterpret_cast<char *>(&num_), sizeof(size_t));
        if (!status.IsOk())
            return status;

        status = file.Append(reinterpret_cast<char *>(data_),
                             sizeof(WordUnit) * NUM_CODES_PER_BLOCK);
        if (!status.IsOk())
            return status;

        return Status::OK();
    }

    Status NaiveColumnBlock::Scan(Comparator comparator, Code code,
                                  BitVectorBlock &bitvector, BitVectorOpt opt) {
        for (size_t offset = 0; offset < num_; offset += NUM_BITS_PER_WORD) {
            WordUnit word_unit = 0;
            for (size_t i = 0; i < NUM_BITS_PER_WORD; i++) {
                size_t id = offset + i;
                if (id >= num_) {
                    break;
                }
                WordUnit bit;
                switch (comparator) {
                    case kEqual:
                        bit = (data_[id] == code);
                        break;
                    case kInequal:
                        bit = (data_[id] != code);
                        break;
                    case kGreater:
                        bit = (data_[id] > code);
                        break;
                    case kLess:
                        bit = (data_[id] < code);
                        break;
                    case kGreaterEqual:
                        bit = (data_[id] >= code);
                        break;
                    case kLessEqual:
                        bit = (data_[id] <= code);
                        break;
                    default:
                        return Status::InvalidArgument("Unknown comparator.");
                }
                word_unit |= bit << (NUM_BITS_PER_WORD - 1 - i);
            }
            size_t word_unit_id = offset / NUM_BITS_PER_WORD;
            WordUnit x;
            switch (opt) {
                case kSet:
                    break;
                case kAnd:
                    x = bitvector.GetWordUnit(word_unit_id);
                    word_unit &= x;
                    break;
                case kOr:
                    word_unit |= bitvector.GetWordUnit(word_unit_id);
                    break;
                default:
                    return Status::InvalidArgument("Unknown bit vector operator.");
            }
            bitvector.SetWordUnit(word_unit_id, word_unit);
        }
        // The column is all NULL in this region, set all bits to be 0s.
        for (size_t offset = bceil(num_, NUM_BITS_PER_WORD) * NUM_BITS_PER_WORD;
             offset < bitvector.GetNum(); offset += NUM_BITS_PER_WORD) {
            size_t word_unit_id = offset / NUM_BITS_PER_WORD;
            bitvector.SetWordUnit(word_unit_id, 0);
        }
        return Status::OK();
    }

    Status NaiveColumnBlock::Scan(Comparator comparator, ColumnBlock &column_block,
                                  BitVectorBlock &bitvector, BitVectorOpt opt) {
        if (bitvector.GetNum() != num_)
            return Status::InvalidArgument("Bit vector length does not match column length.");

        size_t num = std::min(num_, column_block.GetNumCodes());
        for (size_t offset = 0; offset < num; offset += NUM_BITS_PER_WORD) {
            WordUnit word_unit = 0;
            for (size_t i = 0; i < NUM_BITS_PER_WORD; i++) {
                size_t id = offset + i;
                if (id >= num_) {
                    break;
                }
                WordUnit bit;
                Code code;
                column_block.GetCode(id, code);
                switch (comparator) {
                    case kEqual:
                        bit = (data_[id] == code);
                        break;
                    case kInequal:
                        bit = (data_[id] != code);
                        break;
                    case kGreater:
                        bit = (data_[id] > code);
                        break;
                    case kLess:
                        bit = (data_[id] < code);
                        break;
                    case kGreaterEqual:
                        bit = (data_[id] >= code);
                        break;
                    case kLessEqual:
                        bit = (data_[id] <= code);
                        break;
                    default:
                        return Status::InvalidArgument("Unknown comparator.");
                }
                word_unit |= bit << (NUM_BITS_PER_WORD - 1 - i);
            }
            size_t word_unit_id = offset / NUM_BITS_PER_WORD;
            switch (opt) {
                case kSet:
                    break;
                case kAnd:
                    word_unit &= bitvector.GetWordUnit(word_unit_id);
                    break;
                case kOr:
                    word_unit |= bitvector.GetWordUnit(word_unit_id);
                    break;
                default:
                    return Status::InvalidArgument("Unknown bit vector operator.");
            }
            bitvector.SetWordUnit(word_unit_id, word_unit);
        }
        // At least one column is all NULL in this region, set all bits to be 0s.
        for (size_t offset = bceil(num, NUM_BITS_PER_WORD) * NUM_BITS_PER_WORD;
             offset < bitvector.GetNum(); offset += NUM_BITS_PER_WORD) {
            size_t word_unit_id = offset / NUM_BITS_PER_WORD;
            bitvector.SetWordUnit(word_unit_id, 0);
        }
        return Status::OK();
    }

} // namespace bitweaving
