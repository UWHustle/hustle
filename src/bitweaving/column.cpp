// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <iostream>

#include "column.h"
#include "column_block.h"
#include "column_iterator.h"
#include "bitvector_block.h"
#include "file.h"
#include "naive_column_block.h"
#include "bwh_column_block.h"
#include "bwh_column_iterator.h"
#include "bwv_column_block.h"
#include "bwv_column_iterator.h"

namespace hustle::bitweaving {

/**
 * @brief Structure for private members of Class Column.
 */
    struct Column::Rep {
        Rep(ColumnId id, ColumnType type, size_t bit_width, size_t &num)
                : id(id), type(type), bit_width(bit_width), num(num) {
            max_code_ = 0;
            codes_thus_far = 0;
        }

        /**
         * @brief The ID of this column.
         */
        ColumnId id;
        /**
         * @brief The type of this column (Naive/BitWeavingH/BitWeavingV).
         */
        ColumnType type;
        /**
         * @brief The size of code in this column.
         */
        size_t bit_width;
        /**
         * @brief The number of codes in this column.
         */
        size_t &num;
        /**
        * @brief The max code value in the column. Used to track the required bitwidth
        */
        size_t max_code_;
        /**
        * @brief The max code value in the column. Used to track the required bitwidth
        */
        size_t codes_thus_far;
        /**
         * @brief A list of column blocks.
         */
        std::vector<ColumnBlock *> vector;
    };

    Column::Column(ColumnId id, ColumnType type, size_t bit_width, size_t &num)
            : rep_(new Rep(id, type, bit_width, num)) {
    }

    Column::~Column() {
        delete rep_;
    }

    ColumnType Column::GetType() const {
        return rep_->type;
    }

    size_t Column::GetBitWidth() const {
        return rep_->bit_width;
    }

    ColumnId Column::GetColumnId() const {
        return rep_->id;
    }

    size_t Column::GetMaxCode() const {
        return rep_->max_code_;
    }

    size_t Column::GetNumValuesInColumn() const {
        return rep_->codes_thus_far;
    }

    AppendResult *Column::Append(const Code *codes, size_t num) {
        TupleId pos;
        if (rep_->vector.size() == 0) {
            pos = 0;
        } else {
            size_t block_id = rep_->vector.size() - 1;
            size_t block_offset = rep_->vector[block_id]->GetNumCodes();
            pos = block_id * NUM_CODES_PER_BLOCK + block_offset;
        }
        AppendResult* result = Write(pos, codes, num);
        if(result->GetAppendStatus().IsOk()){
            rep_->codes_thus_far += num;
            assert(rep_->codes_thus_far <= rep_->num);
        }
        return result;
    }

    AppendResult *Column::Write(TupleId pos, const Code *codes, size_t num) {
        size_t block_id = pos / NUM_CODES_PER_BLOCK;
        const Code *data_ptr = codes;

        if (pos + num > rep_->num)
            rep_->num = pos + num;

        AppendResult *result = nullptr;
        while (num > 0) {
            if (block_id >= rep_->vector.size())
                rep_->vector.push_back(NULL);
            assert(block_id < rep_->vector.size());
            if (rep_->vector[block_id] == NULL)
                rep_->vector[block_id] = CreateColumnBlock();

            size_t size = std::min(num,
                                   NUM_CODES_PER_BLOCK - rep_->vector[block_id]->GetNumCodes());
            result = rep_->vector[block_id]->Append(data_ptr, size);
            rep_->max_code_ = std::max(rep_->max_code_, rep_->vector[block_id]->GetMaxCode());

            data_ptr += size;
            num -= size;
            block_id++;

            if (!result->DoesCodeFitInBitwidth()) {
                return result;
            }
        }

        if (result == nullptr) {
            result = new AppendResult(Status::OK(), true, GetBitWidth());
        }
        return result;
    }

    Status Column::Scan(Comparator comparator, Code literal,
                        BitVector &bitvector, BitVectorOpt opt) const {
        Status status;
        for (uint32_t i = 0; i < bitvector.GetNumBitVectorBlock(); i++) {
            if (i < rep_->vector.size() && rep_->vector[i] != NULL) {
                status = rep_->vector[i]->Scan(comparator, literal,
                                               bitvector.GetBitVectorBlock(i), opt);
                if (!status.IsOk())
                    return status;
            } else {
                // The block has not been allocated. All Null values.
                if (opt == kAnd || opt == kSet) {
                    bitvector.GetBitVectorBlock(i).SetZeros();
                }
            }
        }
        return Status::OK();
    }

    Status Column::Scan(Comparator comparator, Column &column,
                        BitVector &bitvector, BitVectorOpt opt) const {
        Status status;
        for (uint32_t i = 0; i < rep_->vector.size(); i++) {
            if (i < rep_->vector.size() && rep_->vector[i] != NULL
                && i < column.rep_->vector.size() && column.rep_->vector[i] != NULL) {
                status = rep_->vector[i]->Scan(comparator, *(column.rep_->vector[i]),
                                               bitvector.GetBitVectorBlock(i), opt);
                if (!status.IsOk())
                    return status;
            } else {
                // The block has not been allocated. All Null values.
                if (opt == kAnd || opt == kSet) {
                    bitvector.GetBitVectorBlock(i).SetZeros();
                }
            }
        }
        return Status::OK();
    }

    Status Column::GetCode(TupleId pos, Code &code) const {
        size_t block_id = pos / NUM_CODES_PER_BLOCK;
        size_t block_code_id = pos % NUM_CODES_PER_BLOCK;
        assert(block_id < rep_->vector.size());
        ColumnBlock *block = rep_->vector[block_id];
        if (block == NULL) {
            code = NullCode;
            return Status::OK();
        }
        return block->GetCode(block_code_id, code);
    }

    Status Column::SetCode(TupleId pos, Code code) {
        size_t block_id = pos / NUM_CODES_PER_BLOCK;
        size_t block_code_id = pos % NUM_CODES_PER_BLOCK;
        assert(block_id < rep_->vector.size());
        ColumnBlock *block = rep_->vector[block_id];
        if (block == NULL) {
            // Lazy allocate block
            rep_->vector[block_id] = block = CreateColumnBlock();
        }
        return block->GetCode(block_code_id, code);
    }

    Status Column::Resize(size_t num) {
        rep_->num = num;

        // Lazy allocate space for blocks
        size_t num_blocks = rep_->num / NUM_CODES_PER_BLOCK;
        // TODO: free space to shrink column
        if (num_blocks > rep_->vector.size()) {
            for (size_t block_id = rep_->vector.size(); block_id < num_blocks; block_id++) {
                rep_->vector.push_back(NULL);
            }
            assert(num_blocks == rep_->vector.size());
        }
        return Status::OK();
    }

    ColumnBlock *Column::GetColumnBlock(size_t column_block_id) const {
        if (column_block_id < rep_->vector.size())
            return rep_->vector[column_block_id];
        else
            return NULL;
    }

// Save data to a file
    Status Column::Save(SequentialWriteFile &file) const {
        Status status;

        status = file.Append(reinterpret_cast<const char *>(&rep_->type), sizeof(ColumnType));
        if (!status.IsOk())
            return status;

        status = file.Append(reinterpret_cast<const char *>(&rep_->num), sizeof(size_t));
        if (!status.IsOk())
            return status;

        for (uint32_t i = 0; i < rep_->vector.size(); i++) {
            rep_->vector[i]->Save(file);
        }

        return Status::OK();
    }

    ColumnBlock *Column::CreateColumnBlock() const {
        switch (rep_->type) {
            case kNaive:
                return new NaiveColumnBlock();
            case kBitWeavingH:
                switch (rep_->bit_width) {
                    case 1:
                        return new BwHColumnBlock<1>();
                    case 2:
                        return new BwHColumnBlock<2>();
                    case 3:
                        return new BwHColumnBlock<3>();
                    case 4:
                        return new BwHColumnBlock<4>();
                    case 5:
                        return new BwHColumnBlock<5>();
                    case 6:
                        return new BwHColumnBlock<6>();
                    case 7:
                        return new BwHColumnBlock<7>();
                    case 8:
                        return new BwHColumnBlock<8>();
                    case 9:
                        return new BwHColumnBlock<9>();
                    case 10:
                        return new BwHColumnBlock<10>();
                    case 11:
                        return new BwHColumnBlock<11>();
                    case 12:
                        return new BwHColumnBlock<12>();
                    case 13:
                        return new BwHColumnBlock<13>();
                    case 14:
                        return new BwHColumnBlock<14>();
                    case 15:
                        return new BwHColumnBlock<15>();
                    case 16:
                        return new BwHColumnBlock<16>();
                    case 17:
                        return new BwHColumnBlock<17>();
                    case 18:
                        return new BwHColumnBlock<18>();
                    case 19:
                        return new BwHColumnBlock<19>();
                    case 20:
                        return new BwHColumnBlock<20>();
                    case 21:
                        return new BwHColumnBlock<21>();
                    case 22:
                        return new BwHColumnBlock<22>();
                    case 23:
                        return new BwHColumnBlock<23>();
                    case 24:
                        return new BwHColumnBlock<24>();
                    case 25:
                        return new BwHColumnBlock<25>();
                    case 26:
                        return new BwHColumnBlock<26>();
                    case 27:
                        return new BwHColumnBlock<27>();
                    case 28:
                        return new BwHColumnBlock<28>();
                    case 29:
                        return new BwHColumnBlock<29>();
                    case 30:
                        return new BwHColumnBlock<30>();
                    case 31:
                        return new BwHColumnBlock<31>();
                    case 32:
                        return new BwHColumnBlock<32>();
                    default:
                        std::cerr << "Code size overflow in creating BitWeaving/H block" << std::endl;
                }
                break;
            case kBitWeavingV:
                switch (rep_->bit_width) {
                    case 1:
                        return new BwVColumnBlock<1>();
                    case 2:
                        return new BwVColumnBlock<2>();
                    case 3:
                        return new BwVColumnBlock<3>();
                    case 4:
                        return new BwVColumnBlock<4>();
                    case 5:
                        return new BwVColumnBlock<5>();
                    case 6:
                        return new BwVColumnBlock<6>();
                    case 7:
                        return new BwVColumnBlock<7>();
                    case 8:
                        return new BwVColumnBlock<8>();
                    case 9:
                        return new BwVColumnBlock<9>();
                    case 10:
                        return new BwVColumnBlock<10>();
                    case 11:
                        return new BwVColumnBlock<11>();
                    case 12:
                        return new BwVColumnBlock<12>();
                    case 13:
                        return new BwVColumnBlock<13>();
                    case 14:
                        return new BwVColumnBlock<14>();
                    case 15:
                        return new BwVColumnBlock<15>();
                    case 16:
                        return new BwVColumnBlock<16>();
                    case 17:
                        return new BwVColumnBlock<17>();
                    case 18:
                        return new BwVColumnBlock<18>();
                    case 19:
                        return new BwVColumnBlock<19>();
                    case 20:
                        return new BwVColumnBlock<20>();
                    case 21:
                        return new BwVColumnBlock<21>();
                    case 22:
                        return new BwVColumnBlock<22>();
                    case 23:
                        return new BwVColumnBlock<23>();
                    case 24:
                        return new BwVColumnBlock<24>();
                    case 25:
                        return new BwVColumnBlock<25>();
                    case 26:
                        return new BwVColumnBlock<26>();
                    case 27:
                        return new BwVColumnBlock<27>();
                    case 28:
                        return new BwVColumnBlock<28>();
                    case 29:
                        return new BwVColumnBlock<29>();
                    case 30:
                        return new BwVColumnBlock<30>();
                    case 31:
                        return new BwVColumnBlock<31>();
                    case 32:
                        return new BwVColumnBlock<32>();
                    default:
                        std::cerr << "Code size overflow in creating BitWeaving/V block" << std::endl;
                }
                break;
        }
        return NULL;
    }

    ColumnIterator *Column::CreateColumnIterator() {
        switch (rep_->type) {
            case kBitWeavingH:
                switch (rep_->bit_width) {
                    case 1:
                        return new BwHColumnIterator<1>(this);
                    case 2:
                        return new BwHColumnIterator<2>(this);
                    case 3:
                        return new BwHColumnIterator<3>(this);
                    case 4:
                        return new BwHColumnIterator<4>(this);
                    case 5:
                        return new BwHColumnIterator<5>(this);
                    case 6:
                        return new BwHColumnIterator<6>(this);
                    case 7:
                        return new BwHColumnIterator<7>(this);
                    case 8:
                        return new BwHColumnIterator<8>(this);
                    case 9:
                        return new BwHColumnIterator<9>(this);
                    case 10:
                        return new BwHColumnIterator<10>(this);
                    case 11:
                        return new BwHColumnIterator<11>(this);
                    case 12:
                        return new BwHColumnIterator<12>(this);
                    case 13:
                        return new BwHColumnIterator<13>(this);
                    case 14:
                        return new BwHColumnIterator<14>(this);
                    case 15:
                        return new BwHColumnIterator<15>(this);
                    case 16:
                        return new BwHColumnIterator<16>(this);
                    case 17:
                        return new BwHColumnIterator<17>(this);
                    case 18:
                        return new BwHColumnIterator<18>(this);
                    case 19:
                        return new BwHColumnIterator<19>(this);
                    case 20:
                        return new BwHColumnIterator<20>(this);
                    case 21:
                        return new BwHColumnIterator<21>(this);
                    case 22:
                        return new BwHColumnIterator<22>(this);
                    case 23:
                        return new BwHColumnIterator<23>(this);
                    case 24:
                        return new BwHColumnIterator<24>(this);
                    case 25:
                        return new BwHColumnIterator<25>(this);
                    case 26:
                        return new BwHColumnIterator<26>(this);
                    case 27:
                        return new BwHColumnIterator<27>(this);
                    case 28:
                        return new BwHColumnIterator<28>(this);
                    case 29:
                        return new BwHColumnIterator<29>(this);
                    case 30:
                        return new BwHColumnIterator<30>(this);
                    case 31:
                        return new BwHColumnIterator<31>(this);
                    case 32:
                        return new BwHColumnIterator<32>(this);
                }
                break;
            case kBitWeavingV:
                switch (rep_->bit_width) {
                    case 1:
                        return new BwVColumnIterator<1>(this);
                    case 2:
                        return new BwVColumnIterator<2>(this);
                    case 3:
                        return new BwVColumnIterator<3>(this);
                    case 4:
                        return new BwVColumnIterator<4>(this);
                    case 5:
                        return new BwVColumnIterator<5>(this);
                    case 6:
                        return new BwVColumnIterator<6>(this);
                    case 7:
                        return new BwVColumnIterator<7>(this);
                    case 8:
                        return new BwVColumnIterator<8>(this);
                    case 9:
                        return new BwVColumnIterator<9>(this);
                    case 10:
                        return new BwVColumnIterator<10>(this);
                    case 11:
                        return new BwVColumnIterator<11>(this);
                    case 12:
                        return new BwVColumnIterator<12>(this);
                    case 13:
                        return new BwVColumnIterator<13>(this);
                    case 14:
                        return new BwVColumnIterator<14>(this);
                    case 15:
                        return new BwVColumnIterator<15>(this);
                    case 16:
                        return new BwVColumnIterator<16>(this);
                    case 17:
                        return new BwVColumnIterator<17>(this);
                    case 18:
                        return new BwVColumnIterator<18>(this);
                    case 19:
                        return new BwVColumnIterator<19>(this);
                    case 20:
                        return new BwVColumnIterator<20>(this);
                    case 21:
                        return new BwVColumnIterator<21>(this);
                    case 22:
                        return new BwVColumnIterator<22>(this);
                    case 23:
                        return new BwVColumnIterator<23>(this);
                    case 24:
                        return new BwVColumnIterator<24>(this);
                    case 25:
                        return new BwVColumnIterator<25>(this);
                    case 26:
                        return new BwVColumnIterator<26>(this);
                    case 27:
                        return new BwVColumnIterator<27>(this);
                    case 28:
                        return new BwVColumnIterator<28>(this);
                    case 29:
                        return new BwVColumnIterator<29>(this);
                    case 30:
                        return new BwVColumnIterator<30>(this);
                    case 31:
                        return new BwVColumnIterator<31>(this);
                    case 32:
                        return new BwVColumnIterator<32>(this);
                }
                break;
            case kNaive:
                return new ColumnIterator(this);
        }
        return new ColumnIterator(this);
    }

} // namespace bitweaving


