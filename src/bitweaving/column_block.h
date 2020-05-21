// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_SRC_COLUMN_BLOCK_H_
#define BITWEAVING_SRC_COLUMN_BLOCK_H_

#include "types.h"
#include "bitvector_block.h"
#include "column.h"
#include "file.h"

namespace hustle::bitweaving {

    class BitVector;

    class BitVectorBlock;

    class Column;

    class AppendResult {
    private:
        Status status_;
        bool is_bitwidth_optimum_;
        int suggested_bitwidth_;

    public:
        AppendResult(){};

        AppendResult(Status status, bool is_bitwidth_optimum, int suggested_bitwidth){
            this->status_ = status;
            this->is_bitwidth_optimum_ = is_bitwidth_optimum;
            this->suggested_bitwidth_ = suggested_bitwidth;
        }

        bool DoesCodeFitInBitwidth(){
            return is_bitwidth_optimum_;
        }

        int GetSuggestedBitwidth(){
            return suggested_bitwidth_;
        }

        Status GetAppendStatus(){
            return status_;
        }
    };

/**
 * @brief Class for storing and accessing codes in a fixed-length column block.
 */
    class ColumnBlock {
    public:
        virtual ~ColumnBlock() {}

        /**
         * @brief Get the type of this column block. Naive column, BitWeaving/H
         * column, or BitWeaving/V column.
         * @return The type of this column block.
         */
        ColumnType GetColumnType() { return type_; }

        /**
         * @brief Get the size of a code in this column block.
         * @return The code size
         */
        size_t GetBitWidth() { return bit_width_; }

        /**
         * @brief Get the number of codes in this column block.
         * @return The number of codes.
         */
        size_t GetNumCodes() { return num_; }

        /**
        * @brief Get the max code in this column.
        * @return The max code
        */
        size_t GetMaxCode() { return max_code_; }

        /**
         * @brief Load an set of codes into this column block. Each code in this array
         * should be in the domain of the codes, i.e. the number of bits that are
         * used to represent each code should be less than or equal to GetBitWidth().
         * @param codes The starting address of the input code array.
         * @param num The number of codes in the input code array.
         * @return A Status object to indicate success or failure.
         */
        virtual AppendResult* Append(const Code *codes, size_t num) = 0;

        /**
         * @brief Load this column block from the file system.
         * @param file The name of data file.
         * @return A Status object to indicate success or failure.
         */
        virtual Status Load(SequentialReadFile &file) = 0;

        /**
         * @brief Save this column block to the file system.
         * @param file The name of data file.
         * @return A Status object to indicate success or failure.
         */
        virtual Status Save(SequentialWriteFile &file) = 0;

        /**
         * @brief Scan this column block with an input literal-based predicate. The scan results
         * are combined with the input bit-vector block based on a logical operator.
         * @param comparator The comparator between column codes and the literal.
         * @param code The code to be compared with.
         * @param bitvector The bit-vector block to be updated based on scan results.
         * @param opt The operator indicates how to combine the scan results to the input
         * bit-vector block.
         * @return A Status object to indicate success or failure.
         */
        virtual Status Scan(Comparator comparator, Code code,
                            BitVectorBlock &bitvector, BitVectorOpt opt = kSet) = 0;

        /**
         * @brief Scan this column block with an predicate that compares this column block with
         * another column block. The scan results are combined with the input bit-vector block
         * based on a logical operator.
         * @param comparator The comparator between column codes and the literal.
         * @param column_block The column block to be compared with.
         * @param bitvector The bit-vector block to be updated based on scan results.
         * @param opt The operator indicates how to combine the scan results to the input
         * bit-vector block.
         * @return A Status object to indicate success or failure.
         */
        virtual Status Scan(Comparator comparator, ColumnBlock &column_block,
                            BitVectorBlock &bitvector, BitVectorOpt opt = kSet) = 0;

        /**
         * @brief Get a code at a given position.
         * @warning This function is used for debugging. It is much more efficient to use
         *  Iterator::GetCode.
         * @param pos The code position to get.
         * @param code The code value.
         * @return A Status object to indicate success or failure.
         */
        virtual Status GetCode(TupleId pos, Code &code) = 0;

        /**
         * @brief Set a code at a given position.
         * @warning This function is used for debugging. It is much more efficient to use
         *  Iterator::SetCode.
         * @param pos The code position to set.
         * @param code The code value.
         * @return A Status object to indicate success or failure.
         */
        virtual Status SetCode(TupleId pos, Code code) = 0;

    protected:
        ColumnBlock(ColumnType type, size_t bit_width) :
                type_(type), bit_width_(bit_width), num_(0) {}

        /**
         * @brief The type of this column block.
         */
        ColumnType type_;
        /**
         * @brief The size of a code in this column block.
         */
        size_t bit_width_;
        /**
         * @brief The number of codes in this column block.
         */
        size_t num_;

        /**
         * @brief The max code value in the column block. Used to track the required bitwidth
         */
        size_t max_code_;
    };

} // namespace bitweaving

#endif // BITWEAVING_SRC_COLUMN_BLOCK_H_
