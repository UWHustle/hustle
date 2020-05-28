//
// Created by Sandhya Kannan on 5/20/20.
//

#ifndef HUSTLE_BITWEAVING_COMPARE_H
#define HUSTLE_BITWEAVING_COMPARE_H

#include <iostream>
#include <utility>
#include <arrow/compute/api.h>
#include <arrow/api.h>
#include "table.h"

namespace hustle::bitweaving {

struct BitweavingCompareOptionsUnit {
  explicit BitweavingCompareOptionsUnit(std::shared_ptr<bitweaving::Code> scalar,
                                        std::shared_ptr<bitweaving::Comparator> op,
                                        std::shared_ptr<bitweaving::BitVectorOpt> bitvectorOpt) :
      scalar(std::move(scalar)),
      op(std::move(op)), bitvectorOpt(std::move(bitvectorOpt)) {}

  std::shared_ptr<bitweaving::Code> scalar;
  std::shared_ptr<bitweaving::Comparator> op;
  std::shared_ptr<bitweaving::BitVectorOpt> bitvectorOpt;
};

struct BitweavingCompareOptions {
  explicit BitweavingCompareOptions(bitweaving::Column *column) : column(column) {
  }

  BitweavingCompareOptions(bitweaving::Column *column, std::vector<BitweavingCompareOptionsUnit> opts)
      : column(column), opts(std::move(opts)) {}

  bitweaving::Column *column;
  std::vector<BitweavingCompareOptionsUnit> opts;

  void addOpt(const BitweavingCompareOptionsUnit &opt) {
    opts.push_back(opt);
  }

};

inline arrow::Status AllocateBufferMemory(arrow::compute::FunctionContext* ctx, const std::shared_ptr<arrow::DataType>& type,
                                  arrow::compute::Datum* out) {
    DCHECK_EQ(out->kind(), arrow::compute::Datum::ARRAY);
    arrow::ArrayData* data = out->array().get();
    data->buffers.resize(2);

    // Allocate the value buffer
    const int64_t length = data->length;
    std::shared_ptr<arrow::Buffer>* buffer = &(data->buffers[1]);
    std::shared_ptr<arrow::DataType> type_ptr = arrow::boolean();
    if ((*type).id() != arrow::Type::NA) {
        const auto& fw_type = arrow::internal::checked_cast<const arrow::FixedWidthType&>(*type);

        int bit_width = fw_type.bit_width();
        int64_t buffer_size = 0;

        if (bit_width == 1) {
            buffer_size = arrow::BitUtil::BytesForBits(length);
        } else {
            if(bit_width % 8 != 0)
                return arrow::Status::NotImplemented("Only widths multiple of 8", bit_width);
            buffer_size = length * fw_type.bit_width() / 8;
        }
        RETURN_NOT_OK(ctx->Allocate(buffer_size, buffer));

        if (bit_width == 1 && buffer_size > 0) {
            // Some utility methods access the last byte before it might be
            // initialized this makes valgrind/asan unhappy, so we proactively
            // zero it.
            *(buffer->get()->mutable_data() + (buffer->get()->size() - 1)) = 0;
        }
    }

    return arrow::Status::OK();
}

/**
 *
 * @param context
 * @param table
 * @param column
 * @param scalar
 * @param op
 * @param bitvector_opt
 * @param out
 * @return
 */
arrow::Status BitweavingCompare(arrow::compute::FunctionContext *context, BWTable *table, Column *column,
                                const std::shared_ptr<Code> &scalar, std::shared_ptr<Comparator> op,
                                const std::shared_ptr<BitVectorOpt> &bitvector_opt,
                                arrow::compute::Datum *out);

/**
 *
 * @param ctx
 * @param table
 * @param options
 * @param out
 * @return
 */
arrow::Status BitweavingCompare(arrow::compute::FunctionContext *ctx, BWTable *table,
                                std::vector<BitweavingCompareOptions> options, arrow::compute::Datum *out);
}
#endif //HUSTLE_BITWEAVING_COMPARE_H
