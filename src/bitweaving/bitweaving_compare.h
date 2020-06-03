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

/**
 *
 * @param ctx
 * @param type
 * @param out
 * @return
 */
arrow::Status AllocateBufferMemory(arrow::compute::FunctionContext* ctx, const std::shared_ptr<arrow::DataType>& type,
                                  arrow::compute::Datum* out);

/**
 *
 * @param op
 * @return
 */
std::shared_ptr<bitweaving::Comparator> GetBitweavingCompareOperator(arrow::compute::CompareOperator op);

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
