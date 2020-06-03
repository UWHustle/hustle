//
// Created by Sandhya Kannan on 5/20/20.
//

#include "bitweaving_compare.h"
#include <arrow/compute/api.h>
#include "table.h"

namespace hustle::bitweaving{

std::shared_ptr<bitweaving::Comparator> GetBitweavingCompareOperator(arrow::compute::CompareOperator op){
  std::shared_ptr<bitweaving::Comparator> comp_op;
  switch(op){
    case arrow::compute::CompareOperator::LESS:
      comp_op = std::make_shared<bitweaving::Comparator>(bitweaving::kLess);
      break;

    case arrow::compute::CompareOperator::GREATER:
      comp_op = std::make_shared<bitweaving::Comparator>(bitweaving::kGreater);
      break;

    case arrow::compute::CompareOperator::LESS_EQUAL:
      comp_op = std::make_shared<bitweaving::Comparator>(bitweaving::kLessEqual);
      break;

    case arrow::compute::CompareOperator::GREATER_EQUAL:
      comp_op = std::make_shared<bitweaving::Comparator>(bitweaving::kGreaterEqual);
      break;

    case arrow::compute::CompareOperator::EQUAL:
      comp_op = std::make_shared<bitweaving::Comparator>(bitweaving::kEqual);
      break;

    case arrow::compute::CompareOperator::NOT_EQUAL:
      comp_op = std::make_shared<bitweaving::Comparator>(bitweaving::kInequal);
      break;

    default:
      break;
  }
  return comp_op;
}

arrow::Status AllocateBufferMemory(arrow::compute::FunctionContext* ctx, const std::shared_ptr<arrow::DataType>& type,
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

    arrow::Status BitweavingCompare(arrow::compute::FunctionContext *ctx, BWTable *table, Column *column,
                                    const std::shared_ptr<Code>& scalar, std::shared_ptr<Comparator> op,
                                    const std::shared_ptr<BitVectorOpt>& bitvector_opt,
                                    arrow::compute::Datum* out) {
//            auto pre_start = std::chrono::high_resolution_clock::now();

        out->value = arrow::ArrayData::Make(arrow::boolean(), table->GetNumRows());
        RETURN_NOT_OK(AllocateBufferMemory(ctx, arrow::boolean(), out));

        // Create bitvector
        BitVector *bitvector = table->CreateBitVector();
        auto start = std::chrono::high_resolution_clock::now();
        column->Scan(*op, *scalar , *bitvector, *bitvector_opt);

        auto end = std::chrono::high_resolution_clock::now();
        std::cout << "Bitweaving scan execution time = " <<
                  std::chrono::duration_cast<std::chrono::milliseconds>
                          (end - start).count() << "ms" << std::endl;

        //std::cout << "Result bitvector " << std::endl << bitvector->ToString() << std::endl;

        // Create an iterator to get matching values
        Iterator *iter = table->CreateIterator(*bitvector) ;

        iter->FillDataIntoArrowFormat(out->array());

        auto copy_end = std::chrono::high_resolution_clock::now();
        std::cout << "Bitweaving copy execution time = " <<
                  std::chrono::duration_cast<std::chrono::milliseconds>
                          (copy_end - end).count()<< "ms" << std::endl;

        //cleanup
        delete iter;
        delete bitvector;

        return arrow::Status::OK();
    }


    arrow::Status BitweavingCompare(arrow::compute::FunctionContext *ctx, BWTable *table,
                                    std::vector<BitweavingCompareOptions> options, arrow::compute::Datum *out) {

        out->value = arrow::ArrayData::Make(arrow::boolean(), table->GetNumRows());
        RETURN_NOT_OK(AllocateBufferMemory(ctx, arrow::boolean(), out));

        // Create bitvector
        BitVector *bitvector = table->CreateBitVector();

        //Scan
        auto start = std::chrono::high_resolution_clock::now();

        for(auto & option : options){
            assert(option.column != nullptr);
            for(size_t j=0; j<option.opts.size(); j++){
                option.column->Scan(*option.opts[j].op, *option.opts[j].scalar,
                                    *bitvector, *option.opts[j].bitvectorOpt);
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::cout << "Bitweaving scan execution time = " <<
                  std::chrono::duration_cast<std::chrono::milliseconds>
                          (end - start).count() << "ms" << std::endl;

        // Create an iterator to get matching values
        Iterator *iter = table->CreateIterator(*bitvector) ;
        iter->FillDataIntoArrowFormat(out->array());

        auto copy_end = std::chrono::high_resolution_clock::now();
        std::cout << "Bitweaving copy execution time = " <<
                  std::chrono::duration_cast<std::chrono::milliseconds>
                          (copy_end - end).count()<< "ms" << std::endl;

        //cleanup
        delete iter;
        delete bitvector;

        auto cleanup = std::chrono::high_resolution_clock::now();
        std::cout << "Bitweaving clean execution time = " <<
                  std::chrono::duration_cast<std::chrono::milliseconds>
                          (cleanup - copy_end).count() << "ms" << std::endl;

        return arrow::Status::OK();
    }
}
