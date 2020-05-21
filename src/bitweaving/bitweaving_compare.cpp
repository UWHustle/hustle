//
// Created by Sandhya Kannan on 5/20/20.
//

#include "bitweaving_compare.h"
#include <arrow/compute/api.h>
#include "table.h"

namespace hustle::bitweaving{
    arrow::Status BitweavingCompare(arrow::compute::FunctionContext *context, Table *table, Column *column,
                             const std::shared_ptr<Code>& scalar, std::shared_ptr<Comparator> op,
                             const std::shared_ptr<BitVectorOpt>& bitvector_opt,
                             arrow::compute::Datum* out) {
//            auto pre_start = std::chrono::high_resolution_clock::now();

        out->value = arrow::ArrayData::Make(arrow::boolean(), table->GetNumRows());
        //Status status = arrow::compute::detail::AllocateValueBufferInDatum(context, arrow::boolean(), out);

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


    arrow::Status BitweavingCompare(arrow::compute::FunctionContext *ctx, Table *table,
            std::vector<BitweavingCompareOptions> options, arrow::compute::Datum *out) {

        out->value = arrow::ArrayData::Make(arrow::boolean(), table->GetNumRows());
        //Status status = arrow::compute::detail::AllocateValueBufferInDatum(ctx, boolean(), out);
        DCHECK_EQ(out->kind(), arrow::compute::Datum::ARRAY);
        arrow::ArrayData* result = out->array().get();
        result->buffers.resize(2);

        // Allocate the value buffer
        const int64_t length = result->length;
        std::shared_ptr<arrow::Buffer>* buffer = &(result->buffers[1]);
        std::shared_ptr<arrow::DataType> type_ptr = arrow::boolean();
        const arrow::DataType& type = *type_ptr;
        if (type.id() != arrow::Type::NA) {
            const auto& fw_type = arrow::internal::checked_cast<const arrow::FixedWidthType&>(type);

            int bit_width = fw_type.bit_width();
            int64_t buffer_size = 0;

            if (bit_width == 1) {
                buffer_size = arrow::BitUtil::BytesForBits(length);
            } else {
                ARROW_CHECK_EQ(bit_width % 8, 0)
                << "Only bit widths with multiple of 8 are currently supported";
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
