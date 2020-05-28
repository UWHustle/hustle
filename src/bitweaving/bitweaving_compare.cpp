//
// Created by Sandhya Kannan on 5/20/20.
//

#include "bitweaving_compare.h"
#include <arrow/compute/api.h>
#include "table.h"

namespace hustle::bitweaving{

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
