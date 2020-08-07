#include "Select.h"

#include <utility>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <table/util.h>
#include <iostream>


namespace hustle {
namespace operators {


void pack(int64_t length, const uint8_t *arr,
          std::shared_ptr<arrow::BooleanArray> *out) {
    auto packed = std::static_pointer_cast<arrow::BooleanArray>(
        arrow::MakeArrayFromScalar(arrow::BooleanScalar(false), length)
            .ValueOrDie());

    uint8_t *packed_arr = packed->values()->mutable_data();

    uint8_t current_byte;
    const uint8_t *src_byte = arr;
    uint8_t *dst_byte = packed_arr;

    int64_t remaining_bytes = length / 8;
    while (remaining_bytes-- > 0) {
        current_byte = 0u;
        current_byte = *src_byte++ ? current_byte | 0x01u : current_byte;
        current_byte = *src_byte++ ? current_byte | 0x02u : current_byte;
        current_byte = *src_byte++ ? current_byte | 0x04u : current_byte;
        current_byte = *src_byte++ ? current_byte | 0x08u : current_byte;
        current_byte = *src_byte++ ? current_byte | 0x10u : current_byte;
        current_byte = *src_byte++ ? current_byte | 0x20u : current_byte;
        current_byte = *src_byte++ ? current_byte | 0x40u : current_byte;
        current_byte = *src_byte++ ? current_byte | 0x80u : current_byte;
        *dst_byte++ = current_byte;
    }

    int64_t remaining_bits = length % 8;
    if (remaining_bits) {
        current_byte = 0;
        uint8_t bit_mask = 0x01;
        while (remaining_bits-- > 0) {
            current_byte = *src_byte++ ? current_byte | bit_mask : current_byte;
            bit_mask = bit_mask << 1u;
        }
        *dst_byte++ = current_byte;
    }

    *out = packed;
}

Select::Select(
    const std::size_t query_id,
    std::shared_ptr<OperatorResult> prev_result,
    std::shared_ptr<OperatorResult> output_result,
    std::shared_ptr<PredicateTree> tree) : Operator(query_id) {

    output_result_ = output_result;
    tree_ = tree;

    auto node = tree_->root_;

    // We can figure out which table we are performing the selection on if we
    // look at the LHS of one of the leaf nodes.
    while (!node->is_leaf()) {
        node = node->left_child_;
    }
    table_ = node->predicate_->col_ref_.table;
}

arrow::Datum
Select::get_filter(const std::shared_ptr<Node> &node,
                   const std::shared_ptr<Block> &block) {

    arrow::Status status;

    if (node->is_leaf()) {
        return get_filter(node->predicate_, block);
    }

    auto left_child_filter = get_filter(node->left_child_, block);
    auto right_child_filter = get_filter(node->right_child_, block);
    arrow::Datum block_filter;

    switch (node->connective_) {
        case AND: {
            auto left_data = left_child_filter.array()->GetMutableValues<uint8_t>(1);
            auto right_data = right_child_filter.array()->GetValues<uint8_t>(1);

            auto num_bits = left_child_filter.length();
            auto num_bytes = num_bits / 8;
            if (num_bits % 8 > 0) ++num_bytes;

            for (int i=0; i<num_bytes; ++i) {
                left_data[i] = left_data[i] & right_data[i];
            }

            block_filter = left_child_filter;
            break;
        }
        case OR: {

            auto left_data = left_child_filter.array()->GetMutableValues<uint8_t>(1);
            auto right_data = right_child_filter.array()->GetValues<uint8_t>(1);

            auto num_bits = left_child_filter.length();
            auto num_bytes = num_bits / 8;
            if (num_bits % 8 > 0) ++num_bytes;

            for (int i=0; i<num_bytes; ++i) {
                left_data[i] = left_data[i] | right_data[i];
            }

            block_filter = left_child_filter;
            break;
        }
        case NONE: {
            block_filter = left_child_filter;
        }
    }
    return block_filter;
}

template<typename Functor>
void Select::for_each_batch(int batch_size, int num_batches, std::shared_ptr<arrow::ArrayVector> filter_vector,
                            const Functor &functor) {
    for (int batch_i = 0; batch_i < num_batches; batch_i++) {
        functor(filter_vector, batch_i, batch_size);
    }
}

void Select::execute(Task *ctx) {

    auto filter_vector = std::make_shared<arrow::ArrayVector>();
    filter_vector->resize(table_->get_num_blocks());
    filters_.resize(table_->get_num_blocks());
    filter_exists_.resize(table_->get_num_blocks());

    ctx->spawnTask(CreateTaskChain(
        // Task 1: perform selection on all blocks
        CreateLambdaTask([this, filter_vector](Task *internal) {

            int batch_size = table_->get_num_blocks() / std::thread::hardware_concurrency();
            if (batch_size == 0) batch_size = table_->get_num_blocks();
            int num_batches = table_->get_num_blocks() / batch_size + 1; // if num_chunks is a multiple of batch_size, we don't actually want the +1
            if (num_batches == 0) num_batches = 1;

            auto batch_functor = [&](auto filter_vector, auto batch_i, auto batch_size) {
                internal->spawnLambdaTask([this, filter_vector, batch_i, batch_size]() {
                    int base_i = batch_i * batch_size;
                    for (int i = base_i; i < base_i + batch_size && i < table_->get_num_blocks(); i++) {
                        execute_block(*filter_vector, i);
                    }
                });
            };

            for_each_batch(batch_size, num_batches, filter_vector, batch_functor);
        }),
        // Task 2: create the output result
        CreateLambdaTask([this, filter_vector](Task *internal) {
            finish(filter_vector, internal);
        })
    ));
}

void Select::execute_block(arrow::ArrayVector &filter_vector, int i) {

    auto block = table_->get_block(i);

    std::shared_ptr<arrow::Buffer> filter_buffer;
    auto status = arrow::AllocateBitmap(block->get_num_rows()).Value(&filter_buffer);
    evaluate_status(status, __FUNCTION__, __LINE__);

//    auto filter_array_data = arrow::ArrayData::Make(arrow::boolean(), block->get_num_rows(), {nullptr, filter_buffer});
//    filters_[i] = arrow::MakeArray(filter_array_data);

    auto block_filter = this->get_filter(tree_->root_, block);
//    filter_vector[i] = block_filter.make_array();
    filters_[i] = block_filter.make_array();

}

void Select::finish(std::shared_ptr<arrow::ArrayVector> filter_vector, Task *ctx) {


    for (auto &a: filters_) {
//        std::cout << a->ToString() << std::endl;

    }
    auto chunked_filter = std::make_shared<arrow::ChunkedArray>(filters_);
    LazyTable lazy_table(table_, chunked_filter, arrow::Datum());
    output_result_->append(lazy_table);
}

arrow::Datum Select::get_filter(
    const std::shared_ptr<Predicate> &predicate,
    const std::shared_ptr<Block> &block) {

    switch(predicate->value_.type()->id()) {

        case arrow::Type::UINT8: {

            uint8_t val = std::static_pointer_cast<arrow::UInt8Scalar>(predicate->value_.scalar())->value;

            switch (predicate->comparator_) {
                case arrow::compute::CompareOperator::LESS: {
                    auto f = [](uint8_t x, uint8_t y) -> bool  {return x < y; };
                    return get_filter<uint8_t>(predicate->col_ref_, f, val, block);
                }
                case arrow::compute::CompareOperator::LESS_EQUAL: {
                    auto f = [](uint8_t x, uint8_t y) -> bool  {return x <= y; };
                    return get_filter<uint8_t>(predicate->col_ref_, f, val, block);
                }
                case arrow::compute::CompareOperator::GREATER: {
                    auto f = [](uint8_t x, uint8_t y) -> bool  {return x > y; };
                    return get_filter<uint8_t>(predicate->col_ref_, f, val, block);
                }
                case arrow::compute::CompareOperator::GREATER_EQUAL: {
                    auto f = [](uint8_t x, uint8_t y) -> bool  {return x >= y; };
                    return get_filter<uint8_t>(predicate->col_ref_, f, val, block);
                }
                case arrow::compute::CompareOperator::EQUAL: {
                    auto f = [](uint8_t x, uint8_t y) -> bool  {return x == y; };
                    return get_filter<uint8_t>(predicate->col_ref_, f, val, block);
                }
                // TODO(nicholas): placeholder for BETWEEN
                case arrow::compute::CompareOperator::NOT_EQUAL: {

                    auto num_rows = block->get_num_rows();
                    auto col_data = block->get_column_by_name(predicate->col_ref_.col_name)->data()->GetValues<uint8_t >(1);

                    auto lo = std::static_pointer_cast<arrow::UInt8Scalar>(predicate->value_.scalar())->value;
                    auto hi = std::static_pointer_cast<arrow::UInt8Scalar>(predicate->value2_.scalar())->value;
                    auto diff = hi - lo;

                    uint8_t bytemap[num_rows];

                    auto f = [](uint8_t val, uint8_t diff) -> bool  {return val <= diff; };

                    for (uint32_t i=0; i<num_rows; ++i) {
                        bytemap[i] = f(col_data[i]-lo, diff);
                    }

                    std::shared_ptr<arrow::BooleanArray> out_filter;
                    pack(num_rows, bytemap, &out_filter);
                    return out_filter;
                }
                default : {
                    std::cerr << "No support for comparator" << std::endl;
                }
            }
            break;
        }
        case arrow::Type::INT64: {
            int64_t val = std::static_pointer_cast<arrow::Int64Scalar>(predicate->value_.scalar())->value;

            switch(predicate->comparator_) {
                case arrow::compute::CompareOperator::LESS: {
                    return get_filter<int64_t>(predicate->col_ref_, std::less(), val, block);
                    break;
                }
                case arrow::compute::CompareOperator::LESS_EQUAL: {
                    std::less_equal op;
                    return get_filter<int64_t>(predicate->col_ref_, op, val, block);
                    break;
                }
                case arrow::compute::CompareOperator::GREATER_EQUAL: {
                    std::greater_equal op;
                    return get_filter<int64_t>(predicate->col_ref_, op, val, block);
                    break;

                }
                case arrow::compute::CompareOperator::EQUAL: {
                    std::equal_to op;
                    return get_filter<int64_t>(predicate->col_ref_, op, val, block);
                    break;
                }
                case arrow::compute::CompareOperator::NOT_EQUAL: {
                    auto num_rows = block->get_num_rows();
                    auto col_data = block->get_column_by_name(predicate->col_ref_.col_name)->data()->GetValues<int64_t >(1);

                    auto lo = std::static_pointer_cast<arrow::Int64Scalar>(predicate->value_.scalar())->value;
                    auto hi = std::static_pointer_cast<arrow::Int64Scalar>(predicate->value2_.scalar())->value;
                    auto diff = hi - lo;

                    uint8_t bytemap[num_rows];

                    auto f = [](uint8_t val, uint8_t diff) -> bool  {return val <= diff; };

                    for (uint32_t i=0; i<num_rows; ++i) {
                        bytemap[i] = f(col_data[i]-lo, diff);
                    }

                    std::shared_ptr<arrow::BooleanArray> out_filter;
                    pack(num_rows, bytemap, &out_filter);
                    return out_filter;
                }
                default : {
                    std::cerr << "No supprt for comparator" << std::endl;
                }
            }
            break;
        }

        default : {
            arrow::Status status;

            arrow::compute::CompareOptions compare_options(predicate->comparator_);
            arrow::Datum block_filter;

            auto select_col = block->get_column_by_name(predicate->col_ref_.col_name);
            auto value = predicate->value_;

            status = arrow::compute::Compare(select_col, value, compare_options).Value(&block_filter);
            evaluate_status(status, __FUNCTION__, __LINE__);

            filters_[block->get_id()] = block_filter.make_array();
            return block_filter;
        }
    }
}


template<typename T, typename Op>
arrow::Datum Select::get_filter(const ColumnReference &col_ref, Op comparator, const T &value, const std::shared_ptr<Block> &block) {

    arrow::Datum block_filter;
    auto num_rows = block->get_num_rows();
    auto col_data = block->get_column_by_name(col_ref.col_name)->data()->GetValues<T>(1);

    uint8_t bytemap[num_rows];

    for (uint32_t i=0; i<num_rows; ++i) {
        bytemap[i] = comparator(col_data[i], value);
    }

    std::shared_ptr<arrow::BooleanArray> out_filter;
    pack(num_rows, bytemap, &out_filter);
    return out_filter;
}




} // namespace operators
} // namespace hustle