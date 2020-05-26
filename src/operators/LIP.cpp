//
// Created by Nicholas Corrado on 5/21/20.
//

#include "LIP.h"
#include "../table/util.h"

namespace hustle::operators {

LIP::LIP(const std::size_t query_id,
         std::shared_ptr<OperatorResult> prev_result,
         std::shared_ptr<OperatorResult> output_result,
         hustle::operators::JoinGraph graph) : Operator(query_id) {

    prev_result_ = std::move(prev_result);
    output_result_ = std::move(output_result);
    graph_ = std::move(graph);
}

void LIP::build_filters() {

    for (int i=0; i<dim_tables_.size(); i++) {

        auto lazy_table = dim_tables_[i];
        auto dim_join_col_name = dim_join_col_names_[i];

        // Pre-materialized and save dim table pk columns.
        auto pk_col = lazy_table.get_column_by_name(dim_join_col_name);
        dim_pk_cols_.emplace(dim_join_col_name, pk_col);

        auto bloom_filter = std::make_shared<BloomFilter>(pk_col->length());

        for (int i=0; i<pk_col->num_chunks(); i++) {
            // TODO(nicholas): For now, we assume the column is of INT64 type.
            auto chunk = std::static_pointer_cast<arrow::Int64Array>(pk_col->chunk(i));

            for (int j=0; j<chunk->length(); j++) {
                auto val = chunk->Value(j);

                bloom_filter->insert(val);
            }
        }

        bloom_filter->set_memory(1000);

        dim_filters_.push_back(bloom_filter);
        dim_join_col_num_chunks_.push_back(pk_col->num_chunks());
    }
}

void LIP::probe_filters() {

    arrow::Status status;
    lip_indices_.resize(fact_table_.table->get_num_blocks());

    // Pre-materialized and save fact table fk columns.
    for (int i=0; i<dim_tables_.size(); i++) {
        auto fact_join_col_name = fact_join_col_names_[i];
        auto fact_fk_col = fact_table_.get_column_by_name(fact_join_col_name);
        fact_fk_cols_.emplace(fact_join_col_name, fact_fk_col);
    }

    // Grab any fact table column so we can pre-compute chunk row offsets.
    auto fact_col = fact_table_.get_column(0);
    std::vector<int64_t> chunk_row_offsets(fact_col->num_chunks());
    chunk_row_offsets[0] = 0;
    for (int i = 1; i < fact_col->num_chunks(); i++) {
        chunk_row_offsets[i] =
            chunk_row_offsets[i - 1] + fact_col->chunk(i - 1)->length();
    }
    
    for (int j=0; j<fact_table_.table->get_num_blocks(); j++) {

        int indices_start = 0;
        int indices_size = fact_col->chunk(j)->length();
        auto* indices = (int64_t*) malloc(indices_size*sizeof(int64_t));

        for (int i=0; i<dim_tables_.size(); i++) {

            auto fact_join_col_name = fact_join_col_names_[i];
            auto fact_fk_col = fact_fk_cols_[fact_join_col_name];

            // TODO(nicholas): For now, we assume the column is of INT64 type
            auto chunk = fact_fk_col->chunk(j);
            auto chunk_data = fact_fk_col->chunk(j)->data()->GetValues<int64_t>(1, 0);

            auto bloom_filter = dim_filters_[i];

            if (i==0) {
                for (int row = 0; row < chunk->length(); row++) {

                    auto key = chunk_data[row];

                    if (bloom_filter->probe(key)) {
                        indices[indices_start++] = (row + chunk_row_offsets[j]);
                    }
                    else {
                        indices_size--;
                    }
                }
            }
            else {
                std::vector<int64_t> test(indices, indices+indices_size);
                for (int m=0; m<indices_size; m++) {

                    auto index = indices[m];
                    auto key = chunk_data[index - chunk_row_offsets[j]];

                    if (!bloom_filter->probe(key)) {
                        auto to_remove = indices[m];
                        indices[m] = indices[indices_size-1];
                        indices[indices_size-1] = to_remove;
                        indices_size--;
                    }
                }
            }
        }
        std::vector<int64_t> v(indices, indices+indices_size);
        lip_indices_[j] = v;
    }
}

std::shared_ptr<arrow::ArrayData> LIP::make_empty_filter(int num_bits) {
    auto result = arrow::AllocateResizableBuffer(num_bits/8 + 1);
    evaluate_status(result.status(), __FUNCTION__, __LINE__);
    std::shared_ptr<arrow::ResizableBuffer> valid_buffer = std::move(result.ValueOrDie());
    uint8_t *mutable_data = valid_buffer->mutable_data();
    for (int i=0; i<valid_buffer->size(); i++) {
        mutable_data[i] = ~0u;
    }

    return arrow::ArrayData::Make(arrow::boolean(),num_bits,{nullptr,valid_buffer});
}

void LIP::finish() {
    arrow::Status status;
    arrow::Int64Builder new_indices_builder;
    std::shared_ptr<arrow::Int64Array> new_indices;

    // Append all of the indices to an ArrayBuilder.
    for (int i = 0; i < lip_indices_.size(); i++) {
        status = new_indices_builder.AppendValues(lip_indices_[i]);
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
    }

    // Construct left and right index arrays.
    status = new_indices_builder.Finish(&new_indices);
    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);


    LazyTable result_unit(fact_table_.table, fact_table_.filter, new_indices);
    OperatorResult result({result_unit});
    output_result_->append(std::make_shared<OperatorResult>(result));

    for (int i=0; i<dim_tables_.size(); i++) {
        output_result_->append(dim_tables_[i]);
    }
}

void LIP::execute(Task *ctx) {
    // TODO(nicholas): for now, we assume that there is no need to backpropogate
    //  the LIP result.

    // To handle a variable number of joins, we must store the tasks beforehand. The variadic CreateTaskChain
    // cannot help us here!
    std::vector<Task *> tasks;

    std::vector<std::string> left_col_names;
    std::vector<std::string> right_col_names;

    // TODO(nicholas): For now, we assume joins have simple predicates
    //   without connective operators.
    auto predicates = graph_.get_predicates(0);

    // Loop over the join predicates and store the left/right LazyTables and the
    // left/right join column names
    for (auto &jpred : predicates) {

        auto left_ref = jpred.left_col_ref_;
        auto right_ref = jpred.right_col_ref_;

        fact_join_col_names_.push_back(left_ref.col_name);
        dim_join_col_names_.push_back(right_ref.col_name);

        // The previous result prev may contain many LazyTables. Find the
        // LazyTables that we want to join.
        for (int i = 0; i < prev_result_->lazy_tables_.size(); i++) {
            auto lazy_table = prev_result_->get_table(i);

            if (left_ref.table == lazy_table.table) {
                fact_table_ = lazy_table; // left table is always the same
            } else if (right_ref.table == lazy_table.table) {
                dim_tables_.push_back(lazy_table);
            }
        }
    }

    ctx->spawnLambdaTask([&]{
        build_filters();
        probe_filters();
        finish();
    });
}


}