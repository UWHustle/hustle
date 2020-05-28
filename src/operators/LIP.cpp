//
// Created by Nicholas Corrado on 5/21/20.
//

#include "LIP.h"
#include "../table/util.h"

namespace hustle::operators {

LIP::LIP(const std::size_t query_id,
         std::vector<std::shared_ptr<OperatorResult>> prev_result,
         std::shared_ptr<OperatorResult> output_result,
         hustle::operators::JoinGraph graph) : Operator(query_id) {

    prev_result_ = std::make_shared<OperatorResult>();
    prev_result_vec_ = prev_result;
    output_result_ = std::move(output_result);
    graph_ = std::move(graph);
}

void LIP::build_filters(Task* ctx) {

    dim_filters_.resize(dim_tables_.size());
    dim_join_col_num_chunks_.resize(dim_tables_.size());
//    dim_pk_cols_.resize(dim_tables_.size());

    for (int i=0; i<dim_tables_.size(); i++) {

        ctx->spawnTask(CreateLambdaTask([this, i]() {
            auto lazy_table = dim_tables_[i];
            auto dim_join_col_name = dim_join_col_names_[i];

            // Pre-materialized and save dim table pk columns.
            auto pk_col = lazy_table.get_column_by_name(dim_join_col_name);
//            dim_pk_cols_[i] = pk_col;

            auto bloom_filter = std::make_shared<BloomFilter>(pk_col->length());

            for (int j=0; j<pk_col->num_chunks(); j++) {
                // TODO(nicholas): For now, we assume the column is of INT64 type.
                auto chunk = std::static_pointer_cast<arrow::Int64Array>(pk_col->chunk(j));

                for (int k=0; k<chunk->length(); k++) {
                    auto val = chunk->Value(k);

                    bloom_filter->insert(val);
                }
            }

            bloom_filter->set_memory(1000);

            dim_filters_[i] = (bloom_filter);
            dim_join_col_num_chunks_[i] = (pk_col->num_chunks());
        }));
    }
}

void LIP::probe_filters(int chunk_i) {
//    auto batch_probe_task = CreateLambdaTask([this, block_i, batch_i, fact_col, batch_size, chunk_row_offsets]() {

        std::vector<std::vector<int64_t>> indices(dim_tables_.size());

        for (int filter_j = 0; filter_j < dim_tables_.size(); filter_j++) {

            auto fact_join_col_name = fact_join_col_names_[filter_j];
            auto fact_fk_col = fact_fk_cols_[fact_join_col_name];

            // TODO(nicholas): For now, we assume the column is of INT64 type
            auto chunk = fact_fk_col->chunk(chunk_i);
            auto chunk_data = fact_fk_col->chunk(
                chunk_i)->data()->GetValues<int64_t>(1, 0);

            auto bloom_filter = dim_filters_[filter_j];

            if (filter_j == 0) {
                // Reserve space for the first index vector
                indices[0].reserve(fact_fk_col->chunk(chunk_i)->length()*batch_size_);

                for (int row = 0; row < chunk->length(); row++) {
                    auto key = chunk_data[row];

                    if (bloom_filter->probe(key)) {
                        indices[0].push_back(
                            row + chunk_row_offsets_[chunk_i]);
                    }
                }
            } else {
                // Reserve space for the next index vector
                indices[filter_j].reserve(indices[filter_j - 1].size()*batch_size_);

                for (auto &index : indices[filter_j - 1]) {

                    auto key = chunk_data[index -
                                          chunk_row_offsets_[chunk_i]];

                    if (bloom_filter->probe(key)) {
                        indices[filter_j].push_back(index);
                    }
                }
            }
        }
        lip_indices_[chunk_i] = indices[dim_tables_.size() - 1];
}

void LIP::probe_filters(Task *ctx) {

    arrow::Status status;

    // Pre-materialized and save fact table fk columns.
    for (int i=0; i<dim_tables_.size(); i++) {
        auto fact_join_col_name = fact_join_col_names_[i];
        auto fact_fk_col = fact_table_.get_column_by_name(fact_join_col_name);
        fact_fk_cols_.emplace(fact_join_col_name, fact_fk_col);
    }

    // Grab any fact table column so we can pre-compute chunk row offsets.
    auto fact_col = fact_table_.get_column(0);
    chunk_row_offsets_.resize(fact_col->num_chunks());
    chunk_row_offsets_[0] = 0;
    for (int i = 1; i < fact_col->num_chunks(); i++) {
        chunk_row_offsets_[i] =
            chunk_row_offsets_[i - 1] + fact_col->chunk(i - 1)->length();
    }

    batch_size_ = 1;
    lip_indices_.resize(fact_table_.table->get_num_blocks());

    int num_batches = fact_table_.table->get_num_blocks()/batch_size_ + 1;

    std::vector<Task *> tasks;

    for (int batch_i=0; batch_i<num_batches; batch_i+=batch_size_) {

        ctx->spawnTask(CreateLambdaTask([this, batch_i](Task *internal) {
           for (int block_j=0; block_j<batch_size_ && batch_i+block_j<fact_table_.table->get_num_blocks(); block_j++) {
               internal->spawnLambdaTask([this, batch_i, block_j] {
                   probe_filters(batch_i + block_j);
               });
           }
        }));

            for (auto &bloom_filter: dim_filters_) bloom_filter->update();
            // TODO(nicholas): This sorts the filters while other batches are still being probed!
            //   Threads will be stepping on each other!
//            std::sort(dim_filters_.begin(), dim_filters_.end(), BloomFilter::compare);
    }
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

    for (auto &result : prev_result_vec_) {
        prev_result_->append(result);
    }
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

    ctx->spawnTask(CreateTaskChain(
        CreateLambdaTask(
        [this](Task *internal) {
            build_filters(internal);
        }),
        CreateLambdaTask(
        [this](Task *internal) {
            probe_filters(internal);
        }),
        CreateLambdaTask(
            [this]() {
                finish();
            })
    ));
}


}