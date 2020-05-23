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

        auto pk_col = lazy_table.get_column_by_name(dim_join_col_name);
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

    for (int i=0; i<dim_tables_.size(); i++) {

        auto fact_join_col_name = fact_join_col_names_[i];
        auto fact_fk_col = fact_table_.get_column_by_name(fact_join_col_name);
        auto bloom_filter = dim_filters_[i];

        for (int j=0; j<fact_fk_col->num_chunks(); i++) {

            // TODO(nicholas): For now, we assume the column is of INT64 type
            auto chunk = std::static_pointer_cast<arrow::Int64Array>(
                fact_fk_col->chunk(i));

            arrow::BooleanBuilder filter_builder;
//            status = filter_builder.AppendValues(fact_fk_col->length(), false);
            status = filter_builder.Reserve(fact_fk_col->length());
            evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

            for (int row = 0; row < chunk->length(); row++) {
                auto key = chunk->Value(j);

                if (bloom_filter->probe(key)) {
                    status = filter_builder.Append(true);
                    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
                }
                else{
                    status = filter_builder.Append(false);
                    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
                }
            }

            std::shared_ptr<arrow::Array> filter;
            status = filter_builder.Finish(&filter);
            evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

            fact_filter_vec_[i] = filter;
        }
    }
}

void LIP::finish() {

    auto chunked_filter = std::make_shared<arrow::ChunkedArray>(fact_filter_vec_);
    arrow::compute::Datum filter(chunked_filter);
    LazyTable result_unit(fact_table_.table, filter, fact_table_.indices);
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