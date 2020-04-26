//
// Created by Nicholas Corrado on 4/5/20.
//

#include "Operator.h"

#include <utility>
#include <iostream>
#include "table/util.h"

namespace hustle::operators {


    OperatorResult::OperatorResult(
            std::vector<LazyTable> lazy_tables) {

        lazy_tables_ = std::move(lazy_tables);
    }

    OperatorResult::OperatorResult() = default;

    void OperatorResult::append(std::shared_ptr<Table> table) {
        LazyTable lazy_table(
                std::move(table),
                arrow::compute::Datum(),
                arrow::compute::Datum());
        lazy_tables_.insert(lazy_tables_.begin(),lazy_table);
    }

    void OperatorResult::append(const std::shared_ptr<OperatorResult>& result) {
        for (auto &lazy_table : result->lazy_tables_) {
            lazy_tables_.push_back(lazy_table);
        }
    }

    LazyTable OperatorResult::get_table(int i) {
        return lazy_tables_[i];
    }

    LazyTable OperatorResult::get_table(const std::shared_ptr<Table>& table) {
        LazyTable result;
        for (auto & lazy_table : lazy_tables_) {
            if (lazy_table.table == table) {
                result = lazy_table;
                break;
            }
        }
        return result;
    }
    
    std::shared_ptr<Table>
    OperatorResult::materialize(const std::vector<ColumnReference>& col_refs) {

        arrow::Status status;
        arrow::SchemaBuilder schema_builder;
        std::vector<std::shared_ptr<arrow::ChunkedArray>> out_cols;

        for (auto &col_ref : col_refs) {

            auto table = col_ref.table;
            auto col_name = col_ref.col_name;

            status = schema_builder.AddField(
                    table->get_schema()->GetFieldByName(col_name));
            evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

            auto col = get_table(table).get_column_by_name(col_name);
            out_cols.push_back(col);
        }

        status = schema_builder.Finish().status();
        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
        auto out_schema = schema_builder.Finish().ValueOrDie();

        auto out_table = std::make_shared<Table>("out", out_schema,
                                                 BLOCK_SIZE);


        std::vector<std::shared_ptr<arrow::ArrayData>> out_block_data;

        // TODO(nicholas): Create Table constuctor that accepts a vector
        //  of ChunkedArrays.
        for (int i = 0;
             i < out_cols[0]->num_chunks(); i++) {
            for (auto &col : out_cols) {
                out_block_data.push_back(col->chunk(i)->data());
            }
            out_table->insert_records(out_block_data);
            out_block_data.clear();
        }

        return out_table;
    }

}