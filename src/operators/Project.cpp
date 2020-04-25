//
// Created by Nicholas Corrado on 3/24/20.
//

#include "Project.h"

#include <utility>
#include "../table/util.h"


namespace hustle {
    namespace operators {

        Projection::Projection(std::shared_ptr<OperatorResult> prev_result,
                               std::vector<ColumnReference> col_refs) {

            prev_result_ = std::move(prev_result);
            col_refs_ = std::move(col_refs);
        }
        std::shared_ptr<Table> hustle::operators::Projection::project() {

            arrow::Status status;
            arrow::SchemaBuilder schema_builder;
            std::vector<std::shared_ptr<arrow::ChunkedArray>> out_cols;

            for (auto &col_ref : col_refs_) {

                auto table = col_ref.table;
                auto col_name = col_ref.col_name;

                status = schema_builder.AddField(
                        table->get_schema()->GetFieldByName(col_name));
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
                
                auto col = prev_result_->get_table(table).get_column_by_name(col_name);
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

        std::shared_ptr<OperatorResult> Projection::run() {
            std::vector<LazyTable> units;
            OperatorResult result({units});
            return std::make_shared<OperatorResult>(result);
        }




    }
}
