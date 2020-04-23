//
// Created by Nicholas Corrado on 3/24/20.
//

#include "Project.h"
#include "../table/util.h"


namespace hustle {
    namespace operators {


        hustle::operators::Projection::Projection(
                std::vector<ProjectionUnit> projection_units) {
            projection_lazy_tables = projection_units;
        }

        std::shared_ptr<Table> hustle::operators::Projection::project() {

            arrow::Status status;

            arrow::SchemaBuilder schema_builder;

            arrow::compute::FunctionContext function_context(
                    arrow::default_memory_pool());
            arrow::compute::TakeOptions take_options;
            arrow::compute::FilterOptions filter_options;
            std::shared_ptr<arrow::ChunkedArray> out_col;

            std::vector<std::shared_ptr<arrow::ChunkedArray>> out_table_data;
            // TODO(nicholas): for now, assume that the selections are always arrays
            //  of indices, not filters.
            for (auto &unit : projection_lazy_tables) {

                auto table = unit.ref.table;
                auto filter = unit.ref.filter;
                auto selection = unit.ref.indices;
                auto fields = unit.fields;

                status = schema_builder.AddFields(fields);
                evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

                for (auto &field : fields) {
                    auto col = table->get_column_by_name(field->name());

                    if (filter.kind() == arrow::compute::Datum::CHUNKED_ARRAY) {
                        arrow::compute::Datum datum_col;
                        status = arrow::compute::Filter(&function_context,
                                                        col,
                                                        filter.chunked_array(),
                                                        filter_options,
                                                        &datum_col);
                        evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
                        col = datum_col.chunked_array();
                    }


                    status = arrow::compute::Take(&function_context,
                                                  *col,
                                                  *selection.make_array(),
                                                  take_options,
                                                  &col);
                    evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);

                    out_table_data.push_back(col);
                }
            }

            status = schema_builder.Finish().status();
            evaluate_status(status, __PRETTY_FUNCTION__, __LINE__);
            auto out_schema = schema_builder.Finish().ValueOrDie();

            auto out_table = std::make_shared<Table>("out", out_schema,
                                                     BLOCK_SIZE);


            std::vector<std::shared_ptr<arrow::ArrayData>> out_block_data;

            for (int chunk_i = 0;
                 chunk_i < out_table_data[0]->num_chunks(); chunk_i++) {
                for (auto &col : out_table_data) {
                    out_block_data.push_back(col->chunk(chunk_i)->data());
                }
                out_table->insert_records(out_block_data);
                out_block_data.clear();
            }

            return out_table;
        }

        std::shared_ptr<OperatorResult> run() {
            std::vector<LazyTable> units;
            OperatorResult result({units});
            return std::make_shared<OperatorResult>(result);
        }

    }
}
