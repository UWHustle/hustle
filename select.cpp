#include <iostream>
#include <arrow/api.h>

#include <arrow/compute/api.h>
#include <arrow/stl.h>
#include <gandiva/projector.h>
#include <gandiva/tree_expr_builder.h>

void EvaluateStatus(const arrow::Status& status, const char* function_name, int line_no) {
    if (!status.ok()) {
        std::cout << "Invalid status: " << function_name << ", line " << line_no << ": " << status.message() << std::endl;
    }
}
std::string get_operator_name (arrow::compute::CompareOperator op)
{
    switch (op)
    {
        case arrow::compute::CompareOperator::EQUAL :
            return "equals";

        case arrow::compute::CompareOperator::NOT_EQUAL :
            return "not_equal";
        case arrow::compute::CompareOperator::GREATER :
            return "greater_than";
        case arrow::compute::CompareOperator::GREATER_EQUAL :
            return "greater_than_or_equal_to";
        case arrow::compute::CompareOperator::LESS :
            return "less_than";
        case arrow::compute::CompareOperator::LESS_EQUAL :
            return "less_than_or_equal_to";
        default:
            return "INVALID";
    };
}
std::shared_ptr<arrow::Table> select (
        std::shared_ptr<arrow::Table> in_table,
        std::string select_column,
        arrow::compute::CompareOperator op,
        int32_t literal)
{
    std::shared_ptr<arrow::RecordBatch> in_batch;
    std::unique_ptr<arrow::RecordBatchBuilder> in_batch_builder;
    arrow::Status status = arrow::RecordBatchBuilder::Make(in_table->schema(), arrow::default_memory_pool(), &in_batch_builder);
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
    std::unique_ptr<arrow::RecordBatchBuilder> out_batch_builder;   // to build a RecordBatch from a vector of arrays
    std::vector<std::shared_ptr<arrow::RecordBatch>> out_batches;
    auto* reader = new arrow::TableBatchReader(*in_table);
    while (reader->ReadNext(&in_batch).ok() && in_batch != nullptr)
    {
        auto _field = in_table->schema()->GetFieldByName(select_column);
        auto _schema = in_table->schema();
        auto field_result = field("res", arrow::boolean());
        auto data_type = _field->type();
        auto node_field = gandiva::TreeExprBuilder::MakeField(_field);
        auto node_literal = gandiva::TreeExprBuilder::MakeLiteral(literal);
        std::string op_name = get_operator_name(op);
        std::cout<<op_name<<std::endl;
        auto node_root = gandiva::TreeExprBuilder::MakeFunction (op_name, {node_field, node_literal}, arrow::boolean());
        auto expr = gandiva::TreeExprBuilder::MakeExpression(node_root, field_result);
        std::shared_ptr<gandiva::Projector> projector;
        auto config = gandiva::ConfigurationBuilder().DefaultConfiguration();
        status = gandiva::Projector::Make(_schema, {expr}, config, &projector);
        EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
        arrow::ArrayVector outputs;
        arrow::MemoryPool* pool_;
        status = projector->Evaluate(*in_batch, pool_, &outputs);
        EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);
        auto bool_arr = std::static_pointer_cast<arrow::BooleanArray>(outputs.at(0));
//        for(int i=0; i<bool_arr->length();i++)
//        {
//            if(bool_arr->Value(i)==true)
//            {
//                in_batch.operator*().Slice(i);
//            }
//        }
        auto* out = new arrow::compute::Datum();
        arrow::compute::FunctionContext function_context(arrow::default_memory_pool());
        std::vector<std::shared_ptr<arrow::ArrayData>> out_arrays;      // vector of arrays corresponding to outputted columns in a given batch


        // For each column, store all values that pass the filter
        for (int i=0; i<in_batch->schema()->num_fields(); i++) {
            status = arrow::compute::Filter(&function_context, in_batch->column(i), outputs.at(0), out);
            out_arrays.push_back(out->array());
        }
        auto out_batch = arrow::RecordBatch::Make(in_batch->schema(),out_arrays[0]->length,out_arrays);
        out_batches.push_back(out_batch);

    }
    std::shared_ptr<arrow::Table> result_table;
    status = arrow::Table::FromRecordBatches(out_batches, &result_table);
    EvaluateStatus(status, __PRETTY_FUNCTION__, __LINE__);

    return result_table;
}
