//
// Created by Sandhya Kannan on 3/10/20.
//

#include <arrow/io/api.h>
#include <arrow/csv/options.h>
#include <arrow/csv/reader.h>
#include <arrow/ipc/api.h>
#include <bitweaving/table.h>
#include "bitweaving_test_util.h"
#include "table/table.h"

namespace hustle::bitweaving {


        void evaluate_status(const arrow::Status &status, const char *function_name,
                             int line_no) {
            if (!status.ok()) {
                std::cout << "\nInvalid status: " << function_name << ", line "
                          << line_no << std::endl;
                throw std::runtime_error(status.ToString());
            }
        }

        std::shared_ptr<arrow::Table> build_table(const std::string &file_path, arrow::MemoryPool *pool, std::vector<std::string> &schema) {

            arrow::Result<std::shared_ptr<arrow::io::ReadableFile>> open_result = arrow::io::ReadableFile::Open(file_path, pool);
            assert(open_result.status() == arrow::Status::OK());
            std::shared_ptr<arrow::io::ReadableFile> infile = open_result.ValueOrDie();

            auto read_options = arrow::csv::ReadOptions::Defaults();
            read_options.column_names = schema;
            auto parse_options = arrow::csv::ParseOptions::Defaults();
            parse_options.delimiter = '|';
            auto convert_options = arrow::csv::ConvertOptions::Defaults();

            arrow::Result<std::shared_ptr<arrow::csv::TableReader>> reader_result = arrow::csv::TableReader::Make(arrow::default_memory_pool(),
                                                                                                           infile, read_options, parse_options, convert_options);
            assert(reader_result.status() == arrow::Status::OK());
            std::shared_ptr<arrow::csv::TableReader> reader = reader_result.ValueOrDie();

            arrow::Result<std::shared_ptr<arrow::Table>> table_result = reader->Read();
            assert(table_result.status() == arrow::Status::OK());
            std::shared_ptr<arrow::Table> table = table_result.ValueOrDie();

            return table;
        }

        void addColumnsToBitweavingTable(std::shared_ptr<Table> arrow_table,
                                                         BWTable* bw_table) {
            int num_cols = arrow_table->get_schema()->num_fields();
            std::vector<std::string> col_names = arrow_table->get_schema()->field_names();
            uint8_t bit_width = 16; //Set the default bitwidth to be 16 and let the bitweaving library to 
                                    // figure out the optimum bitwidth
            int total_cols_bitweaving_table = 0;
            for(int i=0; i<num_cols; i++){
                arrow::Type::type col_type = arrow_table->get_column(i)->type()->id();
                //std::cout << "col: " << col_names[i] << "\t col_type: " << col_type << std::endl;

                if(col_type != arrow::Type::INT16 && col_type != arrow::Type::INT32 && 
                    col_type != arrow::Type::INT64 && col_type != arrow::Type::INT8 &&
                    col_type != arrow::Type::UINT16 && col_type != arrow::Type::UINT32 && 
                    col_type != arrow::Type::UINT64 && col_type != arrow::Type::UINT8){
                    continue;
                }

                if(!( col_names[i] == "quantity" || col_names[i] == "discount" || col_names[i] == "year" ||
                    col_names[i] == "year month num" || col_names[i] == "week num in year")){
                    continue; //not adding any other column to decrease the loading time
                }

                //std::cout << "Column : " << col_names[i] << std::endl;

                bw_table->AddColumn(col_names[i], kBitWeavingV, bit_width);
                total_cols_bitweaving_table++;

                Column *column = bw_table->GetColumn(col_names[i]);;
                assert(column != nullptr);

                std::shared_ptr<arrow::ChunkedArray> col = arrow_table->get_column_by_name(col_names[i]);

                int chunk_len;
                for(int c=0; c < col->num_chunks(); c++){
                    std::shared_ptr<arrow::Array> chunk = col->chunk(c);
                    chunk_len = chunk->length();
                    auto *array = (std::static_pointer_cast<arrow::UInt64Array>(chunk))->raw_values();
                    //std::cout << chunk->ToString() << std::endl;
                    //column->Append(array, chunk_len);
                    AppendResult* result = bw_table->AppendToColumn(col_names[i],
                            (Code*)array, chunk_len);
                    assert(result->GetAppendStatus().IsOk());
                }
            }
            //std::cout << "Total number of columns in the arrow table is " << num_cols << std::endl;
            //std::cout << "Total number of columns in the corresponding bitweaving table is " << total_cols_bitweaving_table << std::endl;
        }
}

