//
// Created by Sandhya Kannan on 3/10/20.
//

#include <arrow/io/api.h>
#include <arrow/csv/options.h>
#include <arrow/csv/reader.h>
#include <bitweaving/table.h>
#include "table/table.h"
#include "bitweaving_util.h"
#include <map>

namespace hustle::bitweaving {

BWTable *createBitweavingIndex(const std::shared_ptr<Table> &hustle_table,
                               std::vector<BitweavingColumnIndexUnit> cols,
                               bool auto_tune_bitwidth) {
  Options options = Options();
  options.delete_exist_files = true;
  options.in_memory = true;

  auto* bw_table = new BWTable("./abc", options);
  int num_cols = cols.size();
  for (int i = 0; i < num_cols; i++) {
    arrow::Type::type col_type = hustle_table->get_column(i)->type()->id();
    //std::cout << "col: " << col_names[i] << "\t col_type: " << col_type << std::endl;

    if (col_type != arrow::Type::INT16 && col_type != arrow::Type::INT32 &&
        col_type != arrow::Type::INT64 && col_type != arrow::Type::INT8 &&
        col_type != arrow::Type::UINT16 && col_type != arrow::Type::UINT32 &&
        col_type != arrow::Type::UINT64 && col_type != arrow::Type::UINT8) {
      continue;
    }
    //std::cout << "Column : " << col_names[i] << std::endl;

    bw_table->AddColumn(cols[i].col_name, kBitWeavingV, cols[i].bit_width);

    Column *column = bw_table->GetColumn(cols[i].col_name);;
    assert(column != nullptr);

    std::shared_ptr<arrow::ChunkedArray> col = hustle_table->get_column_by_name(cols[i].col_name);

    int chunk_len;
    for (int c = 0; c < col->num_chunks(); c++) {
      std::shared_ptr<arrow::Array> chunk = col->chunk(c);
      chunk_len = chunk->length();
      auto *array = (std::static_pointer_cast<arrow::UInt64Array>(chunk))->raw_values();

      if (auto_tune_bitwidth) {
        AppendResult *result = bw_table->AppendToColumn(cols[i].col_name,
                                                        (Code *) array, chunk_len);
        assert(result->GetAppendStatus().IsOk());
      } else {
        column->Append(array, chunk_len);
      }
    }
  }

  return bw_table;
}

}

