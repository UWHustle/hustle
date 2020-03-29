#include <iostream>
#include <iostream>
#include <filesystem>
#include <arrow/io/api.h>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "table/table.h"
#include "table/util.h"

using namespace testing;

class HustleUtilTest : public testing::Test {
protected:

    void SetUp() override {

    }
};

TEST_F(HustleUtilTest, CatalogToArrowSchema) {

    hustle::catalog::ColumnType int_type
            (hustle::catalog::HustleType::INTEGER, sizeof(int64_t));
    hustle::catalog::ColumnSchema int_col("int col", int_type, true, true);

    hustle::catalog::ColumnType str_type(hustle::catalog::HustleType::CHAR);
    hustle::catalog::ColumnSchema str_col("str col", str_type, true, true);

    hustle::catalog::TableSchema catalog_schema("schema");
    catalog_schema.addColumn(int_col);
    catalog_schema.addColumn(str_col);

    auto schema = make_schema(catalog_schema);

    std::cout << schema->ToString() << std::endl;
}