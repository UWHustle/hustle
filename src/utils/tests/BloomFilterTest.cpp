#include "gtest/gtest.h"
#include "../../table/util.h"
#include <fstream>
#include <utils/BloomFilter.h>

using namespace testing;

class BloomFilterTestFixture : public testing::Test {
protected:

    void SetUp() override {

    }
};


TEST_F(BloomFilterTestFixture, Test1) {

    auto d  = read_from_file("/Users/corrado/hustle/data/ssb-1/date.hsl");

    auto col = d->get_column(0);
    BloomFilter b(col);
    b.set_memory(10);

    for (int i=0; i<col->num_chunks(); i++) {
        // TODO(nicholas): For now, we assume the column is of string type.
        auto chunk = std::static_pointer_cast<arrow::Int64Array>(col->chunk(i));

        for (int j = 0; j < chunk->length(); j++) {
            auto val = chunk->Value(j);
            b.probe(val/2);
        }

        b.update();
    }


    std::cout << std::endl;
    std::cout << "hit rate = " << b.get_hit_rate() << std::endl;


}