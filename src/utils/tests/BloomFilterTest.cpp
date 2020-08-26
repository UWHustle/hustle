#include <fstream>

#include "../../table/util.h"
#include "gtest/gtest.h"
#include "operators/BloomFilter.h"

using namespace testing;

class BloomFilterTestFixture : public testing::Test {
 protected:
  void SetUp() override {}
};

TEST_F(BloomFilterTestFixture, Test1) {
  auto d = read_from_file("/Users/corrado/hustle/data/ssb-1/date.hsl");

  auto col = d->get_column(0);
  BloomFilter b(col->length());
  b.set_memory(10);

  // Build
  for (int i = 0; i < col->num_chunks(); i++) {
    auto chunk = std::static_pointer_cast<arrow::Int64Array>(col->chunk(i));

    for (int j = 0; j < chunk->length(); j++) {
      auto val = chunk->Value(j);
      b.insert(val);
    }
  }

  // Probe
  for (int i = 0; i < col->num_chunks(); i++) {
    auto chunk = std::static_pointer_cast<arrow::Int64Array>(col->chunk(i));

    for (int j = 0; j < chunk->length(); j++) {
      auto val = chunk->Value(j);
      b.probe(j);
    }

    b.update();
  }

  std::cout << std::endl;
  std::cout << "hit rate = " << b.get_hit_rate() << std::endl;
}