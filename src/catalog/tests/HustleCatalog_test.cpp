#include <stdio.h>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "sqlite3/sqlite3.h"
#include <cereal/archives/json.hpp>
#include <cereal/types/map.hpp>
#include <cereal/types/vector.hpp>
#include <cereal/types/string.hpp>
#include <cereal/types/complex.hpp>
#include <cereal/types/optional.hpp>
#include <cereal/types/memory.hpp>

#include "catalog/Catalog.h"

using namespace testing;
using hustle::catalog::ColumnType;
using hustle::catalog::HustleType;
using hustle::catalog::ColumnSchema;
using hustle::catalog::TableSchema;
using hustle::catalog::Catalog;


TEST(ColumnSchema, HappyPath) {
  ColumnSchema cs("c1", {HustleType::INTEGER}, false, true);
  EXPECT_EQ(cs.getName(), "c1");
  EXPECT_TRUE(cs.isUnique());
  EXPECT_FALSE(cs.isNotNull());
  EXPECT_EQ(cs.getHustleType(), HustleType::INTEGER);
  EXPECT_FALSE(cs.getTypeSize());

  ColumnSchema cs2("c2", {HustleType::CHAR, 15}, true, false);
  EXPECT_EQ(cs2.getName(), "c2");
  EXPECT_FALSE(cs2.isUnique());
  EXPECT_TRUE(cs2.isNotNull());
  EXPECT_EQ(cs2.getHustleType(), HustleType::CHAR);
  EXPECT_EQ(*(cs2.getTypeSize()), 15);
}

TEST(TableSchema, HappyPath) {
  TableSchema ts("Subscriber");

  EXPECT_EQ(ts.getName(), "Subscriber");

  EXPECT_FALSE(ts.ColumnExists("c1"));
  EXPECT_FALSE(ts.ColumnExists("c2"));

  EXPECT_TRUE(ts.addColumn({"c1", {HustleType::INTEGER}, true, false}));
  EXPECT_TRUE(ts.addColumn({"c2", {HustleType::CHAR, 10}, false, true}));
  EXPECT_FALSE(ts.addColumn({"c2", {HustleType::CHAR, 10}, false, true}));

  EXPECT_TRUE(ts.ColumnExists("c1"));
  EXPECT_EQ(ts.ColumnExists("c1").value()->getName(), "c1");
  EXPECT_TRUE(ts.ColumnExists("c2"));
  EXPECT_EQ(ts.ColumnExists("c2").value()->getName(), "c2");

  EXPECT_THAT(ts.getPrimaryKey(), ElementsAre());
  ts.setPrimaryKey({"c1", "c2"});
  EXPECT_THAT(ts.getPrimaryKey(), ElementsAre("c1", "c2"));

}

TEST(CatalogTest, AddTable) {

  std::filesystem::remove("catalog.json");
  std::filesystem::remove("hustle_sqlite.db");
  Catalog catalog = hustle::catalog::Catalog::CreateCatalog("catalog.json", "hustle_sqlite.db");

  EXPECT_FALSE(catalog.TableExists("Subscriber"));

  TableSchema ts("Subscriber");
  ColumnSchema c1("c1", {HustleType::INTEGER, 0}, true, false);
  ColumnSchema c2("c2", {HustleType::CHAR, 10}, false, true);
  ts.addColumn(c1);
  ts.addColumn(c2);
  ts.setPrimaryKey({"c1", "c2"});
  EXPECT_TRUE(catalog.addTable(ts));

  EXPECT_TRUE(catalog.TableExists("Subscriber"));
  EXPECT_FALSE(catalog.TableExists("AccessInfo"));

  TableSchema ts1("AccessInfo");
  ColumnSchema c3("c3", {HustleType::INTEGER, 0}, true, false);
  ColumnSchema c4("c4", {HustleType::CHAR, 5}, false, true);
  ts1.addColumn(c3);
  ts1.addColumn(c4);
  ts1.setPrimaryKey({"c3"});
  EXPECT_TRUE(catalog.addTable(ts1));
  EXPECT_FALSE(catalog.addTable(ts1));

  EXPECT_TRUE(catalog.TableExists("AccessInfo"));

}

TEST(CatalogTest, DropTable) {

  std::filesystem::remove("catalog.json");
  std::filesystem::remove("hustle_sqlite.db");
  Catalog catalog = hustle::catalog::Catalog::CreateCatalog("catalog.json", "hustle_sqlite.db");

  TableSchema ts1("AccessInfo");
  ColumnSchema c3("c3", {HustleType::INTEGER, 0}, true, false);
  ColumnSchema c4("c4", {HustleType::CHAR, 5}, false, true);
  ts1.addColumn(c3);
  ts1.addColumn(c4);
  ts1.setPrimaryKey({"c3"});
  EXPECT_TRUE(catalog.addTable(ts1));
  EXPECT_FALSE(catalog.addTable(ts1));

  EXPECT_TRUE(catalog.TableExists("AccessInfo"));
  EXPECT_EQ(catalog.TableExists("AccessInfo").value()->getName(), "AccessInfo");
  EXPECT_TRUE(catalog.dropTable("AccessInfo"));

  EXPECT_FALSE(catalog.TableExists("AccessInfo"));
  EXPECT_FALSE(catalog.dropTable("AccessInfo"));
}

TEST(ColumnType, Serialization) {
  ColumnType ct = ColumnType(HustleType::INTEGER);

  EXPECT_EQ(ct.getHustleType(), HustleType::INTEGER);
  EXPECT_FALSE(ct.getSize());

  ColumnType ct2 = ColumnType(HustleType::CHAR, 15);
  EXPECT_EQ(ct2.getHustleType(), HustleType::CHAR);
  EXPECT_EQ(*(ct2.getSize()), 15);

  std::stringstream ss;

  {
    cereal::JSONOutputArchive oarchive(ss);
    oarchive(ct, ct2);
  }

  ColumnType ct_cereal, ct2_cereal;
  {
    cereal::JSONInputArchive iarchive(ss);
    iarchive(ct_cereal, ct2_cereal);

  }

  EXPECT_EQ(ct_cereal.getHustleType(), HustleType::INTEGER);
  EXPECT_FALSE(ct_cereal.getSize());

  EXPECT_EQ(ct2_cereal.getHustleType(), HustleType::CHAR);
  EXPECT_EQ(*(ct2_cereal.getSize()), 15);
}

TEST(ColumnSchema, Serialization) {
  ColumnSchema cs("c1", {HustleType::INTEGER}, false, true);
  EXPECT_EQ(cs.getName(), "c1");
  EXPECT_TRUE(cs.isUnique());
  EXPECT_FALSE(cs.isNotNull());
  EXPECT_EQ(cs.getHustleType(), HustleType::INTEGER);
  EXPECT_FALSE(cs.getTypeSize());

  ColumnSchema cs2("c2", {HustleType::CHAR, 15}, true, false);
  EXPECT_EQ(cs2.getName(), "c2");
  EXPECT_FALSE(cs2.isUnique());
  EXPECT_TRUE(cs2.isNotNull());
  EXPECT_EQ(cs2.getHustleType(), HustleType::CHAR);
  EXPECT_EQ(*(cs2.getTypeSize()), 15);

  std::stringstream ss;

  {
    cereal::JSONOutputArchive oarchive(ss);
    oarchive(cs, cs2);
  }

  ColumnSchema cs_cereal, cs2_cereal;
  {
    cereal::JSONInputArchive iarchive(ss);
    iarchive(cs_cereal, cs2_cereal);

  }

  EXPECT_EQ(cs_cereal.getName(), "c1");
  EXPECT_TRUE(cs_cereal.isUnique());
  EXPECT_FALSE(cs_cereal.isNotNull());
  EXPECT_EQ(cs_cereal.getHustleType(), HustleType::INTEGER);
  EXPECT_FALSE(cs_cereal.getTypeSize());

  EXPECT_EQ(cs2_cereal.getName(), "c2");
  EXPECT_FALSE(cs2_cereal.isUnique());
  EXPECT_TRUE(cs2_cereal.isNotNull());
  EXPECT_EQ(cs2_cereal.getHustleType(), HustleType::CHAR);
  EXPECT_EQ(*(cs2_cereal.getTypeSize()), 15);

}

TEST(TableSchema, Serialization) {
  TableSchema ts("Subscriber");

  EXPECT_EQ(ts.getName(), "Subscriber");

  EXPECT_FALSE(ts.ColumnExists("c1"));
  EXPECT_FALSE(ts.ColumnExists("c2"));

  EXPECT_TRUE(ts.addColumn({"c1", {HustleType::INTEGER}, true, false}));
  EXPECT_TRUE(ts.addColumn({"c2", {HustleType::CHAR, 10}, false, true}));
  EXPECT_FALSE(ts.addColumn({"c2", {HustleType::CHAR, 10}, false, true}));

  EXPECT_TRUE(ts.ColumnExists("c1"));
  EXPECT_EQ(ts.ColumnExists("c1").value()->getName(), "c1");
  EXPECT_TRUE(ts.ColumnExists("c2"));
  EXPECT_EQ(ts.ColumnExists("c2").value()->getName(), "c2");

  EXPECT_THAT(ts.getPrimaryKey(), ElementsAre());
  ts.setPrimaryKey({"c1", "c2"});
  EXPECT_THAT(ts.getPrimaryKey(), ElementsAre("c1", "c2"));

  std::stringstream ss;

  {
    cereal::JSONOutputArchive oarchive(ss);
    oarchive(ts);
  }

  TableSchema ts_cereal;
  {
    cereal::JSONInputArchive iarchive(ss);
    iarchive(ts_cereal);

  }

  EXPECT_EQ(ts.getName(), "Subscriber");

  EXPECT_TRUE(ts_cereal.ColumnExists("c1"));
  EXPECT_EQ(ts_cereal.ColumnExists("c1").value()->getName(), "c1");
  EXPECT_TRUE(ts_cereal.ColumnExists("c2"));
  EXPECT_EQ(ts_cereal.ColumnExists("c2").value()->getName(), "c2");

  EXPECT_THAT(ts_cereal.getPrimaryKey(), ElementsAre("c1", "c2"));

}

namespace hustle {
namespace catalog {

// This test is in the same namespace as the catalog to be able to access
// the private constructor of the Catalog.
TEST(CatalogTest, Serialization) {
  std::filesystem::remove("catalog.json");
  std::filesystem::remove("hustle_sqlite.db");
  Catalog catalog = hustle::catalog::Catalog::CreateCatalog("catalog.json", "hustle_sqlite.db");

  EXPECT_FALSE(catalog.TableExists("Subscriber"));

  TableSchema ts("Subscriber");
  ColumnSchema c1("c1", {HustleType::INTEGER, 0}, true, false);
  ColumnSchema c2("c2", {HustleType::CHAR, 10}, false, true);
  ts.addColumn(c1);
  ts.addColumn(c2);
  ts.setPrimaryKey({"c1", "c2"});
  EXPECT_TRUE(catalog.addTable(ts));

  EXPECT_TRUE(catalog.TableExists("Subscriber"));
  EXPECT_FALSE(catalog.TableExists("AccessInfo"));

  TableSchema ts1("AccessInfo");
  ColumnSchema c3("c3", {HustleType::INTEGER, 0}, true, false);
  ColumnSchema c4("c4", {HustleType::CHAR, 5}, false, true);
  ts1.addColumn(c3);
  ts1.addColumn(c4);
  ts1.setPrimaryKey({"c3"});
  EXPECT_TRUE(catalog.addTable(ts1));
  EXPECT_FALSE(catalog.addTable(ts1));

  EXPECT_TRUE(catalog.TableExists("AccessInfo"));
  EXPECT_EQ(catalog.TableExists("AccessInfo").value()->getName(), "AccessInfo");
  EXPECT_FALSE(catalog.TableExists("dd"));
  EXPECT_TRUE(catalog.TableExists("AccessInfo").value()->ColumnExists("c3"));
  EXPECT_FALSE(catalog.TableExists("AccessInfo").value()->ColumnExists("c6"));
  EXPECT_EQ(catalog.TableExists("AccessInfo").value()->ColumnExists("c3").
      value()->getName(), "c3");
  EXPECT_EQ(catalog.TableExists("AccessInfo").value()->ColumnExists("c4").
      value()->getName(), "c4");
  EXPECT_THAT(catalog.TableExists("AccessInfo").value()->getPrimaryKey(),
              ElementsAre("c3"));

  std::stringstream ss;

  {
    cereal::JSONOutputArchive oarchive(ss);
    oarchive(catalog);
  }

  Catalog catalog_cereal;
  {
    cereal::JSONInputArchive iarchive(ss);
    iarchive(catalog_cereal);

  }

  EXPECT_TRUE(catalog_cereal.TableExists("Subscriber"));
  EXPECT_TRUE(catalog_cereal.TableExists("AccessInfo"));
  EXPECT_FALSE(catalog_cereal.addTable(ts1));
  EXPECT_FALSE(catalog_cereal.addTable(ts));

  EXPECT_EQ(catalog_cereal.TableExists("AccessInfo").value()->getName(),
            "AccessInfo");
  EXPECT_TRUE(catalog_cereal.TableExists("AccessInfo").value()->ColumnExists(
      "c3"));
  EXPECT_TRUE(catalog_cereal.TableExists("AccessInfo").value()->ColumnExists(
      "c4"));
  EXPECT_FALSE(catalog_cereal.TableExists("AccessInfo").value()->ColumnExists(
      "c6"));
  EXPECT_EQ(catalog_cereal.TableExists("AccessInfo").value()->ColumnExists("c3").value()->getName(),
            "c3");
  EXPECT_EQ(catalog_cereal.TableExists("AccessInfo").value()->ColumnExists("c4").value()->getName(),
            "c4");
  EXPECT_THAT(catalog_cereal.TableExists("AccessInfo").value()->getPrimaryKey(),
              ElementsAre("c3"));

}

}
}


TEST(CatalogSerialization, LoadFromFile) {
  std::filesystem::remove("catalog.json");
  std::filesystem::remove("hustle_sqlite.db");

  TableSchema ts("Subscriber");
  ColumnSchema c1("c1", {HustleType::INTEGER, 0}, true, false);
  ColumnSchema c2("c2", {HustleType::CHAR, 10}, false, true);
  ts.addColumn(c1);
  ts.addColumn(c2);
  ts.setPrimaryKey({"c1", "c2"});

  TableSchema ts1("AccessInfo");
  ColumnSchema c3("c3", {HustleType::INTEGER, 0}, true, false);
  ColumnSchema c4("c4", {HustleType::CHAR, 5}, false, true);
  ts1.addColumn(c3);
  ts1.addColumn(c4);
  ts1.setPrimaryKey({"c3"});

  {
    Catalog catalog = hustle::catalog::Catalog::CreateCatalog("catalog.json", "hustle_sqlite.db");
    EXPECT_TRUE(catalog.addTable(ts));
    EXPECT_TRUE(catalog.TableExists("Subscriber"));

    EXPECT_TRUE(catalog.addTable(ts1));
    EXPECT_FALSE(catalog.addTable(ts1));
  }

  Catalog catalog = hustle::catalog::Catalog::CreateCatalog("catalog.json", "hustle_sqlite.db");

  EXPECT_TRUE(catalog.TableExists("Subscriber"));
  EXPECT_TRUE(catalog.TableExists("AccessInfo"));
  EXPECT_FALSE(catalog.addTable(ts1));
  EXPECT_FALSE(catalog.addTable(ts));

  EXPECT_EQ(catalog.TableExists("AccessInfo").value()->getName(),
            "AccessInfo");
  EXPECT_TRUE(catalog.TableExists("AccessInfo").value()->ColumnExists(
      "c3"));
  EXPECT_TRUE(catalog.TableExists("AccessInfo").value()->ColumnExists(
      "c4"));
  EXPECT_FALSE(catalog.TableExists("AccessInfo").value()->ColumnExists(
      "c6"));
  EXPECT_EQ(catalog.TableExists("AccessInfo").value()->
      ColumnExists("c3").value()->getName(), "c3");
  EXPECT_EQ(catalog.TableExists("AccessInfo").value()->
      ColumnExists("c4").value()->getName(), "c4");
  EXPECT_THAT(catalog.TableExists("AccessInfo").value()->getPrimaryKey(),
              ElementsAre("c3"));

}
