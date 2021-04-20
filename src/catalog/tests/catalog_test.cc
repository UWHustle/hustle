// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//#include "catalog/catalog.h"
//
//#include <stdio.h>
//
//#include <cereal/archives/json.hpp>
//#include <cereal/types/complex.hpp>
//#include <cereal/types/map.hpp>
//#include <cereal/types/memory.hpp>
//#include <cereal/types/optional.hpp>
//#include <cereal/types/string.hpp>
//#include <cereal/types/vector.hpp>
//#include "api/hustle_db.h"
//#include "gmock/gmock.h"
//#include "gtest/gtest.h"
//#include "sqlite3/sqlite3.h"
//
//using namespace testing;
//using hustle::catalog::Catalog;
//using hustle::catalog::ColumnSchema;
//using hustle::catalog::ColumnType;
//using hustle::catalog::HustleType;
//using hustle::catalog::TableSchema;
//
//TEST(ColumnSchema, HappyPath) {
//  ColumnSchema cs("c1", {HustleType::INTEGER}, false, true);
//  EXPECT_EQ(cs.getName(), "c1");
//  EXPECT_TRUE(cs.isUnique());
//  EXPECT_FALSE(cs.isNotNull());
//  EXPECT_EQ(cs.getHustleType(), HustleType::INTEGER);
//  EXPECT_FALSE(cs.getTypeSize());
//
//  ColumnSchema cs2("c2", {HustleType::CHAR, 15}, true, false);
//  EXPECT_EQ(cs2.getName(), "c2");
//  EXPECT_FALSE(cs2.isUnique());
//  EXPECT_TRUE(cs2.isNotNull());
//  EXPECT_EQ(cs2.getHustleType(), HustleType::CHAR);
//  EXPECT_EQ(*(cs2.getTypeSize()), 15);
//}
//
//TEST(TableSchema, HappyPath) {
//  TableSchema ts("Subscriber");
//
//  EXPECT_EQ(ts.getName(), "Subscriber");
//
//  EXPECT_FALSE(ts.ColumnExists("c1"));
//  EXPECT_FALSE(ts.ColumnExists("c2"));
//
//  EXPECT_TRUE(ts.addColumn({"c1", {HustleType::INTEGER}, true, false}));
//  EXPECT_TRUE(ts.addColumn({"c2", {HustleType::CHAR, 10}, false, true}));
//  EXPECT_FALSE(ts.addColumn({"c2", {HustleType::CHAR, 10}, false, true}));
//
//  EXPECT_TRUE(ts.ColumnExists("c1"));
//  EXPECT_EQ(ts.ColumnExists("c1").value()->getName(), "c1");
//  EXPECT_TRUE(ts.ColumnExists("c2"));
//  EXPECT_EQ(ts.ColumnExists("c2").value()->getName(), "c2");
//
//  EXPECT_THAT(ts.getPrimaryKey(), ElementsAre());
//  ts.setPrimaryKey({"c1", "c2"});
//  EXPECT_THAT(ts.getPrimaryKey(), ElementsAre("c1", "c2"));
//}
//
//TEST(CatalogTest, AddTable) {
//  std::filesystem::remove("catalog.json");
//  std::filesystem::remove("hustle_sqlite.db");
//  hustle::HustleDB hustleDB("db_directory");
//  std::shared_ptr<Catalog> catalog = hustleDB.get_catalog("db_directory/hustle_sqlite.db");
//
//  EXPECT_FALSE(catalog->TableExists("Subscriber"));
//  std::cout << "SQLITE DB PATH: " << hustleDB.get_sqlite_path() << std::endl;
//  TableSchema ts("Subscriber");
//  ColumnSchema c1("c1", {HustleType::INTEGER, 0}, true, false);
//  ColumnSchema c2("c2", {HustleType::CHAR, 10}, false, true);
//  ts.addColumn(c1);
//  ts.addColumn(c2);
//  ts.setPrimaryKey({"c1", "c2"});
//  EXPECT_TRUE(catalog->AddTable(hustleDB.sqlite3_db(), ts));
//
//  EXPECT_TRUE(catalog->TableExists("Subscriber"));
//  EXPECT_FALSE(catalog->TableExists("AccessInfo"));
//
//  TableSchema ts1("AccessInfo");
//  ColumnSchema c3("c3", {HustleType::INTEGER, 0}, true, false);
//  ColumnSchema c4("c4", {HustleType::CHAR, 5}, false, true);
//  ts1.addColumn(c3);
//  ts1.addColumn(c4);
//  ts1.setPrimaryKey({"c3"});
//
//  EXPECT_TRUE(catalog->AddTable(hustleDB.sqlite3_db(), ts1));
//  EXPECT_FALSE(catalog->AddTable(hustleDB.sqlite3_db(), ts1));
//
//  EXPECT_TRUE(catalog->TableExists("AccessInfo"));
//}
//
//TEST(CatalogTest, DropTable) {
//  std::filesystem::remove("catalog.json");
//  std::filesystem::remove("hustle_sqlite.db");
//  hustle::HustleDB hustleDB("db_directory");
//
//  std::shared_ptr<Catalog> catalog = hustleDB.get_catalog("db_directory/hustle_sqlite.db");
//  TableSchema ts1("AccessInfo_drop");
//  ColumnSchema c3("c3", {HustleType::INTEGER, 0}, true, false);
//  ColumnSchema c4("c4", {HustleType::CHAR, 5}, false, true);
//  ts1.addColumn(c3);
//  ts1.addColumn(c4);
//  ts1.setPrimaryKey({"c3"});
//  EXPECT_TRUE(catalog->AddTable(hustleDB.sqlite3_db(), ts1));
//  EXPECT_FALSE(catalog->AddTable(hustleDB.sqlite3_db(), ts1));
//
//  EXPECT_TRUE(catalog->TableExists("AccessInfo_drop"));
//  EXPECT_EQ(catalog->TableExists("AccessInfo_drop").value()->getName(), "AccessInfo_drop");
//  EXPECT_TRUE(catalog->DropTable(hustleDB.sqlite3_db(), "AccessInfo_drop"));
//
//  EXPECT_FALSE(catalog->TableExists("AccessInfo_drop"));
//  EXPECT_FALSE(catalog->DropTable(hustleDB.sqlite3_db(), "AccessInfo_drop"));
//}
//
//TEST(ColumnType, Serialization) {
//  ColumnType ct = ColumnType(HustleType::INTEGER);
//
//  EXPECT_EQ(ct.getHustleType(), HustleType::INTEGER);
//  EXPECT_FALSE(ct.getSize());
//
//  ColumnType ct2 = ColumnType(HustleType::CHAR, 15);
//  EXPECT_EQ(ct2.getHustleType(), HustleType::CHAR);
//  EXPECT_EQ(*(ct2.getSize()), 15);
//
//  std::stringstream ss;
//
//  {
//    cereal::JSONOutputArchive oarchive(ss);
//    oarchive(ct, ct2);
//  }
//
//  ColumnType ct_cereal, ct2_cereal;
//  {
//    cereal::JSONInputArchive iarchive(ss);
//    iarchive(ct_cereal, ct2_cereal);
//  }
//
//  EXPECT_EQ(ct_cereal.getHustleType(), HustleType::INTEGER);
//  EXPECT_FALSE(ct_cereal.getSize());
//
//  EXPECT_EQ(ct2_cereal.getHustleType(), HustleType::CHAR);
//  EXPECT_EQ(*(ct2_cereal.getSize()), 15);
//}
//
//
//TEST(ColumnSchema, Serialization) {
//  ColumnSchema cs("c1", {HustleType::INTEGER}, false, true);
//  EXPECT_EQ(cs.getName(), "c1");
//  EXPECT_TRUE(cs.isUnique());
//  EXPECT_FALSE(cs.isNotNull());
//  EXPECT_EQ(cs.getHustleType(), HustleType::INTEGER);
//  EXPECT_FALSE(cs.getTypeSize());
//
//  ColumnSchema cs2("c2", {HustleType::CHAR, 15}, true, false);
//  EXPECT_EQ(cs2.getName(), "c2");
//  EXPECT_FALSE(cs2.isUnique());
//  EXPECT_TRUE(cs2.isNotNull());
//  EXPECT_EQ(cs2.getHustleType(), HustleType::CHAR);
//  EXPECT_EQ(*(cs2.getTypeSize()), 15);
//
//  std::stringstream ss;
//
//  {
//    cereal::JSONOutputArchive oarchive(ss);
//    oarchive(cs, cs2);
//  }
//
//  ColumnSchema cs_cereal, cs2_cereal;
//  {
//    cereal::JSONInputArchive iarchive(ss);
//    iarchive(cs_cereal, cs2_cereal);
//  }
//
//  EXPECT_EQ(cs_cereal.getName(), "c1");
//  EXPECT_TRUE(cs_cereal.isUnique());
//  EXPECT_FALSE(cs_cereal.isNotNull());
//  EXPECT_EQ(cs_cereal.getHustleType(), HustleType::INTEGER);
//  EXPECT_FALSE(cs_cereal.getTypeSize());
//
//  EXPECT_EQ(cs2_cereal.getName(), "c2");
//  EXPECT_FALSE(cs2_cereal.isUnique());
//  EXPECT_TRUE(cs2_cereal.isNotNull());
//  EXPECT_EQ(cs2_cereal.getHustleType(), HustleType::CHAR);
//  EXPECT_EQ(*(cs2_cereal.getTypeSize()), 15);
//}
//
//
//TEST(TableSchema, Serialization) {
//  TableSchema ts("Subscriber");
//
//  EXPECT_EQ(ts.getName(), "Subscriber");
//
//  EXPECT_FALSE(ts.ColumnExists("c1"));
//  EXPECT_FALSE(ts.ColumnExists("c2"));
//
//  EXPECT_TRUE(ts.addColumn({"c1", {HustleType::INTEGER}, true, false}));
//  EXPECT_TRUE(ts.addColumn({"c2", {HustleType::CHAR, 10}, false, true}));
//  EXPECT_FALSE(ts.addColumn({"c2", {HustleType::CHAR, 10}, false, true}));
//
//  EXPECT_TRUE(ts.ColumnExists("c1"));
//  EXPECT_EQ(ts.ColumnExists("c1").value()->getName(), "c1");
//  EXPECT_TRUE(ts.ColumnExists("c2"));
//  EXPECT_EQ(ts.ColumnExists("c2").value()->getName(), "c2");
//
//  EXPECT_THAT(ts.getPrimaryKey(), ElementsAre());
//  ts.setPrimaryKey({"c1", "c2"});
//  EXPECT_THAT(ts.getPrimaryKey(), ElementsAre("c1", "c2"));
//
//  std::stringstream ss;
//
//  {
//    cereal::JSONOutputArchive oarchive(ss);
//    oarchive(ts);
//  }
//
//  TableSchema ts_cereal;
//  {
//    cereal::JSONInputArchive iarchive(ss);
//    iarchive(ts_cereal);
//  }
//
//  EXPECT_EQ(ts.getName(), "Subscriber");
//
//  EXPECT_TRUE(ts_cereal.ColumnExists("c1"));
//  EXPECT_EQ(ts_cereal.ColumnExists("c1").value()->getName(), "c1");
//  EXPECT_TRUE(ts_cereal.ColumnExists("c2"));
//  EXPECT_EQ(ts_cereal.ColumnExists("c2").value()->getName(), "c2");
//
//  EXPECT_THAT(ts_cereal.getPrimaryKey(), ElementsAre("c1", "c2"));
//}
//
//TEST(TableSchema, basic_arrow_test1) {
//  TableSchema ts("Subscriber");
//
//  EXPECT_EQ(ts.getName(), "Subscriber");
//
//  EXPECT_FALSE(ts.ColumnExists("c1"));
//
//  EXPECT_TRUE(ts.addColumn({"c1", {HustleType::INTEGER}, true, false}));
//
//  EXPECT_TRUE(ts.ColumnExists("c1"));
//  EXPECT_EQ(ts.ColumnExists("c1").value()->getName(), "c1");
//
//  std::shared_ptr<arrow::Schema> arrow_schema = ts.getArrowSchema();
//  EXPECT_EQ(arrow_schema->field_names().size(), 1);
//}
//
//TEST(TableSchema, basic_arrow_test2) {
//  TableSchema ts("Subscriber");
//
//  EXPECT_EQ(ts.getName(), "Subscriber");
//
//  EXPECT_FALSE(ts.ColumnExists("c1"));
//  EXPECT_FALSE(ts.ColumnExists("c2"));
//
//  EXPECT_TRUE(ts.addColumn({"c1", {HustleType::INTEGER}, true, false}));
//  EXPECT_TRUE(ts.addColumn({"c2", {HustleType::CHAR, 10}, false, true}));
//  EXPECT_FALSE(ts.addColumn({"c2", {HustleType::CHAR, 10}, false, true}));
//
//  EXPECT_TRUE(ts.ColumnExists("c1"));
//  EXPECT_EQ(ts.ColumnExists("c1").value()->getName(), "c1");
//  EXPECT_TRUE(ts.ColumnExists("c2"));
//  EXPECT_EQ(ts.ColumnExists("c2").value()->getName(), "c2");
//
//  std::shared_ptr<arrow::Schema> arrow_schema = ts.getArrowSchema();
//  EXPECT_EQ(arrow_schema->field_names().size(), 2);
//}
//
//TEST(TableSchema, arrow_test) {
//  TableSchema ts("Subscriber");
//  EXPECT_EQ(ts.getName(), "Subscriber");
//
//  TableSchema ts_p("Producer");
//  EXPECT_EQ(ts_p.getName(), "Producer");
//
//  EXPECT_FALSE(ts.ColumnExists("c1"));
//  EXPECT_FALSE(ts.ColumnExists("c2"));
//
//  EXPECT_TRUE(ts.addColumn({"c1", {HustleType::INTEGER}, true, false}));
//  EXPECT_TRUE(ts.addColumn({"c2", {HustleType::CHAR, 10}, false, true}));
//  EXPECT_FALSE(ts.addColumn({"c2", {HustleType::CHAR, 10}, false, true}));
//
//  EXPECT_TRUE(ts.ColumnExists("c1"));
//  EXPECT_EQ(ts.ColumnExists("c1").value()->getName(), "c1");
//  EXPECT_TRUE(ts.ColumnExists("c2"));
//  EXPECT_EQ(ts.ColumnExists("c2").value()->getName(), "c2");
//
//  std::shared_ptr<arrow::Schema> arrow_schema = ts.getArrowSchema();
//  EXPECT_EQ(arrow_schema->field_names().size(), 2);
//
//  EXPECT_FALSE(ts_p.ColumnExists("c1"));
//  EXPECT_FALSE(ts_p.ColumnExists("c2"));
//  EXPECT_FALSE(ts_p.ColumnExists("c3"));
//  EXPECT_FALSE(ts_p.ColumnExists("c4"));
//  EXPECT_FALSE(ts_p.ColumnExists("c5"));
//  EXPECT_FALSE(ts_p.ColumnExists("c6"));
//
//  EXPECT_TRUE(ts_p.addColumn({"c1", {HustleType::INTEGER}, true, false}));
//  EXPECT_TRUE(ts_p.addColumn({"c2", {HustleType::CHAR, 10}, false, true}));
//  EXPECT_TRUE(ts_p.addColumn({"c3", {HustleType::INTEGER}, true, false}));
//  EXPECT_TRUE(ts_p.addColumn({"c4", {HustleType::CHAR, 10}, false, true}));
//  EXPECT_TRUE(ts_p.addColumn({"c5", {HustleType::CHAR, 10}, false, true}));
//  EXPECT_TRUE(ts_p.addColumn({"c6", {HustleType::CHAR, 10}, false, true}));
//
//  EXPECT_TRUE(ts_p.ColumnExists("c1"));
//  EXPECT_EQ(ts_p.ColumnExists("c1").value()->getName(), "c1");
//  EXPECT_TRUE(ts_p.ColumnExists("c2"));
//  EXPECT_EQ(ts_p.ColumnExists("c2").value()->getName(), "c2");
//  EXPECT_TRUE(ts_p.ColumnExists("c3"));
//  EXPECT_EQ(ts_p.ColumnExists("c3").value()->getName(), "c3");
//  EXPECT_TRUE(ts_p.ColumnExists("c4"));
//  EXPECT_EQ(ts_p.ColumnExists("c4").value()->getName(), "c4");
//  EXPECT_TRUE(ts_p.ColumnExists("c5"));
//  EXPECT_EQ(ts_p.ColumnExists("c5").value()->getName(), "c5");
//  EXPECT_TRUE(ts_p.ColumnExists("c6"));
//  EXPECT_EQ(ts_p.ColumnExists("c6").value()->getName(), "c6");
//
//  arrow_schema = ts_p.getArrowSchema();
//  EXPECT_EQ(arrow_schema->field_names().size(), 6);
//  EXPECT_EQ((*arrow_schema->field(0)).name(), "c1");
//  EXPECT_EQ((*arrow_schema->field(0)).type(), arrow::int64());
//  EXPECT_EQ((*arrow_schema->field(1)).name(), "c2");
//  EXPECT_EQ((*arrow_schema->field(1)).type(), arrow::utf8());
//  EXPECT_EQ((*arrow_schema->field(2)).name(), "c3");
//  EXPECT_EQ((*arrow_schema->field(2)).type(), arrow::int64());
//  EXPECT_EQ((*arrow_schema->field(3)).name(), "c4");
//  EXPECT_EQ((*arrow_schema->field(3)).type(), arrow::utf8());
//  EXPECT_EQ((*arrow_schema->field(4)).name(), "c5");
//  EXPECT_EQ((*arrow_schema->field(4)).type(), arrow::utf8());
//  EXPECT_EQ((*arrow_schema->field(5)).name(), "c6");
//  EXPECT_EQ((*arrow_schema->field(5)).type(), arrow::utf8());
//}
//
//namespace hustle {
//namespace catalog {
//
//// This test is in the same namespace as the catalog to be able to access
//// the private constructor of the Catalog.
//TEST(CatalogTest, Serialization) {
//  std::filesystem::remove("catalog.json");
//  std::filesystem::remove("hustle_sqlite.db");
//  Catalog catalog = hustle::catalog::Catalog::CreateCatalog("catalog.json",
//                                                            "hustle_sqlite.db");
//
//  EXPECT_FALSE(catalog.TableExists("Subscriber_Serial"));
//
//
//  hustle::HustleDB hustleDB("db_directory");
//  std::shared_ptr<Catalog> catalog_ptr = hustleDB.get_catalog("db_directory/hustle_sqlite.db");
//  EXPECT_TRUE(true);
//  TableSchema ts("Subscriber_Serial");
//  ColumnSchema c1("c1", {HustleType::INTEGER, 0}, true, false);
//  ColumnSchema c2("c2", {HustleType::CHAR, 10}, false, true);
//  ts.addColumn(c1);
//  ts.addColumn(c2);
//  ts.setPrimaryKey({"c1", "c2"});
//  EXPECT_TRUE(catalog_ptr->AddTable(hustleDB.sqlite3_db(), ts));
//  EXPECT_TRUE(catalog_ptr->TableExists("Subscriber_Serial"));
//
//
//  EXPECT_TRUE(catalog_ptr->TableExists("Subscriber_Serial"));
//  EXPECT_FALSE(catalog_ptr->TableExists("AccessInfo_Serial"));
//
//  TableSchema ts1("AccessInfo_Serial");
//  ColumnSchema c3("c3", {HustleType::INTEGER, 0}, true, false);
//  ColumnSchema c4("c4", {HustleType::CHAR, 5}, false, true);
//  ts1.addColumn(c3);
//  ts1.addColumn(c4);
//  ts1.setPrimaryKey({"c3"});
//  EXPECT_TRUE(catalog_ptr->AddTable(hustleDB.sqlite3_db(), ts1));
//  EXPECT_FALSE(catalog_ptr->AddTable(hustleDB.sqlite3_db(), ts1));
//
//  EXPECT_TRUE(catalog_ptr->TableExists("AccessInfo_Serial"));
//  EXPECT_EQ(catalog_ptr->TableExists("AccessInfo_Serial").value()->getName(), "AccessInfo_Serial");
//  EXPECT_FALSE(catalog_ptr->TableExists("dd_Serial"));
//  EXPECT_TRUE(catalog_ptr->TableExists("AccessInfo_Serial").value()->ColumnExists("c3"));
//  EXPECT_FALSE(catalog_ptr->TableExists("AccessInfo_Serial").value()->ColumnExists("c6"));
//  EXPECT_EQ(catalog_ptr->TableExists("AccessInfo_Serial")
//                .value()
//                ->ColumnExists("c3")
//                .value()
//                ->getName(),
//            "c3");
//  EXPECT_EQ(catalog_ptr->TableExists("AccessInfo_Serial")
//                .value()
//                ->ColumnExists("c4")
//                .value()
//                ->getName(),
//            "c4");
//  EXPECT_THAT(catalog_ptr->TableExists("AccessInfo_Serial").value()->getPrimaryKey(),
//              ElementsAre("c3"));
//
//  std::stringstream ss;
//
//  {
//    cereal::JSONOutputArchive oarchive(ss);
//    oarchive(*catalog_ptr);
//  }
//
//  Catalog catalog_cereal;
//  {
//    cereal::JSONInputArchive iarchive(ss);
//    iarchive(catalog_cereal);
//  }
//
//  EXPECT_TRUE(catalog_cereal.TableExists("Subscriber_Serial"));
//  EXPECT_TRUE(catalog_cereal.TableExists("AccessInfo_Serial"));
//  EXPECT_FALSE(catalog_cereal.AddTable(hustleDB.sqlite3_db(), ts1));
//  EXPECT_FALSE(catalog_cereal.AddTable(hustleDB.sqlite3_db(), ts));
//
//  EXPECT_EQ(catalog_cereal.TableExists("AccessInfo_Serial").value()->getName(),
//            "AccessInfo_Serial");
//  EXPECT_TRUE(
//      catalog_cereal.TableExists("AccessInfo_Serial").value()->ColumnExists("c3"));
//  EXPECT_TRUE(
//      catalog_cereal.TableExists("AccessInfo_Serial").value()->ColumnExists("c4"));
//  EXPECT_FALSE(
//      catalog_cereal.TableExists("AccessInfo_Serial").value()->ColumnExists("c6"));
//  EXPECT_EQ(catalog_cereal.TableExists("AccessInfo_Serial")
//                .value()
//                ->ColumnExists("c3")
//                .value()
//                ->getName(),
//            "c3");
//  EXPECT_EQ(catalog_cereal.TableExists("AccessInfo_Serial")
//                .value()
//                ->ColumnExists("c4")
//                .value()
//                ->getName(),
//            "c4");
//  EXPECT_THAT(catalog_cereal.TableExists("AccessInfo_Serial").value()->getPrimaryKey(),
//              ElementsAre("c3"));
//}
//
//}  // namespace catalog
//}  // namespace hustle
//
//TEST(CatalogSerialization, LoadFromFile) {
//  std::filesystem::remove_all("db_directory");
//  hustle::HustleDB hustleDB("db_directory");
//  std::shared_ptr<Catalog> catalog = hustleDB.get_catalog("db_directory/hustle_sqlite.db");
//
//
//  TableSchema ts("Subscriber2");
//  ColumnSchema c1("c1", {HustleType::INTEGER, 0}, true, false);
//  ColumnSchema c2("c2", {HustleType::CHAR, 10}, false, true);
//  ts.addColumn(c1);
//  ts.addColumn(c2);
//  ts.setPrimaryKey({"c1", "c2"});
//
//  TableSchema ts1("AccessInfo2");
//  ColumnSchema c3("c3", {HustleType::INTEGER, 0}, true, false);
//  ColumnSchema c4("c4", {HustleType::CHAR, 5}, false, true);
//  ts1.addColumn(c3);
//  ts1.addColumn(c4);
//  ts1.setPrimaryKey({"c3"});
//  EXPECT_TRUE(catalog->AddTable(hustleDB.sqlite3_db(), ts));
//  EXPECT_TRUE(catalog->TableExists("Subscriber2"));
//
//  EXPECT_TRUE(catalog->AddTable(hustleDB.sqlite3_db(), ts1));
//  EXPECT_FALSE(catalog->AddTable(hustleDB.sqlite3_db(), ts1));
//
//  EXPECT_TRUE(catalog->TableExists("Subscriber2"));
//  EXPECT_TRUE(catalog->TableExists("AccessInfo2"));
//  EXPECT_FALSE(catalog->AddTable(hustleDB.sqlite3_db(), ts1));
//  EXPECT_FALSE(catalog->AddTable(hustleDB.sqlite3_db(), ts));
//
//  EXPECT_EQ(catalog->TableExists("AccessInfo2").value()->getName(), "AccessInfo2");
//  EXPECT_TRUE(catalog->TableExists("AccessInfo2").value()->ColumnExists("c3"));
//  EXPECT_TRUE(catalog->TableExists("AccessInfo2").value()->ColumnExists("c4"));
//  EXPECT_FALSE(catalog->TableExists("AccessInfo2").value()->ColumnExists("c6"));
//  EXPECT_EQ(catalog->TableExists("AccessInfo2")
//                .value()
//                ->ColumnExists("c3")
//                .value()
//                ->getName(),
//            "c3");
//  EXPECT_EQ(catalog->TableExists("AccessInfo2")
//                .value()
//                ->ColumnExists("c4")
//                .value()
//                ->getName(),
//            "c4");
//  EXPECT_THAT(catalog->TableExists("AccessInfo2").value()->getPrimaryKey(),
//              ElementsAre("c3"));
//}
