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


#include "benchmark/tatp_workload.h"
#include "utils/event_profiler.h"

namespace hustle::operators {

TATP::TATP() {
    CreateTable();
}

void TATP::CreateTable() {

  std::shared_ptr<arrow::Schema>  s_schema, ai_schema,
      sf_schema, cf_schema;
  hustle::catalog::TableSchema subscriber("Subscriber");
  hustle::catalog::ColumnSchema s_id(
      "s_id", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema sub_nbr(
      "sub_nbr", {hustle::catalog::HustleType::CHAR, 15}, true, false);
  hustle::catalog::ColumnSchema bit_1(
      "bit_1", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
   hustle::catalog::ColumnSchema bit_2(
      "bit_2", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
   hustle::catalog::ColumnSchema bit_3(
      "bit_3", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
    hustle::catalog::ColumnSchema bit_4(
      "bit_4", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
   hustle::catalog::ColumnSchema bit_5(
      "bit_5", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema bit_6(
      "bit_6", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema bit_7(
      "bit_7", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema bit_8(
      "bit_8", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema bit_9(
      "bit_9", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema bit_10(
      "bit_10", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema hex_1(
      "hex_1", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
   hustle::catalog::ColumnSchema hex_2(
      "hex_2", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
   hustle::catalog::ColumnSchema hex_3(
      "hex_3", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema hex_4(
      "hex_4", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema hex_5(
      "hex_5", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
   hustle::catalog::ColumnSchema hex_6(
      "hex_6", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
   hustle::catalog::ColumnSchema hex_7(
      "hex_7", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
   hustle::catalog::ColumnSchema hex_8(
      "hex_8", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
   hustle::catalog::ColumnSchema hex_9(
      "hex_9", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
   hustle::catalog::ColumnSchema hex_10(
      "hex_10", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
   hustle::catalog::ColumnSchema byte2_1(
      "byte2_1", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
   hustle::catalog::ColumnSchema byte2_2(
      "byte2_2", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
    hustle::catalog::ColumnSchema byte2_3(
      "byte2_3", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
   hustle::catalog::ColumnSchema byte2_4(
      "byte2_4", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  
   hustle::catalog::ColumnSchema byte2_5(
      "byte2_5", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
   hustle::catalog::ColumnSchema byte2_6(
      "byte2_6", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
   hustle::catalog::ColumnSchema byte2_7(
      "byte2_7", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
   hustle::catalog::ColumnSchema byte2_8(
      "byte2_8", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
   hustle::catalog::ColumnSchema byte2_9(
      "byte2_9", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
   hustle::catalog::ColumnSchema byte2_10(
      "byte2_10", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
    hustle::catalog::ColumnSchema msc_location(
      "msc_location", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
   hustle::catalog::ColumnSchema vlr_location(
      "vlr_location", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  
  
  subscriber.addColumn(s_id);
  subscriber.addColumn(sub_nbr);
  subscriber.addColumn(bit_1);
  subscriber.addColumn(bit_2);
  subscriber.addColumn(bit_3);
  subscriber.addColumn(bit_4);
  subscriber.addColumn(bit_5);
  subscriber.addColumn(bit_6);
  subscriber.addColumn(bit_7);
  subscriber.addColumn(bit_8);
  subscriber.addColumn(bit_9);
  subscriber.addColumn(bit_10);
  subscriber.addColumn(hex_1);
  subscriber.addColumn(hex_2);
  subscriber.addColumn(hex_3);
  subscriber.addColumn(hex_4);
  subscriber.addColumn(hex_5);
  subscriber.addColumn(hex_6);
  subscriber.addColumn(hex_7);
  subscriber.addColumn(hex_8);
  subscriber.addColumn(hex_9);
  subscriber.addColumn(hex_10);
  subscriber.addColumn(byte2_1);
  subscriber.addColumn(byte2_2);
  subscriber.addColumn(byte2_3);
  subscriber.addColumn(byte2_4);
  subscriber.addColumn(byte2_5);
  subscriber.addColumn(byte2_6);
  subscriber.addColumn(byte2_7);
  subscriber.addColumn(byte2_8);
  subscriber.addColumn(byte2_9);
  subscriber.addColumn(byte2_10);
  subscriber.addColumn(msc_location);
  subscriber.addColumn(vlr_location);
  subscriber.setPrimaryKey({});
  s_schema = subscriber.getArrowSchema();

  hustle::catalog::TableSchema access_info("Access_Info");
 /* hustle::catalog::ColumnSchema s_id(
      "s_id", {hustle::catalog::HustleType::INTEGER, 0}, true, false);*/
  hustle::catalog::ColumnSchema ai_type(
      "ai_type", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema data1(
      "data1", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema data2(
      "data2", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema data3(
      "data3", {hustle::catalog::HustleType::CHAR, 3}, true, false);
  hustle::catalog::ColumnSchema data4(
      "data4", {hustle::catalog::HustleType::CHAR, 5}, true, false);
  access_info.addColumn(s_id);
  access_info.addColumn(ai_type);
  access_info.addColumn(data1);
  access_info.addColumn(data2);
  access_info.addColumn(data3);
  access_info.addColumn(data4);

  access_info.setPrimaryKey({});
  ai_schema = access_info.getArrowSchema();

  hustle::catalog::TableSchema special_facility("Special_Facility");
  hustle::catalog::ColumnSchema sf_s_id(
      "sf_s_id", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema sf_sf_type(
      "sf_sf_type", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema is_active(
      "is_active", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema error_cntrl(
      "error_cntrl", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema data_a(
      "data_a", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema data_b(
      "data_b", {hustle::catalog::HustleType::CHAR, 5}, true, false);
  special_facility.addColumn(sf_s_id);
  special_facility.addColumn(sf_sf_type);
  special_facility.addColumn(is_active);
  special_facility.addColumn(error_cntrl);
  special_facility.addColumn(data_a);
  special_facility.addColumn(data_b);
  special_facility.setPrimaryKey({});
  sf_schema = special_facility.getArrowSchema();

  hustle::catalog::TableSchema call_forwarding("Call_Forwarding");
  hustle::catalog::ColumnSchema cf_s_id(
      "cf_s_id", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema cf_sf_type(
      "cf_sf_type", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema start_time(
      "start_time", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema end_time(
      "end_time", {hustle::catalog::HustleType::INTEGER, 0}, true, false);
  hustle::catalog::ColumnSchema numberx(
      "numberx", {hustle::catalog::HustleType::CHAR, 15}, true, false);
  call_forwarding.addColumn(cf_s_id);
  call_forwarding.addColumn(cf_sf_type);
  call_forwarding.addColumn(start_time);
  call_forwarding.addColumn(end_time);
  call_forwarding.addColumn(numberx);
  call_forwarding.setPrimaryKey({});
  cf_schema = call_forwarding.getArrowSchema();
  
  std::shared_ptr<DBTable> s, ai, sf, cf;

  s = std::make_shared<hustle::storage::DBTable>("table", s_schema, BLOCK_SIZE);
  ai = std::make_shared<hustle::storage::DBTable>("table", ai_schema, BLOCK_SIZE);
  sf = std::make_shared<hustle::storage::DBTable>("table", sf_schema, BLOCK_SIZE);
  cf = std::make_shared<hustle::storage::DBTable>("table", cf_schema, BLOCK_SIZE);
  std::filesystem::remove_all("db_directory");
  // EXPECT_FALSE(std::filesystem::exists("db_directory"));

  hustle::HustleDB hustleDB("db_directory2");
  // it will only start if it is not running.
  hustle::HustleDB::startScheduler();

  hustleDB.createTable(subscriber, s);

  hustleDB.createTable(access_info, ai); 

  hustleDB.createTable(special_facility, sf);
  hustleDB.createTable(call_forwarding, cf);

 for (int i  = 9; i < 1000; i++) {
    std::string query =
        "BEGIN TRANSACTION; "
        "INSERT INTO Subscriber VALUES ("+std::to_string(i)+", 'hello"+std::to_string(i)+"', 131321,"
        "131321, 131321,"
        "131321, 131321, 131321, 131321, 131321,"
        "131321, 1131321, 131321, 131321, 131321,"
        "131321, 131321, 131321, 131321, 131321,"
        "131321, 131321, 131321, 131321, 131321,"
        "131321, 131321, 131321, 131321, 131321,"
        "131321, 131321, 131321, 131321);"
        "COMMIT;";
    hustleDB.executeQuery(query);
 }
 for (int i  = 9; i < 1000; i++) {
    std::string query =
        "BEGIN TRANSACTION; "
        "INSERT INTO Access_Info VALUES ("+std::to_string(i)+", 131321,"
        "131321, 131321,"
        "'LOW', 'Great');"
        "COMMIT;";
    hustleDB.executeQuery(query);
 }

 for (int i  = 9; i < 1000; i++) {
    std::string query =
        "BEGIN TRANSACTION; "
        "INSERT INTO Special_Facility VALUES ("+std::to_string(i)+", "+std::to_string(i)+", 131321,"
        "131321, 131321,"
        "'great');"
        "COMMIT;";
    hustleDB.executeQuery(query);
 }

  for (int i  = 9; i < 1000; i++) {
    std::string query =
        "BEGIN TRANSACTION; "
        "INSERT INTO Call_Forwarding VALUES ("+std::to_string(i)+", "+std::to_string(i)+", 131,"
        "131321,"
        "'great');"
        "COMMIT;";
    hustleDB.executeQuery(query);
 }

 std::cout << "Query1 : " << std::endl;
 std::string query1 = "SELECT s_id, sub_nbr,"
    "bit_1, bit_2, bit_3, bit_4, bit_5, bit_6, bit_7,"
    "bit_8, bit_9, bit_10,"
    "hex_1, hex_2, hex_3, hex_4, hex_5, hex_6, hex_7,"
    "hex_8, hex_9, hex_10,"
    "byte2_1, byte2_2, byte2_3, byte2_4, byte2_5,"
    "byte2_6, byte2_7, byte2_8, byte2_9, byte2_10,"
    "msc_location, vlr_location "
    "FROM Subscriber "
    "WHERE s_id=10;";
 auto container = simple_profiler.getContainer();
 container->startEvent("tatp - 1");
 hustleDB.executeQuery(query1);
 container->endEvent("tatp - 1");
 simple_profiler.summarizeToStream(std::cout);
 simple_profiler.clear();


  std::cout << "Query 2" << std::endl;
 std::string query2 =
     "SELECT numberx "
    "FROM Special_Facility, Call_Forwarding "
    "WHERE "
    "cf_s_id=sf_s_id "
    "AND cf_sf_type=sf_sf_type "
    "AND sf_s_id=10 "
    "AND sf_sf_type=10 "
    "AND is_active=131321 "
    "AND start_time\<=1000 "
    "AND end_time\> 1000;";
 container = simple_profiler.getContainer();
 container->startEvent("tatp - 2");
 hustleDB.executeQuery(query2);
 container->endEvent("tatp - 2");
 simple_profiler.summarizeToStream(std::cout);
 simple_profiler.clear();

 std::cout << "Running Query3 : " << std::endl;
 std::string query3 = "SELECT data1, data2, data3, data4 "
    "FROM Access_Info "
    "WHERE s_id=10 "
    "AND ai_type=131321;";
 container = simple_profiler.getContainer();
 container->startEvent("tatp - 3");
 hustleDB.executeQuery(query3);
 container->endEvent("tatp - 3");
 simple_profiler.summarizeToStream(std::cout);
 simple_profiler.clear();

 std::cout << "Running Query4.1 : " << std::endl;
 std::string query4 = "UPDATE Subscriber "
    "SET bit_1=999 "
    "WHERE s_id=10; ";
 container = simple_profiler.getContainer();
 container->startEvent("tatp - 4.1");
 hustleDB.executeQuery(query4);
 container->endEvent("tatp - 4.1");
 simple_profiler.summarizeToStream(std::cout);
 simple_profiler.clear();

 std::cout << "Verify Query 4.1: " << std::endl;
 query4 = "SELECT s_id, sub_nbr,"
    "bit_1, bit_2, bit_3, bit_4, bit_5, bit_6, bit_7,"
    "bit_8, bit_9, bit_10,"
    "hex_1, hex_2, hex_3, hex_4, hex_5, hex_6, hex_7,"
    "hex_8, hex_9, hex_10,"
    "byte2_1, byte2_2, byte2_3, byte2_4, byte2_5,"
    "byte2_6, byte2_7, byte2_8, byte2_9, byte2_10,"
    "msc_location, vlr_location "
    "FROM Subscriber "
    "WHERE s_id=10;";
 hustleDB.executeQuery(query4);

 std::cout << "Running Query4.2 : " << std::endl;
 query4 = "UPDATE Special_Facility "
            "SET data_a = 999 "
            "WHERE sf_s_id=10 "
            "AND sf_sf_type=10"; 
 container = simple_profiler.getContainer();
 container->startEvent("tatp - 4.2");
 hustleDB.executeQuery(query4);
 container->endEvent("tatp - 4.2");
 simple_profiler.summarizeToStream(std::cout);
 simple_profiler.clear();

 std::cout << "Verify Query 4.2: " << std::endl;
 query4 = "SELECT sf_s_id, sf_sf_type,"
    "data_a "
    "FROM Special_Facility "
    "WHERE sf_s_id=10 AND sf_sf_type=10;";
 hustleDB.executeQuery(query4);

 std::cout << "Updating Query 5: " << std::endl;
 std::string query5 = "UPDATE Subscriber"
    " SET  vlr_location=50 "
    "WHERE sub_nbr='hello10';";
 container = simple_profiler.getContainer();
 container->startEvent("tatp - 5");
 hustleDB.executeQuery(query5);
 container->endEvent("tatp - 5");
 simple_profiler.summarizeToStream(std::cout);
 simple_profiler.clear();
std::cout << "Verify Query 5: " << std::endl;
 query4 = "SELECT s_id, sub_nbr,"
    "bit_1, bit_2, bit_3, bit_4, bit_5, bit_6, bit_7,"
    "bit_8, bit_9, bit_10,"
    "hex_1, hex_2, hex_3, hex_4, hex_5, hex_6, hex_7,"
    "hex_8, hex_9, hex_10,"
    "byte2_1, byte2_2, byte2_3, byte2_4, byte2_5,"
    "byte2_6, byte2_7, byte2_8, byte2_9, byte2_10,"
    "msc_location, vlr_location "
    "FROM Subscriber "
    "WHERE s_id=10;";
 hustleDB.executeQuery(query5);

 std::cout << "Query 6" << std::endl;
 std::string query6 =
        "BEGIN TRANSACTION; "
        "INSERT INTO Call_Forwarding VALUES (1111111, 1111111, 131321,"
        "131321,"
        "'great');"
        "COMMIT;";
 container = simple_profiler.getContainer();
 container->startEvent("tatp - 6");
 hustleDB.executeQuery(query6);
 container->endEvent("tatp - 6");
 simple_profiler.summarizeToStream(std::cout);
 simple_profiler.clear();
 std::cout << "Verify Query 6: " << std::endl;
 query6 = "SELECT cf_s_id, start_time, end_time "
    "FROM Call_Forwarding "
    "WHERE cf_s_id=1111111;";
 hustleDB.executeQuery(query6);

 std::cout << "Query 7" << std::endl;
 std::string query7 =
        "DELETE FROM Call_Forwarding "
        "WHERE cf_s_id = 1111111;";
 container = simple_profiler.getContainer();
 container->startEvent("tatp - 7");
 hustleDB.executeQuery(query5);
 container->endEvent("tatp - 7");
 simple_profiler.summarizeToStream(std::cout);
 simple_profiler.clear();
 std::cout << "Verify Query 7: " << std::endl;
 query7 = "SELECT cf_s_id, start_time, end_time "
    "FROM Call_Forwarding "
    "WHERE cf_s_id=1111111;";
 hustleDB.executeQuery(query7);


 hustle::HustleDB::stopScheduler();
 
}


void TATP::RunBenchmark() {

}

void TATP::RunQuery1() {
    
}

void TATP::RunQuery2() {
    
}


void TATP::RunQuery3() {
    
}

void TATP::RunQuery4() {
    
}


void TATP::RunQuery5() {
    
}

void TATP::RunQuery6() {
    
}

void TATP::RunQuery7() {
    
}



}