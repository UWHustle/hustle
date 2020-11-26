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
#ifdef __cplusplus
extern "C" {
#endif

#define MEMLOG_INIT_SIZE 100
#define MEMLOG_OK 0
#define MEMLOG_ERROR 1

/**
 * MemLog is a temporary in-memory store used by the SQLite write transaction
 * to record the updates and during commit, the records in the Memlog are 
 * updated to the arrow array.
 * 
 * The store is implemented as a array of linked lists, where each slot in the array
 * represents a table and identified by the table id (SQLite's root page id).
 * */

struct DBRecordList {
    struct DBRecord *head;
    struct DBRecord *tail;
    int curr_size;
};

struct DBRecord {
  const void *data;
  int nData;
  struct DBRecord *next_record;
};

typedef int Status;
typedef struct DBRecord DBRecord;
typedef struct DBRecordList DBRecordList;

typedef struct {
  DBRecordList *record_list; // array of Linked List, each slot belongs to a table
  int total_size;
} HustleMemLog;


/**
 * Initialize the memlog for each sqlite db connection
 * mem_log - pointer to the memlog
 * initial_size - the initial array size of the store 
 * */
Status hustle_memlog_initialize(HustleMemLog **mem_log, int initial_size);

/**
 * Create a DBRecord - each node in the linkedlist.
 * data - SQLite's data record format with header in the begining
 * nData - the size of the data
 * */
DBRecord* hustle_memlog_create_record(const void *data, int nData);

/**
 * Insert's the record to the memlog and grows the array size, if the table id
 * is greater than the array size.
 * 
 * mem_log - pointer to the memlog
 * record - DBRecord needs to be inserted
 * table_id - root page id of the table
 * */
Status hustle_memlog_insert_record(HustleMemLog *mem_log, DBRecord *record, int table_id) ;

/**
 * Iterate through all the records for a table in the memlog.
 * 
 * mem_log - pointer to the memlog
 * table_id - root page id of the table
 * */
DBRecordList* hustle_memlog_get_records(HustleMemLog *mem_log, int table_id);


/**
 * Update the arrow array with the records present in the memlog
 * 
 * mem_log - pointer to the memlog
 * */
Status hustle_memlog_update_db(HustleMemLog *mem_log);

/**
 * Update the arrow array with the records present in the memlog
 * and free the records in the memlog.
 * 
 * mem_log - pointer to the memlog
 * */
Status hustle_memlog_update_db_free(HustleMemLog *mem_log);

/**
 * Make the memlog contents empty by clearing/freeing up
 * the records in the memlog.
 * 
 * mem_log - pointer to the memlog
 * */
Status hustle_memlog_clear(HustleMemLog *mem_log);

/**
 * Free the memlog, usually used when we close the
 * sqlite db connection.
 * 
 * mem_log - pointer to the memlog
 * */
Status hustle_memlog_free(HustleMemLog *mem_log);

#ifdef __cplusplus
}
#endif