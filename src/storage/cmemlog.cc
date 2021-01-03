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

#include "cmemlog.h"

#include <arrow/io/api.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <iostream>
#include <map>
#include <mutex>
#include <string>

#include "api/hustle_db.h"
#include "record_transformer.h"

// Maps the Btree root id to hustle table id
static std::map<int, std::map<int, std::string>> table_map;
static std::mutex instance_lock;

void memlog_add_table_mapping(int db_id, int root_page_id, char *table_name) {
  std::lock_guard<std::mutex> lock(instance_lock);
  table_map[db_id][root_page_id] = std::string(table_name);
}

/**
 * Initialize the memlog for each sqlite db connection
 * mem_log - double-pointer to the memlog
 * initial_size - the initial array size of the store
 * */
Status hustle_memlog_initialize(HustleMemLog **mem_log, char *db_name,
                                int initial_size) {
  if (initial_size <= 0) {
    return MEMLOG_ERROR;
  }
  *mem_log = (HustleMemLog *)malloc(sizeof(HustleMemLog));
  (*mem_log)->record_list =
      (DBRecordList *)malloc(initial_size * sizeof(DBRecordList));
  (*mem_log)->total_size = initial_size;
  (*mem_log)->db_name = db_name;
  int table_index = 0;
  while (table_index < (*mem_log)->total_size) {
    (*mem_log)->record_list[table_index].head = NULL;
    (*mem_log)->record_list[table_index].tail = NULL;
    (*mem_log)->record_list[table_index].curr_size = 0;
    table_index++;
  }
  return MEMLOG_OK;
}

/**
 * Create a DBRecord - each node in the linkedlist.
 * data - SQLite's data record format with header in the begining
 * nData - the size of the data
 * */
DBRecord *hustle_memlog_create_record(int mode, int rowId, const void *data,
                                      int nData) {
  if (data == NULL && mode != MEMLOG_HUSTLE_DELETE) {
    return NULL;
  }
  DBRecord *record = (DBRecord *)malloc(sizeof(DBRecord));
  record->mode = mode;
  record->rowId = rowId;
  record->data = (const void *)malloc(nData);
  memcpy((void *)record->data, data, nData);
  // record->data = data;
  record->nData = nData;
  record->next_record = NULL;
  return record;
}

/**
 * Insert's the record to the memlog and grows the array size, if the table id
 * is greater than the array size.
 *
 * mem_log - pointer to the memlog
 * record - DBRecord needs to be inserted
 * table_id - root page id of the table
 * */
Status hustle_memlog_insert_record(HustleMemLog *mem_log, DBRecord *record,
                                   int table_id) {
  if (mem_log == NULL ||
      (record == NULL && record->mode != MEMLOG_HUSTLE_DELETE)) {
    return MEMLOG_ERROR;
  }
  if (table_id >= mem_log->total_size) {
    int old_table_list_size = mem_log->total_size;
    mem_log->total_size = 2 * table_id;
    mem_log->record_list = (DBRecordList *)realloc(
        mem_log->record_list, mem_log->total_size * sizeof(DBRecordList));
    int table_index = old_table_list_size;
    while (table_index < mem_log->total_size) {
      mem_log->record_list[table_index].head = NULL;
      mem_log->record_list[table_index].tail = NULL;
      mem_log->record_list[table_index].curr_size = 0;
      table_index++;
    }
  }

  DBRecord *tail = mem_log->record_list[table_id].tail;
  mem_log->record_list[table_id].tail = record;
  if (tail != NULL) {
    tail->next_record = record;
  }
  if (mem_log->record_list[table_id].head == NULL) {
    mem_log->record_list[table_id].head = record;
  }
  mem_log->record_list[table_id].curr_size += 1;
  return MEMLOG_OK;
}

/**
 * Iterate through all the records for a table in the memlog.
 *
 * mem_log - pointer to the memlog
 * table_id - root page id of the table
 * */
DBRecordList *hustle_memlog_get_records(HustleMemLog *mem_log, int table_id) {
  if (mem_log == NULL || table_id >= mem_log->total_size) {
    return NULL;
  }
  return &mem_log->record_list[table_id];
}

/**
 * Update the arrow array with the records present in the memlog
 * and free the records in the memlog.
 *
 * mem_log - pointer to the memlog
 * is_free - whether to free the records after updating
 * */
Status hustle_memlog_update_db(HustleMemLog *mem_log, int is_free) {
  if (mem_log == NULL) {
    return MEMLOG_ERROR;
  }
  using namespace hustle;

  std::shared_ptr<catalog::Catalog> catalog =
      HustleDB::getCatalog(mem_log->db_name);

  int table_index = 0;
  struct DBRecord *tmp_record;
  while (table_index < mem_log->total_size) {
    struct DBRecord *head = mem_log->record_list[table_index].head;
    auto search = table_map[DEFAULT_DB_ID].find(table_index);
    if (search == table_map[DEFAULT_DB_ID].end()) {
      table_index++;
      continue;
    }

    auto table =
        catalog->getTable(table_map[DEFAULT_DB_ID][table_index].c_str());
    if (table == nullptr) {
      table_index++;
      continue;
    }
    while (head != NULL) {
      tmp_record = head;

      if (head->mode == MEMLOG_HUSTLE_DELETE) {
        table->delete_record_table(head->rowId);
      } else {
        u32 hdrLen;
        // Read header len in the record
        u32 nBytes = getVarint32((const unsigned char *)head->data, hdrLen);
        u32 idx = nBytes;
        const unsigned char *hdr = (const unsigned char *)head->data;
        std::vector<int32_t> byte_widths;
        /* read the col width from the record and user serialTypeLen
            to convert to exact col width */
        while (idx < hdrLen) {
          u32 typeLen;
          nBytes = getVarint32(((const unsigned char *)hdr + idx), typeLen);
          byte_widths.emplace_back(serialTypeLen(typeLen));
          idx += nBytes;
        }

        // Todo: (@suryadev) handle update and delete
        uint8_t *record_data = (uint8_t *)head->data;
        if (table != nullptr) {
          size_t len = byte_widths.size();
          int32_t widths[len];
          for (size_t i = 0; i < len; i++) {
            widths[i] = byte_widths[i];
          }
          // Insert record to the arrow table
          if (head->mode == MEMLOG_HUSTLE_INSERT) {
            //std::cout << "Insert record " << std::endl;
            table->insert_record_table(head->rowId, record_data + hdrLen,
                                       widths);
          } else if (head->mode == MEMLOG_HUSTLE_UPDATE) {
            //std::cout << "Update record " << std::endl;
            table->update_record_table(head->rowId, head->nUpdateMetaInfo,
                                       head->updateMetaInfo,
                                       record_data + hdrLen, widths);
          }
        }
      }

      head = head->next_record;
      if (is_free) {
        uint8_t *record_data = (uint8_t *)tmp_record->data;
        free(record_data);
        free(tmp_record);
      }
    }
    mem_log->record_list[table_index].head = NULL;
    mem_log->record_list[table_index].tail = NULL;
    mem_log->record_list[table_index].curr_size = 0;
    table_index++;
  }
  return MEMLOG_OK;
}

/**
 * Make the memlog contents empty by clearing/freeing up
 * the records in the memlog.
 *
 * mem_log - pointer to the memlog
 * */
Status hustle_memlog_clear(HustleMemLog *mem_log) {
  if (mem_log == NULL) {
    return MEMLOG_ERROR;
  }
  struct DBRecord *tmp_record;
  int table_index = 0;
  while (table_index < mem_log->total_size) {
    struct DBRecord *head = mem_log->record_list[table_index].head;
    while (head != NULL) {
      tmp_record = head;
      head = head->next_record;
      free(tmp_record);
    }
    mem_log->record_list[table_index].head = NULL;
    mem_log->record_list[table_index].tail = NULL;
    mem_log->record_list[table_index].curr_size = 0;
    table_index++;
  }
  return MEMLOG_OK;
}

/**
 * Free the memlog, usually used when we close the
 * sqlite db connection.
 *
 * mem_log - pointer to the memlog
 * */
Status hustle_memlog_free(HustleMemLog *mem_log) {
  if (mem_log == NULL) {
    return MEMLOG_ERROR;
  }
  hustle_memlog_clear(mem_log);

  free(mem_log->record_list);
  free(mem_log);
  return MEMLOG_OK;
}
