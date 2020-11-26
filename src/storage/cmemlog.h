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

#include <stdlib.h>
#include <stdio.h>

#define MEMLOG_INIT_SIZE 100
#define MEMLOG_OK 0
#define MEMLOG_ERROR 1

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
  DBRecordList *record_list;
  int total_size;
} HustleMemLog;

Status hustle_memlog_initialize(HustleMemLog **mem_log, int initial_size) {
  if (initial_size <= 0) {
      return MEMLOG_ERROR;
  }
  *mem_log = (HustleMemLog*)malloc(sizeof(HustleMemLog)); 
  (*mem_log)->record_list = (DBRecordList *)malloc(initial_size * sizeof(DBRecordList));
  (*mem_log)->total_size = initial_size;
  int table_index = 0;
  while (table_index < (*mem_log)->total_size) {
    (*mem_log)->record_list[table_index].head = NULL;
    (*mem_log)->record_list[table_index].tail = NULL;
    (*mem_log)->record_list[table_index].curr_size = 0;
    table_index++;
  }
  return MEMLOG_OK;
}


DBRecord* hustle_memlog_create_record(const void *data, int nData) {
    if (data == NULL) {
      return NULL;
    }
    DBRecord* record = (DBRecord*) malloc(sizeof(DBRecord));
    record->data = data;
    record->nData = nData;
    record->next_record = NULL;
    return record;
}

Status hustle_memlog_insert_record(HustleMemLog *mem_log, DBRecord *record, int table_id) {
  if (mem_log == NULL || record == NULL) {
    return MEMLOG_ERROR;
  }
  if (table_id >= mem_log->total_size) {
    int old_table_list_size = mem_log->total_size;
    mem_log->total_size *= 2;
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

DBRecordList* hustle_memlog_get_records(HustleMemLog *mem_log, int table_id) {
  if (mem_log == NULL) {
    return NULL;
  }
  return &mem_log->record_list[table_id];
}

Status hustle_memlog_update_db(HustleMemLog *mem_log) {
  if (mem_log == NULL) {
    return MEMLOG_ERROR;
  }
  int table_index = 0;
  struct DBRecord *tmp_record;
  while (table_index < mem_log->total_size) {
    struct DBRecord *head = mem_log->record_list[table_index].head;
    while (head != NULL) {
      tmp_record = head;
      head = head->next_record;
      // Todo: (@suryadev) update arrow arrays
    }
    table_index++;
  }
  return MEMLOG_OK;
}


Status hustle_memlog_update_db_free(HustleMemLog *mem_log) {
  if (mem_log == NULL) {
    return MEMLOG_ERROR;
  }
  int table_index = 0;
  struct DBRecord *tmp_record;
  while (table_index < mem_log->total_size) {
    struct DBRecord *head = mem_log->record_list[table_index].head;
    while (head != NULL) {
      tmp_record = head;
      head = head->next_record;
      // Todo: (@suryadev) update arrow arrays

      free(tmp_record);
    }
    mem_log->record_list[table_index].head = NULL;
    mem_log->record_list[table_index].tail = NULL;
    mem_log->record_list[table_index].curr_size = 0;
    table_index++;
  }
  return MEMLOG_OK;
}

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

Status hustle_memlog_free(HustleMemLog *mem_log) {
  if (mem_log == NULL) {
    return MEMLOG_ERROR;
  }
  hustle_memlog_clear(mem_log);

  free(mem_log->record_list);
  free(mem_log);
  return MEMLOG_OK;
}
