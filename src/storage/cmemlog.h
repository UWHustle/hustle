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

typedef struct DBRecord DBRecord;
typedef struct DBRecordList DBRecordList;

typedef struct {
  DBRecordList *record_list;
  int curr_size;
  int total_size;
} MemLog;

void initialize_memlog(MemLog **mem_log, int initial_size) {
  printf("Initialize memlog!");
  *mem_log = (MemLog*)malloc(sizeof(MemLog)); 
  (*mem_log)->record_list = (DBRecordList *)malloc(initial_size * sizeof(DBRecordList));
  (*mem_log)->total_size = initial_size;
  int table_index = 0;
  while (table_index < (*mem_log)->total_size) {
    (*mem_log)->record_list[table_index].head = NULL;
    (*mem_log)->record_list[table_index].tail = NULL;
    (*mem_log)->record_list[table_index].curr_size = 0;
    table_index++;
  }
}


DBRecord* create_record(const void *data, int nData) {
    DBRecord* record = (DBRecord*) malloc(sizeof(DBRecord));
    record->data = data;
    record->nData = nData;
    record->next_record = NULL;
    return record;
}

void insert_record(MemLog *mem_log, DBRecord *record, int table_id) {
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
  printf("insert record data: %s\n", record->data);
}

void update_mem_db(MemLog *mem_log) {
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
}


void update_mem_db_free(MemLog *mem_log) {
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
    table_index++;
  }
  free(mem_log->record_list);
  mem_log->record_list = NULL;
  mem_log->total_size = 0;
}


void free_memlog(MemLog *mem_log) {
  struct DBRecord *tmp_record;
  int table_index = 0;
  while (table_index < mem_log->total_size) {
    struct DBRecord *head = mem_log->record_list[table_index].head;
    while (head != NULL) {
      tmp_record = head;
      head = head->next_record;
      free(tmp_record);
    }
    table_index++;
  }

  free(mem_log->record_list);
  mem_log->record_list = NULL;
  mem_log->total_size = 0;
}