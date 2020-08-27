//
// Created by Nicholas Corrado on 7/27/20.
//

#ifndef HUSTLE_SKEW_H
#define HUSTLE_SKEW_H

#include <arrow/api.h>

#include "scheduler/scheduler.h"

void skew_column_internal(int i, std::shared_ptr<arrow::ChunkedArray> col) {
  auto chunk = col->chunk(i);
  auto chunk_length = chunk->length();
  // Assume we are skewing int64 column
  auto chunk_data = chunk->data()->GetMutableValues<int64_t>(1, 0);

  //        std::shared_ptr<arrow::Buffer> buffer;
  //        arrow::AllocateBuffer(chunk_length).Value(&buffer);
  //        auto out_data = buffer->mutable_data();

  // LIP batch size = # of threads
  auto batch_size = 8;

  if (i % (2 * batch_size) < batch_size) {
    for (auto j = 0; j < chunk_length; ++j) {
      //                chunk_data[j] = 15; // 3.1
      chunk_data[j] = 19;  // 3.2
                           //                chunk_data[j] = 147; // 3.3
    }
  } else {
    for (auto j = 0; j < chunk_length; ++j) {
      chunk_data[j] = -1;
    }
  }
}

void skew_column(std::shared_ptr<arrow::ChunkedArray> col, bool write_to_file) {
  auto& scheduler = hustle::Scheduler::GlobalInstance();

  auto num_chunks = col->num_chunks();
  for (auto i = 0; i < num_chunks; ++i) {
    scheduler.addTask(hustle::CreateLambdaTask(
        [i, &col]() { skew_column_internal(i, col); }));
  }

  scheduler.start();
  scheduler.join();
  //        if (i%(5*batch_size) < batch_size) {
  //            for (auto j=0; j<chunk_length; ++j) {
  ////                chunk_data[j] = 15; // 3.1 This one won't show much
  /// improvement because the most selective filter is just 1/5
  //                chunk_data[j] = 19; // 3.2
  ////                chunk_data[j] = 147; // 3.3
  //            }
  //        } else{
  //            for (auto j=0; j<chunk_length; ++j) {
  //                chunk_data[j] = -1;
  //            }
  //        }
}

void skew_column(std::shared_ptr<arrow::ChunkedArray> col, double val) {
  auto num_chunks = col->num_chunks();

  for (auto i = 0; i < num_chunks; ++i) {
    auto chunk = col->chunk(i);
    auto chunk_length = chunk->length();
    // Assume we are skewing int64 column
    auto chunk_data = chunk->data()->GetMutableValues<int64_t>(1);

    for (auto j = 0; j < chunk_length; ++j) {
      chunk_data[j] = val;
    }
  }
}

#endif  // HUSTLE_SKEW_H
