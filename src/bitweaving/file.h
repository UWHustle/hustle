// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_SRC_FILE_H_
#define BITWEAVING_SRC_FILE_H_

#include <cstdio>
#include <string>

#include "types.h"
#include "status.h"

namespace hustle::bitweaving {

/**
 * @brief Class for sequentially reading a file.
 */
class SequentialReadFile {
public:
  /**
   * @brief Constructor.
   */
  SequentialReadFile() {}

  /**
   * @brief Destructor.
   */
  ~SequentialReadFile() {}

  /**
   * @brief Open a file.
   * @param fname The name of the file.
   * @return A Status object to indicate success or failure.
   */
  Status Open(const std::string& fname);

  /**
   * @brief Read a chunk of data from this file.
   * @param data Load data into this position.
   * @param size The size of the chunk.
   * @return A Status object to indicate success or failure.
   */
  Status Read(char* data, size_t size);

  /**
   * @brief Close this file.
   * @return A Status object to indicate success or failure.
   */
  Status Close();

  /**
   * @brief Check if reach the end of this file.
   * @return True iff reach the end of this file.
   */
  bool IsEnd();

private:
  std::string filename_;
  FILE * file_;
};

/**
 * @brief Class for sequentially writing a file.
 */
class SequentialWriteFile {
public:
  /**
   * @brief Constructor.
   */
  SequentialWriteFile() {}

  /**
   * @brief Destructor.
   */
  ~SequentialWriteFile() {}

  /**
   * @brief Open a file.
   * @param fname The name of the file.
   * @return A Status object to indicate success or failure.
   */
  Status Open(const std::string& fname);

  /**
   * @brief Append a chunk of data to this file.
   * @param data The starting position of the data chunk.
   * @param size The size of the chunk.
   * @return A Status object to indicate success or failure.
   */
  Status Append(const char* data, size_t size);

  /**
   * @brief Flush the data into this file.
   * @return A Status object to indicate success or failure.
   */
  Status Flush();

  /**
   * @brief Cloase this file.
   * @return A Status object to indicate success or failure.
   */
  Status Close();

private:
  std::string filename_;
  FILE * file_;
};

} // namespace bitweaving

#endif // BITWEAVING_SRC_FILE_H_
