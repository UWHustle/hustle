// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_SOURCE_ENV_H_
#define BITWEAVING_SOURCE_ENV_H_

#include <string>

#include "types.h"
#include "status.h"

namespace hustle::bitweaving {

/**
 * @brief Class for operations in the file system.
 */
class Env {
public:
  /**
   * @brief Check if the file exists in the file system.
   * @param filename The file name.
   * @return True iff the file exists.
   */
  bool IsFileExist(std::string filename);

  /**
   * @brief Check if the directory exists in the file system.
   * @param path The location of the directory.
   * @return True iff the directory exists.
   */
  bool IsDirectoryExist(std::string path);

  /**
   * @brief Create a path of a file in the file system.
   * @param directory The location of the directory.
   * @param file The name of the file.
   * @param path The generated path of the file.
   * @return A Status object to indicate success or failure.
   */
  Status FilePath(std::string directory, std::string file, std::string & path);

  /**
   * @brief Create a directory in the file system.
   * @param path The location of the directory.
   * @return A Status object to indicate success or failure.
   */
  Status CreateDirectory(const std::string path);

  /**
   * @brief Delete a directory and all files in this directory.
   * @param path The location of the directory.
   * @return A Status object to indicate success or failure.
   */
  Status DeleteDirectory(const std::string path);

  /**
   * @brief Create all directories in a path in the file system.
   * @param path The location of the directory.
   * @return A status object to indicate success or failure.
   */
  Status CreateDirectoryPath(std::string path);
};

} // namespace bitweaving

#endif // BITWEAVING_SOURCE_ENV_H_
