// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_INCLUDE_OPTIONS_H_
#define BITWEAVING_INCLUDE_OPTIONS_H_

#include "types.h"

namespace hustle::bitweaving {

/**
 * @brief Structure of options to control the table.
 */
struct Options {
public:
  /**
   * @brief Flag to ignore exist files in the file system.
   */
  bool delete_exist_files;
  /**
   * @brief Flag to disable I/O in the file system.
   */
  bool in_memory;

  /**
   * @brief Constructor for a default options.
   */
  Options();
};

} // namespace bitweaving

#endif // BITWEAVING_INCLUDE_OPTIONS_H_
