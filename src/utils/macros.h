#ifndef PROJECT_UTILITY_MACROS_HPP_
#define PROJECT_UTILITY_MACROS_HPP_

#include <iostream>

#define DISALLOW_COPY_AND_ASSIGN(classname)  \
  classname(const classname &orig) = delete; \
  classname &operator=(const classname &rhs) = delete

#endif  // PROJECT_UTILITY_MACROS_HPP_
