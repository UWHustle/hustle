// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_INCLUDE_STATUS_H_
#define BITWEAVING_INCLUDE_STATUS_H_

#include <string>
#include <sstream>

namespace hustle::bitweaving {

/**
 * @brief Class of a returned status indicating success or failure.
 */
class Status {

public:
  /**
   * @brief Constructor for a success status.
   */
  Status() : message_(NULL) { }

  /**
   * @brief Destructor.
   */
  ~Status() { delete message_; }

  /**
   * @brief Assignment operator.
   * @param status Assignment operand.
   */
  void operator=(const Status & status) {
    delete message_;
    message_ = status.message_ == NULL ?
        NULL : new StatusMessage(status.message_);
  }

  /**
   * @brief Return a success status.
   * @return A status indicating success.
   */
  static Status OK() { return Status(); }

  /**
   * @brief Return a status indicating an error of invalid argument.
   * @param m Error message.
   * @return A status indicating an error of invalid argument.
   */
  static Status InvalidArgument(std::string m) { return Status(kInvalidArgument, m); }

  /**
   * @brief Return a status indicating an usage error.
   * @param m Error message.
   * @return A status indicating an usage error.
   */
  static Status UsageError(std::string m) { return Status(kUsageError, m); }

  /**
   * @brief Return a status indicating an I/O error.
   * @param filename The name of error file.
   * @param error_code The error code of I/O error.
   * @return A status indicating an I/O error.
   */
  static Status IOError(std::string filename, int error_code) {
    std::ostringstream convert;
    convert << error_code;
    std::string error_code_string = convert.str();
    return Status(kIOError, filename + " Error: " + error_code_string);
  }

  static Status InvalidBitwidth(std::string m){
      return Status(kInvalidbitwidth, m);
  }

  /**
   * @brief Check if this status is success.
   * @return True iff this status is success.
   */
  bool IsOk() { return message_ == NULL; }

private:
  enum StatusCode {
    kOk,
    kNotSupported,
    kInvalidArgument,
    kUsageError,
    kIOError,
    kInvalidbitwidth

  };

  struct StatusMessage {
    StatusCode code;
    std::string message;

    StatusMessage(StatusMessage * sm) :
      code(sm->code), message(sm->message) { }
    StatusMessage(StatusCode c, std::string m) :
      code(c), message(m) { }
  };

  Status(StatusCode code, std::string message) :
    message_(new StatusMessage(code, message)) {}

  StatusMessage * message_;
};

} // namespace bitweaving

#endif // BITWEAVING_INCLUDE_STATUS_H_
