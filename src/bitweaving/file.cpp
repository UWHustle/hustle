// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <string>
#include <cerrno>
#include "file.h"

namespace hustle::bitweaving {

Status SequentialReadFile::Open(const std::string& filename)
{
  filename_ = filename;
  file_ = fopen (filename_.c_str(),"rb");
  if (file_ == NULL) {
    return Status::IOError(filename_, errno);
  }
  return Status::OK();
}

Status SequentialReadFile::Read(char* data, size_t size)
{
  size_t count = fread(data, sizeof(char), size, file_);
  if (count != size) {
    return Status::IOError(filename_, errno);
  }
  return Status::OK();
}

Status SequentialReadFile::Close()
{
  if (fclose(file_) != 0) {
    return Status::IOError(filename_, errno);
  }
  return Status::OK();
}

bool SequentialReadFile::IsEnd()
{
  return feof(file_);
}

Status SequentialWriteFile::Open(const std::string& filename)
{
  filename_ = filename;
  file_ = fopen(filename_.c_str(),"wb");
  if (file_ == NULL) {
    return Status::IOError(filename_, errno);
  }
  return Status::OK();
}

Status SequentialWriteFile::Append(const char* data, size_t size)
{
  size_t count = fwrite(data, sizeof(char), size, file_);
  if (count != size) {
    return Status::IOError(filename_, errno);
  }
  return Status::OK();
}

Status SequentialWriteFile::Flush()
{
  if (fflush(file_) != 0) {
    return Status::IOError(filename_, errno);
  }
  return Status::OK();
}

Status SequentialWriteFile::Close()
{
  if (fclose(file_) != 0) {
    return Status::IOError(filename_, errno);
  }
  return Status::OK();
}

} // namespace bitweaving
