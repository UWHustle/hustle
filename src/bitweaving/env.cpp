// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <string>
#include <map>
#include <cerrno>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "env.h"

namespace hustle::bitweaving {

bool Env::IsFileExist(std::string filename)
{
  return access(filename.c_str(), F_OK) == 0;
}

bool Env::IsDirectoryExist(std::string path)
{
  struct stat st;
  return stat(path.c_str(), &st) == 0;
}

Status Env::FilePath(std::string directory, std::string file, std::string & path)
{
  if (file.find('/') != std::string::npos) {
    return Status::InvalidArgument("file name contains '/'.");
  }
  if (directory.length() == 0) {
    return Status::InvalidArgument("empty directory name.");
  }
  if (directory[directory.length() - 1] == '/') {
    path = directory + file;
  } else {
    path = directory + "/" + file;
  }
  return Status::OK();
}

Status Env::CreateDirectory(const std::string path)
{
  if (IsDirectoryExist(path)) {
    return Status::OK();
  }

  if (mkdir(path.c_str(), 0755) != 0) {
    return Status::IOError(path, errno);
  }
  return Status::OK();
}

Status Env::DeleteDirectory(const std::string path)
{
  if (rmdir(path.c_str()) != 0) {
    return Status::IOError(path, errno);
  }
  return Status::OK();
};

Status Env::CreateDirectoryPath(std::string path)
{
  size_t start = 0;
  size_t pos = 0;
  Status status;
  while ((pos = path.find('/', start)) != std::string::npos) {
    if (pos != start) {
      status = CreateDirectory(path.substr(0, pos));
      if (!status.IsOk())
        return status;
    }
    start = pos + 1;
  }
  return CreateDirectory(path);
}

} // namespace bitweaving
