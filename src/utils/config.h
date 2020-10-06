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

#ifndef HUSTLE_CONFIG_H
#define HUSTLE_CONFIG_H

#include <stdlib.h>

#include <libconfig.h++>
#include <string>

class Config {
 public:
  static void Init(const char* filename) { GetInstance(filename); }

  static Config& GetInstance(const char* filename = nullptr) {
    static Config instance(filename);
    return instance;
  }

  double GetDoubleValue(const char* name) {
    double value;
    cfg.lookupValue(name, value);
    return value;
  }

 private:
  libconfig::Config cfg;
  const char* filename_;

  Config(const char* filename) : filename_(filename) {
    try {
      cfg.readFile(filename);
    } catch (const libconfig::FileIOException& io) {
      std::cerr << "[FileIOException] Error in reading this file:" << filename
                << std::endl;
    } catch (const libconfig::ParseException& pe) {
      std::cerr << "[ParseException] error in file" << pe.getFile() << ":"
                << pe.getLine() << std::endl;
    }
  }
};

#endif  // HUSTLE_CONFIG_H
