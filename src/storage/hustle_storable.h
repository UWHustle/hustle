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

#ifndef HUSTLE_SRC_STORAGE_INTERFACES_HUSTLE_STORABLE
#define HUSTLE_SRC_STORAGE_INTERFACES_HUSTLE_STORABLE

namespace hustle::storage {

/**
 * sqlite3 callback type
 * Used by multiple classes within the hustle
 * storage module.
 */
typedef int (*sqlite3_callback)(void *, int, char **, char **);

/**
 * Signaling enum to communicate general execution state to a hustle storable.
 * There is no guarantee that a particular object will act on a particular
 * signal.
 */
enum HustleStorableSignal { QUERY_BEGIN, QUERY_END };

/**
 * Abstract Class.
 * Base class that all hustle-related objects implement.
 * In practice, other interfaces will derive this class.
 */
class HustleStorable {
 public:
  virtual ~HustleStorable() = 0;
  virtual void ReceiveSignal(HustleStorableSignal signal) = 0;
};

}  // namespace hustle::storage
#endif  // HUSTLE_SRC_STORAGE_INTERFACES_HUSTLE_STORABLE
