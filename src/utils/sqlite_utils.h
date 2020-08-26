#ifndef HUSTLE_SQLITE_UTILS_H
#define HUSTLE_SQLITE_UTILS_H

namespace hustle {
namespace utils {

// Executes the sql query specified in sql on the database at sqlitePath,
// no output is returned.
bool executeSqliteNoOutput(const std::string &sqlitePath,
                           const std::string &sql);

// Executes the sql query specified in sql on the database at sqlitePath,
// the output is returned as a string.
std::string executeSqliteReturnOutputString(const std::string &sqlitePath,
                                            const std::string &sql);

}  // namespace utils
}  // namespace hustle
#endif  // HUSTLE_SQLITE_UTILS_H
