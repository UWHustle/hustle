#ifndef TXBENCH_BENCHMARKS_TATP_HUSTLE_LOADER_H_
#define TXBENCH_BENCHMARKS_TATP_HUSTLE_LOADER_H_

#include "txbench/loader.h"
#include "api/hustle_db.h"
#include "catalog/catalog.h"
#include "catalog/column_schema.h"
#include "catalog/table_schema.h"
#include "sqlite3/sqlite3.h"
#include "storage/util.h"

namespace txbench {

class TATPHustleLoader : public Loader {
public:
  TATPHustleLoader(int n_rows, const std::string &host, int port,
                  const std::string &user);

  void load() override;

private:
  int n_rows_;
};

} // namespace txbench

#endif // TXBENCH_BENCHMARKS_TATP_HUSTLE_LOADER_H_
