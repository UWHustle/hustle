#ifndef TXBENCH_BENCHMARKS_TATP_MYSQL_TATP_MYSQL_LOADER_H_
#define TXBENCH_BENCHMARKS_TATP_MYSQL_TATP_MYSQL_LOADER_H_

#include "loader.h"
#include <mysqlx/xdevapi.h>

namespace txbench {

class TATPMySQLLoader : public Loader {
public:
  TATPMySQLLoader(int n_rows, const std::string &host, int port,
                  const std::string &user);

  void load() override;

private:
  int n_rows_;

  mysqlx::Session session_;
};

} // namespace txbench

#endif // TXBENCH_BENCHMARKS_TATP_MYSQL_TATP_MYSQL_LOADER_H_
