#include "tatp_mysql_benchmark.h"
#include <iostream>

int main(int argc, char **argv) {
  int n_workers = 1;
  int warmup_duration = 5;
  int measurement_duration = 10;
  int n_rows = 1000;

  if (argc == 5) {
    n_workers = std::stoi(argv[1]);
    warmup_duration = std::stoi(argv[2]);
    measurement_duration = std::stoi(argv[3]);
    n_rows = std::stoi(argv[4]);
  }

   std::cout << "n_workers: " << n_workers << "\n"
	     << "warmup_duration: " << warmup_duration << "\n"
             << "measurement_duration: " << measurement_duration << "\n"
	     << "n_rows: " << n_rows << "\n";

  txbench::TATPMySQLBenchmark benchmark(n_workers, warmup_duration,
                                        measurement_duration, n_rows);
  double tps = benchmark.run();
  std::cout << "tps: " << tps << std::endl;
}
