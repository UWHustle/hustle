# txbench
A small benchmarking library for OLTP database systems

## Overview
The aim of txbench is to provide extensible benchmarking utilities for OLTP database systems. It is particularly useful for testing systems in the early stages of development that may not yet have a well-defined client interface such as a SQL interpreter. Instead, systems must simply implement C++ functions that represent the workload's procedures.

## Adding a new target system
The steps to add a new target system are the following.
1. Implement the benchmark loader by extending the `Loader` class in `loader.h`.
2. Implement the system connector by extending the desired benchmark's connector. For example, a new TATP connector would extend the `TATPConnector` class in `tatp_connector.h`.
3. Implement the benchmark configuration by extending the `Benchmark` class in `benchmark.h`.
4. Write the executable to run the benchmark on the new target system.

## Currently supported benchmarks and target systems
Currently, txbench only supports TATP on MySQL. More benchmarks and systems will be added in the future.

## System-specific instructions

### MySQL
To run bencharks on MySQL, the MySQL Connector/C++ version 8.0.19 needs to be installed. CMake looks in a specific place to find the headers and object file of this library. This will be made more flexible in the future. The following commands may be run to install Connector/C++.
```bash
wget https://dev.mysql.com/get/Downloads/Connector-C++/libmysqlcppconn8-2_8.0.19-1ubuntu18.04_amd64.deb
wget https://dev.mysql.com/get/Downloads/Connector-C++/libmysqlcppconn7_8.0.19-1ubuntu18.04_amd64.deb
wget https://dev.mysql.com/get/Downloads/Connector-C++/libmysqlcppconn-dev_8.0.19-1ubuntu18.04_amd64.deb
sudo dpkg -i libmysqlcppconn8-2_8.0.19-1ubuntu18.04_amd64.deb
sudo dpkg -i libmysqlcppconn7_8.0.19-1ubuntu18.04_amd64.deb
sudo dpkg -i libmysqlcppconn-dev_8.0.19-1ubuntu18.04_amd64.deb
```

MySQL needs to have a user named 'txbench' with the necessary permissions. The following commands may be run in the MySQL shell to create this user.
```SQL
CREATE USER 'txbench'@'localhost';
GRANT ALL ON * . * TO 'txbench'@'localhost';
```
