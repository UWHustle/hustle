#!/bin/bash

cur_dir=`dirname $0`
src_dir=${cur_dir}/../..

cd ${src_dir}/build/src/txbench/benchmarks/tatp_hustle
# clean the previous db file
rm -rf db_directory2
# run the benchmark
./tatp_hustle
