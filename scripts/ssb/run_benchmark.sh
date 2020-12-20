#!/bin/bash

cur_dir=`dirname $0`
src_dir=${cur_dir}/../..

# run the benchmark
cd ${src_dir}/build/src/benchmark/
./hustle_src_benchmark_main --test $1 --agg_op $2
