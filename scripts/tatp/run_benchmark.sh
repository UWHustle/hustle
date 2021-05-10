#!/bin/bash

cur_dir=`dirname $0`
src_dir=${cur_dir}/../..

cd ${src_dir}/build/src/txbench/benchmarks/tatp_hustle
# clean the previous db file
rm -rf db_directory2
# run the benchmark
for i in 1 2 3
do
	echo "Trail - $i"
  	for j in 1
	do
		./tatp_hustle $j 5 15 100000
		rm -rf db_directory2
  	done
done

