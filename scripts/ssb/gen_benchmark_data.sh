#!/bin/bash

SCALE_FACTOR=1

cur_dir=`dirname $0`
src_dir=${cur_dir}/../..

mkdir -p ${src_dir}/ssb/data
cd ${src_dir}/ssb/data

# clone and build ssb-dbgen repo
if [ ! -d "ssb-dbgen" ]
then
    git clone https://github.com/eyalroz/ssb-dbgen.git
fi
cd ssb-dbgen
cmake . && cmake --build .

# generate ssb data for benchmark
./dbgen -v -s $SCALE_FACTOR
cp *.tbl ../.
