[![Hustle](https://circleci.com/gh/UWHustle/hustle.svg?style=shield)](https://circleci.com/gh/UWHustle/hustle)

# Hustle
Hustle is an efficient data platform, organized as a collection of data processing kernels, based on a micro services architecture, serving heterogeneous application query languages that map to core relational algebraic DSL.

## Building blocks
Hustle is built on [Apache Arrow](https://github.com/apache/arrow). 

## Coding Guidelines
We follow [these guidelines](https://arrow.apache.org/docs/developers/cpp/development.html) for development.

## Build Hustle

User on MacOS shall install [homebrew](https://brew.sh/) before running the following scripts.

To install the required packages for Hustle use the following scripts:

```
./install_requirements.sh
./install_arrow.sh
```
The scripts will install g++9, cmake 3.15 and Apache Arrow.

Then use cmake to build Hustle:
```
mkdir build
cd build
cmake -D CMAKE_BUILD_TYPE=RELEASE .. 
make -j all  
```

To run the test go into the build directory and use:
```
./run_test.sh 
```

## Run Benchmark

You can use the following commands to run the ssb benchmark, (before running the below commands make sure you have built the executable from the source files using the steps provided in the previous section).

To generate the ssb benchmark data, 

```
sh ./scripts/ssb/gen_benchmark_data.sh ${SCALE_FACTOR}
```
(Usually scale factor can be 1 or 10).


To run the ssb benchmark,

```
sh ./scripts/ssb/run_benchmark.sh  ssb_queries
```

To run the tatp benchmark,

```
sh ./scripts/tatp/run_benchmark.sh 
```
