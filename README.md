[![CircleCI](https://circleci.com/github/UWQuickstep/hustle.svg?style=shield&circle-token=7ac82dc32c93004b877cc6ac9009e5a43f59e88b)](https://circleci.com/gh/UWQuickstep/hustle)


# Hustle
Hustle is an efficient data platform, organized as a collection of data processing kernels, based on a micro services architecture, serving heterogeneous application query languages that map to core relational algebraic DSL

## Building blocks
Hustle is built on [Apache Arrow](https://github.com/apache/arrow). 

## Coding Guidelinees
We follow [these guidelines](https://arrow.apache.org/docs/developers/cpp/development.html).


## Build Hustle

To install the required packages for Hustle use the following script:

```
./install_requirements.sh

```
This script will install g++9 and cmake 3.15.

Then use cmake to build Hustle:
```
mkdir build
cd build
cmake -D CMAKE_BUILD_TYPE=DEBUG .. 
```

To run the test go into the build directory and use:
```
ctest --output-on-failure
```
