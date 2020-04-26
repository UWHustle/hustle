<<<<<<< HEAD
[![Build Status](https://travis-ci.com/UWQuickstep/hustle.svg?token=d9R9n5FhmzDqBYneC2Kt&branch=master)](https://travis-ci.com/UWQuickstep/hustle)
=======
[![Hustle](https://circleci.com/gh/UWQuickstep/hustle.svg?style=shield)](https://circleci.com/gh/UWQuickstep/hustle)

>>>>>>> 250dd5cb40dadb22138c04b1025fa5c26165027c

# Hustle
Hustle is an efficient data platform, organized as a collection of data processing kernels, based on a micro services architecture, serving heterogeneous application query languages that map to core relational algebraic DSL

## Building blocks
Hustle is built on [Apache Arrow](https://github.com/apache/arrow). 

## Coding Guidelinees
We follow [these guidelines](https://arrow.apache.org/docs/developers/cpp/development.html).


## Build Hustle

<<<<<<< HEAD
To install the required packages for Hustle use the following script:

```
./install_requirements.sh

```
This script will install g++9 and cmake 3.15.
=======
To install the required packages for Hustle use the following scripts:

```
./install_requirements.sh
./install_arrow.sh
```
The scripts will install g++9, cmake 3.15 and Apache Arrow.
>>>>>>> 250dd5cb40dadb22138c04b1025fa5c26165027c

Then use cmake to build Hustle:
```
mkdir build
cd build
cmake -D CMAKE_BUILD_TYPE=DEBUG .. 
<<<<<<< HEAD
=======
make -j<number of cores>
>>>>>>> 250dd5cb40dadb22138c04b1025fa5c26165027c
```

To run the test go into the build directory and use:
```
ctest --output-on-failure
```
