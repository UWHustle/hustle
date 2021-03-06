#!/usr/bin/env bash

# Checkout the arrow 3.0.0 Version
if [ ! -d "arrow" ]; then
  git clone https://github.com/apache/arrow.git
  cd arrow || exit 1
  git checkout apache-arrow-3.0.0
  cd ..
fi

# Make the release
mkdir -p arrow/cpp/release
cd arrow/cpp/release || exit 1

CMAKE_BIN=cmake
CMAKE_FLAGS="-DARROW_COMPUTE=ON -DARROW_DEPENDENCY_SOURCE=AUTO -DCMAKE_C_COMPILER=gcc-10 -DCMAKE_CXX_COMPILER=g++-10"

# TODO: (Remove) Since we have installed cmake globally in Linux,
#       we may not need to reference the previous cmake binary again.
if [[ $(uname) == "Darwin" ]]; then
  # -DARROW_DEPENDENCY_SOURCE=AUTO: try to build dependencies from source if they are not found on the machine.
  CMAKE_BIN=cmake
elif [[ $(uname) == "Linux" ]]; then
  CMAKE_BIN=../../../cmake-3.15.5/bin/cmake
fi

# Build arrow
${CMAKE_BIN} ${CMAKE_FLAGS} ..
sudo make install -j4
