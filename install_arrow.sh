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
CMAKE_FLAGS="-DARROW_COMPUTE=ON -DARROW_DEPENDENCY_SOURCE=AUTO"

if [[ $(uname) == "Darwin" ]]; then
  # -DARROW_DEPENDENCY_SOURCE=AUTO: try to build dependencies from source if they are not found on the machine.
  CMAKE_BIN=cmake
elif [[ $(uname) == "Linux" ]]; then
  CMAKE_BIN=../../../cmake-3.15.5/bin/cmake
fi

# Build arrow
${CMAKE_BIN} ${CMAKE_FLAGS} ..
sudo make install -j4
