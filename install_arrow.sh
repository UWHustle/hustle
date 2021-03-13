#!/usr/bin/env bash

# Checkout the arrow 3.0.0 Version
if [ ! -d "arrow" ]; then
  git clone https://github.com/apache/arrow.git
  cd arrow
  git checkout apache-arrow-3.0.0
  cd ..
fi

# Make the release
mkdir -p arrow/cpp/release
cd arrow/cpp/release
if [[ $(uname) == "Darwin" ]]; then
  # -DARROW_DEPENDENCY_SOURCE=AUTO: try to build dependencies from source if they are not found on the machine.
  cmake -DARROW_COMPUTE=ON -DARROW_DEPENDENCY_SOURCE=AUTO ..
elif [[ $(uname) == "Linux" ]]; then
  ../../../cmake-3.15.5/bin/cmake -DARROW_COMPUTE=ON -DARROW_DEPENDENCY_SOURCE=AUTO ..
fi
sudo make install -j4
