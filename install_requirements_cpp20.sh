#!/usr/bin/env bash

# macOS Install script
if [[ $(uname) == "Darwin" ]]; then
  # Check if user is using brew
  which -s brew
  if [[ $? != 0 ]]; then
    echo Homebrew not found. Please first install homebrew through: https://brew.sh/
    exit 1
  fi
  brew update && brew bundle --file=Brewfile
fi

# Ubuntu Install script
if [[ $(uname) == "Linux" ]]; then
  # Make sure metal (docker) container can execute the following script.
  # - DEBIAN_FRONTEND=noninteractive: make sure software common install will not be interactive.
  apt-get update --yes && apt-get install sudo wget make git --yes
  sudo apt-get update
  DEBIAN_FRONTEND=noninteractive apt-get install tzdata --yes
  sudo apt-get install software-properties-common --yes
  sudo add-apt-repository ppa:ubuntu-toolchain-r/test --yes
  sudo apt-get update

  # Install gcc compiler.
  # For Ubuntu 16.04, we fall back to gcc-9. TODO: Shall we host our own gcc-10 compiler repo?
  # For Ubuntu 18.04 and 20.04, directly pull gcc-10.
  LINUX_VERSION=$(lsb_release -rs)
  if [[ ${LINUX_VERSION} == "18.04" || ${LINUX_VERSION} == "20.04" ]]; then
    echo "Detected compatible Ubuntu version: ${LINUX_VERSION}. Install gcc-10 as default compiler."
    sudo apt-get install gcc-10 g++-10 --yes
    sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 60 --slave /usr/bin/g++ g++ /usr/bin/g++-10
  elif [[ ${LINUX_VERSION} == "16.04" ]]; then
    echo "Detected compatible Ubuntu version: ${LINUX_VERSION}. Fallback to gcc-9."
    sudo apt-get install gcc-9 g++-9 --yes
    sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 60 --slave /usr/bin/g++ g++ /usr/bin/g++-9
  # else
  #   echo "Incompatible Linux version. Expect to build on an Ubuntu LTS version."
  #   exit 1
  fi

  # Install libconfig
  sudo apt-get update -y
  sudo apt-get install -y libconfig++-dev

  # Install curl that enables SSL (https) protocol.
  # Otherwise CMake fails in Ubuntu 18 and 20 because of the dependencies.
  # TODO: Put this in a tmp directory.
  sudo apt-get install libssl-dev autoconf libtool libcurl4 --yes
  # TODO: May want to lock to a release
  if [ ! -d "curl" ]; then
    git clone https://github.com/bagder/curl.git
  fi
  cd curl || exit
  autoreconf -fi
  ./configure --with-ssl
  make -j$(nproc)
  sudo make install
  cd ..
  rm -rf curl

  # Install CMake 3.15
  if [ ! -d "cmake-3.15.5" ]; then
    wget https://cmake.org/files/v3.15/cmake-3.15.5.tar.gz
    tar -xzvf cmake-3.15.5.tar.gz
    rm -f cmake-3.15.5.tar.gz
  fi
  cd cmake-3.15.5 || exit
  ./bootstrap
  make -j$(nproc)
  sudo make install
  cd ..

  # TODO: (Remove) Google benchmark is covered in CMake toolchain.
  #   The CMake dependency toolchain throws error if we just make
  #   google benchmark as a dependency.
  if [ ! -d "benchmark" ]; then
    git clone https://github.com/google/benchmark.git
    git clone https://github.com/google/googletest.git benchmark/googletest
    cd benchmark || exit 1
    cmake -E make_directory "build"
    cmake -E chdir "build" cmake -DCMAKE_BUILD_TYPE=Release ../
    sudo cmake --build "build" --config Release --target install
  else
    cd benchmark/build || exit
    sudo make install -j4
  fi
fi
