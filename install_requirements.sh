#!/usr/bin/env bash

if [[ `uname` == "Darwin" ]]; then
    # Check if user is using brew
    which -s brew
    if [[ $? != 0 ]] ; then
        echo Homebrew not found. Please first install homebrew through: https://brew.sh/
    exit 1
    fi
    brew update && brew bundle --file=Brewfile
elif [[ `uname` == "Linux" ]]; then
    if [ ! -d "cmake-3.15.5" ]
    then
      wget https://cmake.org/files/v3.15/cmake-3.15.5.tar.gz
      tar -xzvf cmake-3.15.5.tar.gz
      rm -f cmake-3.15.5.tar.gz
      cd cmake-3.15.5
      ./bootstrap
      make -j$(nproc)
    else
      cd cmake-3.15.5
    fi
    sudo make install
    cd ..
    sudo apt-get update
    sudo apt-get install software-properties-common --yes
    sudo add-apt-repository ppa:ubuntu-toolchain-r/test --yes
    sudo apt-get update
    sudo apt-get install gcc-9 g++-9 --yes
    sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 60 --slave /usr/bin/g++ g++ /usr/bin/g++-9
    
    sudo apt-get update -y
    sudo apt-get install -y libconfig++-dev

    if [ ! -d "benchmark" ]
    then
      git clone https://github.com/google/benchmark.git
      git clone https://github.com/google/googletest.git benchmark/googletest
      cd benchmark
      cmake -E make_directory "build"
      cmake -E chdir "build" cmake -DCMAKE_BUILD_TYPE=Release ../
      sudo cmake --build "build" --config Release --target install
    else
      cd benchmark/build
    fi
    sudo make install -j4
fi
