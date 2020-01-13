#!/usr/bin/env bash

if [[ `uname` == "Darwin" ]]; then
		brew update && brew bundle --file=Brewfile
elif [[ `uname` == "Linux" ]]; then
		wget https://cmake.org/files/v3.15/cmake-3.15.5.tar.gz
		tar -xzvf cmake-3.15.5.tar.gz
		rm -f cmake-3.15.5.tar.gz
		cd cmake-3.15.5 && ./bootstrap && make -j 6 && sudo make install
		rm -rf cmake-3.15.5
		./install_arrow.sh
fi
