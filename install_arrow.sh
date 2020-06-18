if [ ! -d "arrow" ]
then
  git clone https://github.com/apache/arrow.git
  cd arrow
  git checkout 4d99ec48a0f36531124a368afe7818b56e25dfb3
  cd cpp
  mkdir release
  cd release
  if [[ `uname` == "Darwin" ]]; then
    cmake -DARROW_COMPUTE=ON -DARROW_CSV=ON ..
  elif [[ `uname` == "Linux" ]]; then
    ../../../cmake-3.15.5/bin/cmake -DARROW_COMPUTE=ON -DARROW_CSV=ON ..
  fi
  sudo make install -j4
else
  cd arrow/cpp/release
  sudo make install -j4
fi