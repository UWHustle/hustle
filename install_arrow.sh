if [ ! -d "arrow" ]
then
  git clone https://github.com/apache/arrow.git
  cd arrow
  git checkout d5dfa0ec083163f5d4b62dd35d9c305bdcb856b2
  cd cpp
  mkdir release
  cd release
  ../../../cmake-3.15.5/bin/cmake -DARROW_COMPUTE=ON -DARROW_CSV=ON ..
  sudo make install -j4
else
  cd arrow/cpp/release
  sudo make install -j4
fi
