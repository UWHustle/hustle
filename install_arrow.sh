if [ ! -d "arrow" ]
then
  git clone https://github.com/apache/arrow.git
  cd arrow
  git checkout apache-arrow-1.0.1
  cd cpp
  mkdir release
  cd release
  if [[ `uname` == "Darwin" ]]; then
    # -DARROW_DEPENDENCY_SOURCE=AUTO: try to build dependencies from source if they are not found on the machine.
    cmake -DARROW_COMPUTE=ON -DARROW_DEPENDENCY_SOURCE=AUTO ..
  elif [[ `uname` == "Linux" ]]; then
    ../../../cmake-3.15.5/bin/cmake -DARROW_COMPUTE=ON -DARROW_DEPENDENCY_SOURCE=AUTO ..
  fi
  sudo make install -j4
else
  cd arrow/cpp/release
  sudo make install -j4
fi