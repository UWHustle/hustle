git clone https://github.com/apache/arrow.git
git checkout d5dfa0ec083163f5d4b62dd35d9c305bdcb856b2
cd arrow/cpp
mkdir release
cd release
cmake -DARROW_COMPUTE=ON -DARROW_CSV=ON ..
sudo make install -j4

