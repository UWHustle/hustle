git clone https://github.com/apache/arrow.git
cd arrow/cpp
mkdir release
cd release
cmake -DARROW_COMPUTE=ON -DARROW_CSV=ON ..
sudo make install -j4

