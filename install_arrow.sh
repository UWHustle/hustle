<<<<<<< HEAD
sudo apt update
sudo apt install -y -V apt-transport-https gnupg lsb-release wget
sudo wget -O /usr/share/keyrings/apache-arrow-keyring.gpg https://dl.bintray.com/apache/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-keyring.gpg
sudo tee /etc/apt/sources.list.d/apache-arrow.list <<APT_LINE
deb [arch=amd64 signed-by=/usr/share/keyrings/apache-arrow-keyring.gpg] https://dl.bintray.com/apache/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/ $(lsb_release --codename --short) main
deb-src [signed-by=/usr/share/keyrings/apache-arrow-keyring.gpg] https://dl.bintray.com/apache/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/ $(lsb_release --codename --short) main
APT_LINE
sudo apt update
sudo apt install -y -V libarrow-dev # For C++
sudo apt install -y -V libarrow-glib-dev # For GLib (C)
sudo apt install -y -V libarrow-flight-dev # For Flight C++
sudo apt install -y -V libplasma-dev # For Plasma C++
sudo apt install -y -V libplasma-glib-dev # For Plasma GLib (C)
sudo apt install -y -V libgandiva-dev # For Gandiva C++
sudo apt install -y -V libgandiva-glib-dev # For Gandiva GLib (C)
sudo apt install -y -V libparquet-dev # For Apache Parquet C++
sudo apt install -y -V libparquet-glib-dev # For Apache Parquet GLib (C)

=======
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
>>>>>>> 250dd5cb40dadb22138c04b1025fa5c26165027c
