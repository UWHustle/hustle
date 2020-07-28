# This only works on Ubuntu
#

file="/etc/apt/sources.list.d/monetdb.list"
if [!-f "$file"]; then
  touch "$file"
fi

suite = $(lsb_release -cs)

sudo echo "deb https://dev.monetdb.org/downloads/deb/ ${suite} monetdb" > file
sudo echo "deb-src https://dev.monetdb.org/downloads/deb/ ${suite} monetdb" >> file

wget --output-document=- https://www.monetdb.org/downloads/MonetDB-GPG-KEY | sudo apt-key add
sudo apt update-
sudo apt install monetdb5-sql monetdb-client