sudo git clone https://github.com/UWQuickstep/SQL-benchmark-data-generator.git
cd SQL_benchmark-data-generator/ssbgen/
sudo make clean
sudo make -j10
sudo ./generate-all-tables.sh