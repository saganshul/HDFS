apt-get install default-jdk
apt-get install protobuf-compiler
apt-get install libprotobuf-java
apt-get install build-essential
apt-get install mysql-server
mysql -u root -p -e 'create database hdfs'
mysql -u root -p -e 'create table if not exists datablock(blocknum int,data longtext,primary key(blocknum))' hdfs
