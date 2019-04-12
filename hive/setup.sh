#!/bin/bash

HADOOP_HOME=/usr/local/hadoop
HIVE_HOME=/usr/local/hive

$HADOOP_HOME/bin/hadoop fs -mkdir -p /tmp
$HADOOP_HOME/bin/hadoop fs -mkdir -p /user/hive/warehouse
$HADOOP_HOME/bin/hadoop fs -chmod g+w /tmp
$HADOOP_HOME/bin/hadoop fs -chmod g+w /user/hive/warehouse

$HIVE_HOME/bin/schematool -dbType mysql -initSchema