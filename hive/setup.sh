#!/bin/bash

HADOOP_HOME=/usr/local/hadoop

$HADOOP_HOME/bin/hadoop fs -mkdir -p /tmp
$HADOOP_HOME/bin/hadoop fs -mkdir -p /user/hive/warehouse
$HADOOP_HOME/bin/hadoop fs -chmod g+w /tmp
$HADOOP_HOME/bin/hadoop fs -chmod g+w /user/hive/warehouse