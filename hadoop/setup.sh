#!/bin/bash

HADOOP_HOME=/usr/local/hadoop

# Set hdfs url to make it easier
HDFS_URL="hdfs://${PUBLIC_DNS}:9000"
echo "export HDFS_URL=${HDFS_URL}" >> ~/.bash_profile

DATANODE_PATH="${HADOOP_HOME}/data/hdfs/datanode"
NAMENODE_PATH="${HADOOP_HOME}/data/hdfs/namenode"

pushd ${HADOOP_HOME} > /dev/null

${HOME}/hadoop-ec2/hadoop/hadoop-conf.py "${PUBLIC_DNS}" "namenode_datanode"
echo ${SLAVES} > ${HADOOP_HOME}/etc/hadoop/slaves

for node in ${SLAVES} ${OTHER_MASTERS}; do
  echo "Configuring slave node: ${node}"
  ssh -t -t ${SSH_OPTS} ubuntu@${node} "hadoop-ec2/hadoop/hadoop-conf.py" "${PUBLIC_DNS}" "datanode" & sleep 0.3
done
wait

if [[ -f "${NAMENODE_PATH}/current/VERSION" ]] && [[ -f "${NAMENODE_PATH}/current/fsimage" ]]; then
  echo "Hadoop namenode appears to be formatted: skipping"
else
  echo "Formatting HDFS namenode..."
  ${HADOOP_HOME}/bin/hadoop namenode -format
fi

# This is different depending on version.
echo "Starting HDFS..."
${HADOOP_HOME}/sbin/start-dfs.sh
echo "Starting YARN..."
${HADOOP_HOME}/sbin/start-yarn.sh

popd > /dev/null