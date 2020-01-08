#!/bin/bash

HADOOP_HOME=/usr/local/hadoop
HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop

# Set hdfs url to make it easier
HDFS_URL="hdfs://${PUBLIC_DNS}:9000"
echo "export HDFS_URL=${HDFS_URL}" >> ~/.bash_profile

DATANODE_PATH="${HADOOP_HOME}/data/hdfs/datanode"
NAMENODE_PATH="${HADOOP_HOME}/data/hdfs/namenode"

pushd ${HADOOP_HOME} > /dev/null

${HOME}/hadoop-ec2/hadoop/hadoop-conf.py "${PUBLIC_DNS}" "namenode_datanode" ${AWS_ACCESS_KEY_ID} ${AWS_SECRET_ACCESS_KEY}
echo ${SLAVES} > ${HADOOP_HOME}/etc/hadoop/slaves

for node in ${SLAVES} ${OTHER_MASTERS}; do
  echo "Configuring slave node: ${node}"
  ssh -t -t ${SSH_OPTS} ubuntu@${node} "hadoop-ec2/hadoop/hadoop-conf.py" "${PUBLIC_DNS}" "datanode" ${AWS_ACCESS_KEY_ID} ${AWS_SECRET_ACCESS_KEY} & sleep 0.3
done
wait

if [[ -f "${NAMENODE_PATH}/current/VERSION" ]] && [[ -f "${NAMENODE_PATH}/current/fsimage" ]]; then
  echo "Hadoop namenode appears to be formatted: skipping"
else
  echo "Formatting HDFS namenode..."
  ${HADOOP_HOME}/bin/hdfs namenode -format
fi

# This is different depending on version.
echo "Starting HDFS..."
${HADOOP_HOME}/sbin/start-dfs.sh
echo "Starting YARN..."
${HADOOP_HOME}/sbin/start-yarn.sh
echo "Starting History Server..."
$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh --config ${HADOOP_CONF_DIR} start historyserver

popd > /dev/null