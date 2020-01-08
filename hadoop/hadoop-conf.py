#!/usr/bin/env python

import sys
import xml.etree.ElementTree as ETree
from optparse import OptionParser
from xml.dom import minidom
import os

HADOOP_HOME = os.getenv('HADOOP_HOME', '/usr/local/hadoop')
HADOOP_CONF_DIR = os.getenv('HADOOP_CONF_DIR', os.path.join(HADOOP_HOME, 'etc/hadoop'))

YARN_MINIMUM_ALLOCATION_MB = '4096'
YARN_MAXIMUM_ALLOCATION_MB = '32768'
YARN_MINIMUM_CORES = '1'
YARN_MAXIMUM_CORES = '64'
YARN_SHUFFLE_CLASS = 'org.apache.hadoop.mapred.ShuffleHandler'

MAPREDUCE_MAP_MEMORY = '4096'
MAPREDUCE_REDUCE_MEMORY = '4096'
MAPREDUCE_JAVA_OPTS = '-Xmx1536m'


def make_relative_path(path):
    abs_path = os.path.join(HADOOP_HOME, path)
    os.makedirs(abs_path)
    print("Created directory {}".format(path))
    return 'file://' + abs_path


def create_conf():
    return ETree.Element('configuration')


def add_property(conf, name, value):
    prop = ETree.SubElement(conf, 'property')
    prop_name = ETree.SubElement(prop, 'name')
    prop_name.text = name
    prop_value = ETree.SubElement(prop, 'value')
    prop_value.text = value


def write_conf(conf, conf_name):
    conf_data = ETree.tostring(conf, 'utf-8')
    conf_file = os.path.join(HADOOP_CONF_DIR, conf_name)
    core_site = open(conf_file, 'w')
    core_site.write(minidom.parseString(conf_data).toprettyxml(indent='  '))
    core_site.close()
    print("Wrote configuration file {}".format(conf_file))


def init_core_site(name_node='localhost', access_key_id=None, secret_access_key=None):
    conf = create_conf()

    add_property(conf, 'fs.defaultFS', 'hdfs://{}:9000'.format(name_node))

    if access_key_id is not None and secret_access_key is not None:
        add_property(conf, 'fs.s3.awsAccessKeyId', access_key_id)
        add_property(conf, 'fs.s3.awsSecretAccessKey', secret_access_key)

    write_conf(conf, 'core-site.xml')


def init_yarn_site(name_node='localhost', vcores='64', mem='204800'):
    conf = create_conf()

    add_property(conf, 'yarn.resourcemanager.hostname', name_node)
    add_property(conf, 'yarn.nodemanager.aux-services', 'mapreduce_shuffle')
    add_property(conf, 'yarn.nodemanager.aux-services.mapreduce.shuffle.class', YARN_SHUFFLE_CLASS)
    add_property(conf, 'yarn.nodemanager.local-dirs', make_relative_path('data/yarn/local'))
    add_property(conf, 'yarn.nodemanager.resource.cpu-vcores', vcores)
    add_property(conf, 'yarn.nodemanager.resource.memory-mb', mem)
    add_property(conf, 'yarn.nodemanager.vmem-check-enabled', 'false')
    add_property(conf, 'yarn.nodemanager.log-aggregation.roll-monitoring-interval-seconds', '7200')
    add_property(conf, 'yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage', '99')
    add_property(conf, 'yarn.acl.enable', '0')
    add_property(conf, 'yarn.scheduler.capacity.node-locality-delay', '0')
    add_property(conf, 'yarn.scheduler.minimum-allocation-mb', YARN_MINIMUM_ALLOCATION_MB)
    add_property(conf, 'yarn.scheduler.maximum-allocation-mb', YARN_MAXIMUM_ALLOCATION_MB)
    add_property(conf, 'yarn.scheduler.minimum-allocation-vcores', YARN_MINIMUM_CORES)
    add_property(conf, 'yarn.scheduler.maximum-allocation-vcores', YARN_MAXIMUM_CORES)
    add_property(conf, 'yarn.app.mapreduce.am.staging-dir', '/user')

    write_conf(conf, 'yarn-site.xml')


def init_mapred_site(name_node='localhost'):
    conf = create_conf()

    add_property(conf, 'mapreduce.jobtracker.address', name_node)
    add_property(conf, 'mapreduce.framework.name', 'yarn')
    add_property(conf, 'mapreduce.task.tmp.dir', make_relative_path('data/mr/tmp'))
    add_property(conf, 'mapreduce.cluster.local.dir', make_relative_path('data/mr/data'))
    add_property(conf, 'mapreduce.map.memory.mb', MAPREDUCE_MAP_MEMORY)
    add_property(conf, 'mapreduce.reduce.memory.mb', MAPREDUCE_REDUCE_MEMORY)
    add_property(conf, 'mapreduce.map.java.opts', MAPREDUCE_JAVA_OPTS)
    add_property(conf, 'mapreduce.reduce.java.opts', MAPREDUCE_JAVA_OPTS)
    add_property(conf, 'mapreduce.jobhistory.address', name_node + ':10020')
    add_property(conf, 'hadoop.proxyuser.mapred.groups', '*')
    add_property(conf, 'hadoop.proxyuser.mapred.hosts', '*')

    write_conf(conf, 'mapred-site.xml')


def init_hdfs_site(is_name_node, is_data_node, name_node='localhost'):
    conf = create_conf()

    add_property(conf, 'dfs.replication', '1')
    add_property(conf, 'dfs.permissions', 'false')
    add_property(conf, 'dfs.namenode.rpc-address', name_node + ':9000')
    add_property(conf, 'dfs.namenode.checkpoint.dir', make_relative_path('data/hdfs/namesecondary'))

    if is_name_node:
        add_property(conf, 'dfs.namenode.name.dir', make_relative_path('data/hdfs/namenode'))

    if is_data_node:
        add_property(conf, 'dfs.datanode.data.dir', make_relative_path('data/hdfs/datanode'))

    write_conf(conf, 'hdfs-site.xml')


def main():
    parser = OptionParser(
        prog="hadoop-conf",
        usage="%prog <name-node> <node-type> <aws-access-key-id> <aws-secret-access-key>\n\n")

    (opts, args) = parser.parse_args()
    if len(args) != 4:
        parser.print_help()
        sys.exit(1)
    (name_node, node_type, aws_access_key_id, aws_secret_access_key) = args

    is_name_node = 'namenode' in node_type
    is_data_node = 'datanode' in node_type

    init_core_site(name_node, aws_access_key_id, aws_secret_access_key)
    init_yarn_site(name_node)
    init_mapred_site(name_node)
    init_hdfs_site(is_name_node, is_data_node, name_node)


if __name__ == '__main__':
    main()
