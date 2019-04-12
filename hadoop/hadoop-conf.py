#!/usr/bin/env python

import sys
import xml.etree.ElementTree as ETree
from optparse import OptionParser
from xml.dom import minidom
import os

HADOOP_HOME = os.getenv('HADOOP_HOME', '/usr/local/hadoop')
HADOOP_CONF_DIR = os.getenv('HADOOP_CONF_DIR', os.path.join(HADOOP_HOME, 'etc/hadoop'))


def init_core_site(name_node='localhost', access_key_id=None, secret_access_key=None):
    conf = ETree.Element('configuration')

    prop = ETree.SubElement(conf, 'property')
    name = ETree.SubElement(prop, 'name')
    name.text = 'fs.defaultFS'
    value = ETree.SubElement(prop, 'value')
    value.text = 'hdfs://%s:9000' % name_node

    if access_key_id is not None and secret_access_key is not None:
        prop2 = ETree.SubElement(conf, 'property')
        name2 = ETree.SubElement(prop2, 'name')
        name2.text = 'fs.s3.awsAccessKeyId'
        value2 = ETree.SubElement(prop2, 'value')
        value2.text = access_key_id

        prop3 = ETree.SubElement(conf, 'property')
        name3 = ETree.SubElement(prop3, 'name')
        name3.text = 'fs.s3.awsSecretAccessKey'
        value3 = ETree.SubElement(prop3, 'value')
        value3.text = secret_access_key

    conf_data = ETree.tostring(conf, 'utf-8')
    conf_file = os.path.join(HADOOP_CONF_DIR, 'core-site.xml')
    core_site = open(conf_file, 'w')
    core_site.write(minidom.parseString(conf_data).toprettyxml(indent='  '))
    core_site.close()
    print("Wrote configuration file {}".format(conf_file))


def init_yarn_site(name_node='localhost'):
    conf = ETree.Element('configuration')

    prop = ETree.SubElement(conf, 'property')
    name = ETree.SubElement(prop, 'name')
    name.text = 'yarn.nodemanager.aux-services'
    value = ETree.SubElement(prop, 'value')
    value.text = 'mapreduce_shuffle'

    prop2 = ETree.SubElement(conf, 'property')
    name2 = ETree.SubElement(prop2, 'name')
    name2.text = 'yarn.resourcemanager.hostname'
    value2 = ETree.SubElement(prop2, 'value')
    value2.text = name_node

    conf_data = ETree.tostring(conf, 'utf-8')
    conf_file = os.path.join(HADOOP_CONF_DIR, 'yarn-site.xml')
    yarn_site = open(conf_file, 'w')
    yarn_site.write(minidom.parseString(conf_data).toprettyxml(indent='  '))
    yarn_site.close()
    print("Wrote configuration file {}".format(conf_file))


def init_mapred_site(name_node='localhost'):
    conf = ETree.Element('configuration')

    prop = ETree.SubElement(conf, 'property')
    name = ETree.SubElement(prop, 'name')
    name.text = 'mapreduce.jobtracker.address'
    value = ETree.SubElement(prop, 'value')
    value.text = name_node

    prop2 = ETree.SubElement(conf, 'property')
    name2 = ETree.SubElement(prop2, 'name')
    name2.text = 'mapreduce.framework.name'
    value2 = ETree.SubElement(prop2, 'value')
    value2.text = 'yarn'

    prop3 = ETree.SubElement(conf, 'property')
    name3 = ETree.SubElement(prop3, 'name')
    name3.text = 'mapreduce.task.tmp.dir'
    value3 = ETree.SubElement(prop3, 'value')
    tmp_path = os.path.join(HADOOP_HOME, 'data/mr/tmp')
    value3.text = tmp_path
    os.makedirs(tmp_path)
    print("Created directory {}".format(tmp_path))

    prop4 = ETree.SubElement(conf, 'property')
    name4 = ETree.SubElement(prop4, 'name')
    name4.text = 'mapreduce.cluster.local.dir'
    value4 = ETree.SubElement(prop4, 'value')
    data_path = os.path.join(HADOOP_HOME, 'data/mr/data')
    value4.text = data_path
    os.makedirs(data_path)
    print("Created directory {}".format(data_path))

    conf_data = ETree.tostring(conf, 'utf-8')
    conf_file = os.path.join(HADOOP_CONF_DIR, 'mapred-site.xml')
    mapred_site = open(conf_file, 'w')
    mapred_site.write(minidom.parseString(conf_data).toprettyxml(indent='  '))
    mapred_site.close()
    print("Wrote configuration file {}".format(conf_file))


def init_hdfs_site(is_name_node, is_data_node):
    conf = ETree.Element('configuration')

    prop = ETree.SubElement(conf, 'property')
    name = ETree.SubElement(prop, 'name')
    name.text = 'dfs.replication'
    value = ETree.SubElement(prop, 'value')
    value.text = '1'

    if is_name_node:
        prop2 = ETree.SubElement(conf, 'property')
        name2 = ETree.SubElement(prop2, 'name')
        name2.text = 'dfs.namenode.name.dir'
        value2 = ETree.SubElement(prop2, 'value')
        name_node_path = os.path.join(HADOOP_HOME, 'data/hdfs/namenode')
        value2.text = 'file://' + name_node_path
        os.makedirs(name_node_path)
        print("Created directory {}".format(name_node_path))

    if is_data_node:
        prop3 = ETree.SubElement(conf, 'property')
        name3 = ETree.SubElement(prop3, 'name')
        name3.text = 'dfs.datanode.data.dir'
        value3 = ETree.SubElement(prop3, 'value')
        data_node_path = os.path.join(HADOOP_HOME, 'data/hdfs/datanode')
        value3.text = 'file://' + data_node_path
        os.makedirs(data_node_path)
        print("Created directory {}".format(data_node_path))

    conf_data = ETree.tostring(conf, 'utf-8')
    conf_file = os.path.join(HADOOP_CONF_DIR, 'hdfs-site.xml')
    hdfs_site = open(conf_file, 'w')
    hdfs_site.write(minidom.parseString(conf_data).toprettyxml(indent='  '))
    hdfs_site.close()
    print("Wrote configuration file {}".format(conf_file))


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
    init_hdfs_site(is_name_node, is_data_node)


if __name__ == '__main__':
    main()
