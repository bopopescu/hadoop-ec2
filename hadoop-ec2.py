import itertools
import logging
import os
import pipes
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
from datetime import datetime
from optparse import OptionParser
from stat import S_IRUSR
from sys import stderr

import boto
from boto import ec2

if sys.version < "3":
    pass
else:
    raw_input = input
    xrange = range

DEFAULT_AMI_ID = 'ami-01e7d52933c219330'
HADOOP_HOME = os.getenv('HADOOP_HOME', '/usr/local/hadoop')
HADOOP_CONF_DIR = os.getenv('HADOOP_CONF_DIR', os.path.join(HADOOP_HOME, 'etc/hadoop'))
HADOOP_EC2_DIR = os.path.dirname(os.path.realpath(__file__))
AWS_REGION = 'us-west-2'
AWS_AZ = 'us-west-2c'
HADOOP_USER = 'ubuntu'

EC2_INSTANCE_TYPES = {
    "c1.medium": "pvm",
    "c1.xlarge": "pvm",
    "c3.large": "hvm",
    "c3.xlarge": "hvm",
    "c3.2xlarge": "hvm",
    "c3.4xlarge": "hvm",
    "c3.8xlarge": "hvm",
    "c4.large": "hvm",
    "c4.xlarge": "hvm",
    "c4.2xlarge": "hvm",
    "c4.4xlarge": "hvm",
    "c4.8xlarge": "hvm",
    "cc1.4xlarge": "hvm",
    "cc2.8xlarge": "hvm",
    "cg1.4xlarge": "hvm",
    "cr1.8xlarge": "hvm",
    "d2.xlarge": "hvm",
    "d2.2xlarge": "hvm",
    "d2.4xlarge": "hvm",
    "d2.8xlarge": "hvm",
    "g2.2xlarge": "hvm",
    "g2.8xlarge": "hvm",
    "hi1.4xlarge": "pvm",
    "hs1.8xlarge": "pvm",
    "i2.xlarge": "hvm",
    "i2.2xlarge": "hvm",
    "i2.4xlarge": "hvm",
    "i2.8xlarge": "hvm",
    "m1.small": "pvm",
    "m1.medium": "pvm",
    "m1.large": "pvm",
    "m1.xlarge": "pvm",
    "m2.xlarge": "pvm",
    "m2.2xlarge": "pvm",
    "m2.4xlarge": "pvm",
    "m3.medium": "hvm",
    "m3.large": "hvm",
    "m3.xlarge": "hvm",
    "m3.2xlarge": "hvm",
    "m4.large": "hvm",
    "m4.xlarge": "hvm",
    "m4.2xlarge": "hvm",
    "m4.4xlarge": "hvm",
    "m4.10xlarge": "hvm",
    "r3.large": "hvm",
    "r3.xlarge": "hvm",
    "r3.2xlarge": "hvm",
    "r3.4xlarge": "hvm",
    "r3.8xlarge": "hvm",
    "t1.micro": "pvm",
    "t2.micro": "hvm",
    "t2.small": "hvm",
    "t2.medium": "hvm",
    "t2.large": "hvm",
}


class UsageError(Exception):
    pass


def stringify_command(parts):
    if isinstance(parts, str):
        return parts
    else:
        return ' '.join(map(pipes.quote, parts))


def ssh_args(opts):
    parts = ['-o', 'StrictHostKeyChecking=no']
    parts += ['-o', 'UserKnownHostsFile=/dev/null']
    if opts.identity_file is not None:
        parts += ['-i', opts.identity_file]
    return parts


def ssh_command(opts):
    return ['ssh'] + ssh_args(opts)


def scp_command(opts):
    return ['scp'] + ssh_args(opts)


# Run a command on a host through ssh, retrying up to five times
# and then throwing an exception if ssh continues to fail.
def ssh(host, opts, command):
    tries = 0
    while True:
        try:
            return subprocess.check_call(
                ssh_command(opts) + ['-t', '-t', '%s@%s' % (HADOOP_USER, host),
                                     stringify_command(command)])
        except subprocess.CalledProcessError as e:
            if tries > 5:
                # If this was an ssh failure, provide the user with hints.
                if e.returncode == 255:
                    raise UsageError(
                        "Failed to SSH to remote host {0}.\n"
                        "Please check that you have provided the correct --identity-file and "
                        "--key-pair parameters and try again.".format(host))
                else:
                    raise e
            print("Error executing remote command, retrying after 30 seconds: {0}".format(e),
                  file=stderr)
            time.sleep(30)
            tries = tries + 1


def scp(host, opts, src):
    tries = 0
    while True:
        try:
            return subprocess.check_call(
                scp_command(opts) + ['-r', src, '%s@%s:~/' % (HADOOP_USER, host)])
        except subprocess.CalledProcessError as e:
            if tries > 5:
                # If this was an ssh failure, provide the user with hints.
                if e.returncode == 255:
                    raise UsageError(
                        "Failed to SCP to remote host {0}.\n"
                        "Please check that you have provided the correct --identity-file and "
                        "--key-pair parameters and try again.".format(host))
                else:
                    raise e
            print("Error executing remote command, retrying after 30 seconds: {0}".format(e),
                  file=stderr)
            time.sleep(30)
            tries = tries + 1


# Backported from Python 2.7 for compatibility with 2.6 (See SPARK-1990)
def _check_output(*popenargs, **kwargs):
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        raise subprocess.CalledProcessError(retcode, cmd, output=output)
    return output


def ssh_read(host, opts, command):
    return _check_output(
        ssh_command(opts) + ['%s@%s' % (HADOOP_USER, host), stringify_command(command)])


def ssh_write(host, opts, command, arguments):
    tries = 0
    while True:
        proc = subprocess.Popen(
            ssh_command(opts) + ['%s@%s' % (HADOOP_USER, host), stringify_command(command)],
            stdin=subprocess.PIPE)
        proc.stdin.write(arguments)
        proc.stdin.close()
        status = proc.wait()
        if status == 0:
            break
        elif tries > 5:
            raise RuntimeError("ssh_write failed with error %s" % proc.returncode)
        else:
            print("Error {0} while executing remote command, retrying after 30 seconds".
                  format(status), file=stderr)
            time.sleep(30)
            tries = tries + 1


def is_ssh_available(host, opts, print_ssh_output=True):
    """
    Check if SSH is available on a host.
    """
    s = subprocess.Popen(
        ssh_command(opts) + ['-t', '-t', '-o', 'ConnectTimeout=3',
                             '%s@%s' % (HADOOP_USER, host), stringify_command('true')],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT  # we pipe stderr through stdout to preserve output order
    )
    cmd_output = s.communicate()[0]  # [1] is stderr, which we redirected to stdout

    if s.returncode != 0 and print_ssh_output:
        # extra leading newline is for spacing in wait_for_cluster_state()
        print(textwrap.dedent("""\n
            Warning: SSH connection error. (This could be temporary.)
            Host: {h}
            SSH return code: {r}
            SSH output: {o}
        """).format(
            h=host,
            r=s.returncode,
            o=cmd_output.strip()
        ))

    return s.returncode == 0


def is_cluster_ssh_available(cluster_instances, opts):
    """
    Check if SSH is available on all the instances in a cluster.
    """
    for i in cluster_instances:
        dns_name = get_dns_name(i)
        if not is_ssh_available(host=dns_name, opts=opts):
            return False
    else:
        return True


# Get the EC2 security group of the given name, creating it if it doesn't exist
def get_or_make_group(conn, name):
    groups = conn.get_all_security_groups()
    group = [g for g in groups if g.name == name]
    if len(group) > 0:
        return group[0]
    else:
        print("Creating security group " + name)
        return conn.create_security_group(name, "Hadoop group", None)


# Gets the IP address
def get_ip_address(instance):
    return instance.ip_address


# Gets the DNS name
def get_dns_name(instance):
    dns = instance.public_dns_name
    if not dns:
        raise UsageError("Failed to determine hostname of {0}.".format(instance))
    return dns


def get_num_disks(instance_type):
    # Source: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/InstanceStorage.html
    # Last Updated: 2015-06-19
    # For easy maintainability, please keep this manually-inputted dictionary sorted by key.
    disks_by_instance = {
        "c1.medium": 1,
        "c1.xlarge": 4,
        "c3.large": 2,
        "c3.xlarge": 2,
        "c3.2xlarge": 2,
        "c3.4xlarge": 2,
        "c3.8xlarge": 2,
        "c4.large": 0,
        "c4.xlarge": 0,
        "c4.2xlarge": 0,
        "c4.4xlarge": 0,
        "c4.8xlarge": 0,
        "cc1.4xlarge": 2,
        "cc2.8xlarge": 4,
        "cg1.4xlarge": 2,
        "cr1.8xlarge": 2,
        "d2.xlarge": 3,
        "d2.2xlarge": 6,
        "d2.4xlarge": 12,
        "d2.8xlarge": 24,
        "g2.2xlarge": 1,
        "g2.8xlarge": 2,
        "hi1.4xlarge": 2,
        "hs1.8xlarge": 24,
        "i2.xlarge": 1,
        "i2.2xlarge": 2,
        "i2.4xlarge": 4,
        "i2.8xlarge": 8,
        "m1.small": 1,
        "m1.medium": 1,
        "m1.large": 2,
        "m1.xlarge": 4,
        "m2.xlarge": 1,
        "m2.2xlarge": 1,
        "m2.4xlarge": 2,
        "m3.medium": 1,
        "m3.large": 1,
        "m3.xlarge": 2,
        "m3.2xlarge": 2,
        "m4.large": 0,
        "m4.xlarge": 0,
        "m4.2xlarge": 0,
        "m4.4xlarge": 0,
        "m4.10xlarge": 0,
        "r3.large": 1,
        "r3.xlarge": 1,
        "r3.2xlarge": 1,
        "r3.4xlarge": 1,
        "r3.8xlarge": 2,
        "t1.micro": 0,
        "t2.micro": 0,
        "t2.small": 0,
        "t2.medium": 0,
        "t2.large": 0,
    }
    if instance_type in disks_by_instance:
        return disks_by_instance[instance_type]
    else:
        print("WARNING: Don't know number of disks on instance type %s; assuming 1"
              % instance_type, file=stderr)
        return 1


def get_existing_cluster(conn, cluster_name, die_on_error=True):
    """
    Get the EC2 instances in an existing cluster if available.
    Returns a tuple of lists of EC2 instance objects for the masters and slaves.
    """
    print("Searching for existing cluster {c} in region {r}...".format(
        c=cluster_name, r=AWS_REGION))

    def get_instances(group_names):
        """
        Get all non-terminated instances that belong to any of the provided security groups.
        EC2 reservation filters and instance states are documented here:
            http://docs.aws.amazon.com/cli/latest/reference/ec2/describe-instances.html#options
        """
        reservations = conn.get_all_reservations(
            filters={"instance.group-name": group_names})
        instances = itertools.chain.from_iterable(r.instances for r in reservations)
        return [i for i in instances if i.state not in ["shutting-down", "terminated"]]

    master_instances = get_instances([cluster_name + "-master"])
    slave_instances = get_instances([cluster_name + "-slaves"])

    if any((master_instances, slave_instances)):
        print("Found {m} master{plural_m}, {s} slave{plural_s}.".format(
            m=len(master_instances),
            plural_m=('' if len(master_instances) == 1 else 's'),
            s=len(slave_instances),
            plural_s=('' if len(slave_instances) == 1 else 's')))

    if not master_instances and die_on_error:
        print("ERROR: Could not find a master for cluster {c} in region {r}.".format(
            c=cluster_name, r=AWS_REGION), file=sys.stderr)
        sys.exit(1)

    return master_instances, slave_instances


def wait_for_cluster_state(conn, opts, cluster_instances, cluster_state):
    """
    Wait for all the instances in the cluster to reach a designated state.
    cluster_instances: a list of boto.ec2.instance.Instance
    cluster_state: a string representing the desired state of all the instances in the cluster
           value can be 'ssh-ready' or a valid value from boto.ec2.instance.InstanceState such as
           'running', 'terminated', etc.
           (would be nice to replace this with a proper enum: http://stackoverflow.com/a/1695250)
    """
    sys.stdout.write("Waiting for cluster to enter '{s}' state.".format(s=cluster_state))
    sys.stdout.flush()

    start_time = datetime.now()
    num_attempts = 0

    while True:
        time.sleep(5 * num_attempts)  # seconds

        for i in cluster_instances:
            i.update()

        max_batch = 100
        statuses = []
        for j in range(0, len(cluster_instances), max_batch):
            batch = [i.id for i in cluster_instances[j:j + max_batch]]
            statuses.extend(conn.get_all_instance_status(instance_ids=batch))

        if cluster_state == 'ssh-ready':
            if all(i.state == 'running' for i in cluster_instances) and \
                    all(s.system_status.status == 'ok' for s in statuses) and \
                    all(s.instance_status.status == 'ok' for s in statuses) and \
                    is_cluster_ssh_available(cluster_instances, opts):
                break
        else:
            if all(i.state == cluster_state for i in cluster_instances):
                break

        num_attempts += 1

        sys.stdout.write(".")
        sys.stdout.flush()

    sys.stdout.write("\n")

    end_time = datetime.now()
    print("Cluster is now in '{s}' state. Waited {t} seconds.".format(
        s=cluster_state,
        t=(end_time - start_time).seconds
    ))


# Launch a cluster of the given name, by setting up its security groups,
# and then starting new instances in them.
# Returns a tuple of EC2 reservation objects for the master and slaves
# Fails if there already instances running in the cluster's groups.
def launch_cluster(conn, opts, cluster_name):
    if opts.identity_file is None:
        print("ERROR: Must provide an identity file (-i) for ssh connections.", file=stderr)
        sys.exit(1)

    if opts.key_pair is None:
        print("ERROR: Must provide a key pair name (-k) to use on instances.", file=stderr)
        sys.exit(1)

    print("Setting up security groups...")
    master_group = get_or_make_group(conn, cluster_name + "-master")
    slave_group = get_or_make_group(conn, cluster_name + "-slaves")
    if not master_group.rules:  # Group was just now created
        master_group.authorize(src_group=master_group)
        master_group.authorize(src_group=slave_group)
        master_group.authorize('tcp', 22, 22, "0.0.0.0/0")
        master_group.authorize('tcp', 8080, 8081, "0.0.0.0/0")
        master_group.authorize('tcp', 18080, 18080, "0.0.0.0/0")
        master_group.authorize('tcp', 19999, 19999, "0.0.0.0/0")
        master_group.authorize('tcp', 50030, 50030, "0.0.0.0/0")
        master_group.authorize('tcp', 50070, 50070, "0.0.0.0/0")
        master_group.authorize('tcp', 60070, 60070, "0.0.0.0/0")
        master_group.authorize('tcp', 4040, 4045, "0.0.0.0/0")
        # Rstudio (GUI for R) needs port 8787 for web access
        master_group.authorize('tcp', 8787, 8787, "0.0.0.0/0")
        # HDFS NFS gateway requires 111,2049,4242 for tcp & udp
        master_group.authorize('tcp', 111, 111, "0.0.0.0/0")
        master_group.authorize('udp', 111, 111, "0.0.0.0/0")
        master_group.authorize('tcp', 2049, 2049, "0.0.0.0/0")
        master_group.authorize('udp', 2049, 2049, "0.0.0.0/0")
        master_group.authorize('tcp', 4242, 4242, "0.0.0.0/0")
        master_group.authorize('udp', 4242, 4242, "0.0.0.0/0")
        # RM in YARN mode uses 8088
        master_group.authorize('tcp', 8088, 8088, "0.0.0.0/0")
    if not slave_group.rules:  # Group was just now created
        slave_group.authorize(src_group=master_group)
        slave_group.authorize(src_group=slave_group)
        slave_group.authorize('tcp', 22, 22, "0.0.0.0/0")
        slave_group.authorize('tcp', 8080, 8081, "0.0.0.0/0")
        slave_group.authorize('tcp', 50060, 50060, "0.0.0.0/0")
        slave_group.authorize('tcp', 50075, 50075, "0.0.0.0/0")
        slave_group.authorize('tcp', 60060, 60060, "0.0.0.0/0")
        slave_group.authorize('tcp', 60075, 60075, "0.0.0.0/0")

    # Check if instances are already running in our groups
    existing_masters, existing_slaves = get_existing_cluster(conn, cluster_name, die_on_error=False)
    if existing_slaves or existing_masters:
        print("ERROR: There are already instances running in group %s or %s" %
              (master_group.name, slave_group.name), file=stderr)
        sys.exit(1)

    # Figure out AMI
    if opts.ami is None:
        opts.ami = DEFAULT_AMI_ID

    # we use group ids to work around https://github.com/boto/boto/issues/350
    print("Launching instances...")

    try:
        image = conn.get_all_images(image_ids=[opts.ami])[0]
    except:
        print("Could not find AMI " + opts.ami, file=stderr)
        sys.exit(1)

    # Launch slaves
    if opts.spot_price is not None:
        # Launch spot instances with the requested price
        print("Requesting %d slaves as spot instances with price $%.3f" % (opts.slaves, opts.spot_price))
        my_req_ids = []
        slave_reqs = conn.request_spot_instances(
            price=opts.spot_price,
            image_id=opts.ami,
            launch_group="launch-group-%s" % cluster_name,
            placement=AWS_AZ,
            count=opts.slaves,
            key_name=opts.key_pair,
            security_group_ids=[slave_group.id],
            instance_type=opts.instance_type)
        my_req_ids += [req.id for req in slave_reqs]

        print("Waiting for spot instances to be granted...")
        try:
            while True:
                time.sleep(10)
                reqs = conn.get_all_spot_instance_requests()
                id_to_req = {}
                for r in reqs:
                    id_to_req[r.id] = r
                active_instance_ids = []
                for i in my_req_ids:
                    if i in id_to_req and id_to_req[i].state == "active":
                        active_instance_ids.append(id_to_req[i].instance_id)
                if len(active_instance_ids) == opts.slaves:
                    print("All %d slaves granted" % opts.slaves)
                    reservations = conn.get_all_reservations(active_instance_ids)
                    slave_nodes = []
                    for r in reservations:
                        slave_nodes += r.instances
                    break
                else:
                    print("%d of %d slaves granted, waiting longer" % (len(active_instance_ids), opts.slaves))
        except:
            print("Canceling spot instance requests")
            conn.cancel_spot_instance_requests(my_req_ids)
            # Log a warning if any of these requests actually launched instances:
            (master_nodes, slave_nodes) = get_existing_cluster(conn, cluster_name, die_on_error=False)
            running = len(master_nodes) + len(slave_nodes)
            if running:
                print(("WARNING: %d instances are still running" % running), file=stderr)
            sys.exit(0)
    else:
        # Launch non-spot instances
        slave_nodes = []
        slave_res = image.run(
            key_name=opts.key_pair,
            security_group_ids=[slave_group.id],
            instance_type=opts.instance_type,
            placement=AWS_AZ,
            min_count=opts.slaves,
            max_count=opts.slaves)
        slave_nodes += slave_res.instances
        print("Launched {s} slave{plural_s} in {z}, regid = {r}".format(
            s=opts.slaves,
            plural_s=('' if opts.slaves == 1 else 's'),
            z=AWS_AZ,
            r=slave_res.id))

    # Launch or resume masters
    if existing_masters:
        print("Starting master...")
        for inst in existing_masters:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        master_nodes = existing_masters
    else:
        master_type = opts.master_instance_type
        if master_type == "":
            master_type = opts.instance_type
        master_res = image.run(
            key_name=opts.key_pair,
            security_group_ids=[master_group.id],
            instance_type=master_type,
            placement=AWS_AZ,
            min_count=1,
            max_count=1)

        master_nodes = master_res.instances
        print("Launched master in %s, regid = %s" % (AWS_AZ, master_res.id))

    # This wait time corresponds to SPARK-4983
    print("Waiting for AWS to propagate instance metadata...")
    time.sleep(15)

    for master in master_nodes:
        master.add_tags(
            dict(Name='{cn}-master-{iid}'.format(cn=cluster_name, iid=master.id))
        )

    for slave in slave_nodes:
        slave.add_tags(
            dict(Name='{cn}-slave-{iid}'.format(cn=cluster_name, iid=slave.id))
        )

    # Return all the instances
    return master_nodes, slave_nodes


# Deploy the configuration file templates in a given local directory to
# a cluster, filling in any template parameters with information about the
# cluster (e.g. lists of masters and slaves). Files are only deployed to
# the first master instance in the cluster, and we expect the setup
# script to be run on that instance to copy them to other nodes.
#
# root_dir should be an absolute path to the directory with the files we want to deploy.
def deploy_files(conn, root_dir, opts, master_nodes, slave_nodes, modules):
    active_master = get_dns_name(master_nodes[0])

    master_addresses = [get_dns_name(i) for i in master_nodes]
    slave_addresses = [get_dns_name(i) for i in slave_nodes]
    template_vars = {"master_list": '\n'.join(master_addresses),
                     "active_master": active_master,
                     "slave_list": '\n'.join(slave_addresses),
                     "modules": '\n'.join(modules),
                     "aws_access_key_id": conn.aws_access_key_id,
                     "aws_secret_access_key": conn.aws_secret_access_key}

    # Create a temp directory in which we will place all the files to be
    # deployed after we substitute template parameters in them
    tmp_dir = tempfile.mkdtemp()
    for path, dirs, files in os.walk(root_dir):
        if path.find(".svn") == -1:
            dest_dir = os.path.join('/', path[len(root_dir):])
            local_dir = tmp_dir + dest_dir
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
            for filename in files:
                if filename[0] not in '#.~' and filename[-1] != '~':
                    dest_file = os.path.join(dest_dir, filename)
                    local_file = tmp_dir + dest_file
                    with open(os.path.join(path, filename)) as src:
                        with open(local_file, "w") as dest:
                            text = src.read()
                            for key in template_vars:
                                text = text.replace("{{" + key + "}}", template_vars[key])
                            dest.write(text)
                            dest.close()
    # rsync the whole directory over to the master machine
    command = [
        'rsync', '-rv',
        '-e', stringify_command(ssh_command(opts)),
        "%s/" % tmp_dir,
        "%s@%s:/" % (HADOOP_USER, active_master)
    ]
    subprocess.check_call(command)
    # Remove the temp directory we created above
    shutil.rmtree(tmp_dir)


def setup_hadoop_cluster(master, opts):
    ssh(master, opts, "chmod u+x hadoop-ec2/setup.sh")
    ssh(master, opts, "hadoop-ec2/setup.sh")
    print("Hadoop standalone cluster started at http://%s:9000" % master)


# Deploy configuration files and run setup scripts on a newly launched
# or started EC2 cluster.
def setup_cluster(conn, master_nodes, slave_nodes, opts, deploy_ssh_key):
    master = get_dns_name(master_nodes[0])
    if deploy_ssh_key:
        print("Generating cluster's SSH key on master...")
        key_setup = """
          [ -f ~/.ssh/id_rsa ] ||
            (ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa &&
             cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys)
        """
        ssh(master, opts, key_setup)
        dot_ssh_tar = ssh_read(master, opts, ['tar', 'c', '.ssh'])
        print("Transferring cluster's SSH key to slaves...")
        for slave in slave_nodes:
            slave_address = get_dns_name(slave)
            print(slave_address)
            ssh_write(slave_address, opts, ['tar', 'x'], dot_ssh_tar)

    modules = ['hadoop', 'hive']

    # NOTE: We should clone the repository before running deploy_files to
    # prevent ec2-variables.sh from being overwritten
    print("Copying hadoop-ec2 scripts from {p} on master...".format(p=HADOOP_EC2_DIR))
    scp(host=master,
        opts=opts,
        src=HADOOP_EC2_DIR)

    print("Deploying files to master...")
    deploy_files(
        conn=conn,
        root_dir=HADOOP_EC2_DIR + "/" + "deploy.generic",
        opts=opts,
        master_nodes=master_nodes,
        slave_nodes=slave_nodes,
        modules=modules
    )

    print("Running setup on master...")
    setup_hadoop_cluster(master, opts)
    print("Done!")


def parse_args():
    parser = OptionParser(
        prog="hadoop-ec2",
        version="%prog",
        usage="%prog [options] <action> <cluster_name>\n\n"
              + "<action> can be: launch, destroy, login, stop, start, get-master, reboot-slaves")

    parser.add_option(
        "-s", "--slaves", type="int", default=1,
        help="Number of slaves to launch (default: %default)")
    parser.add_option(
        "-k", "--key-pair",
        help="Key pair to use on instances")
    parser.add_option(
        "-i", "--identity-file",
        help="SSH private key file to use for logging into instances")
    parser.add_option(
        "-t", "--instance-type", default="m4.10xlarge",
        help="Type of instance to launch (default: %default). " +
             "WARNING: must be 64-bit; small instances won't work")
    parser.add_option(
        "-m", "--master-instance-type", default="m4.10xlarge",
        help="Master instance type (leave empty for same as instance-type)")
    parser.add_option(
        "--spot-price", metavar="PRICE", type="float",
        help="If specified, launch slaves as spot instances with the given " +
             "maximum price (in dollars)")
    parser.add_option(
        "-a", "--ami",
        help="Amazon Machine Image ID to use")

    (opts, args) = parser.parse_args()
    if len(args) != 2:
        parser.print_help()
        sys.exit(1)
    (action, cluster_name) = args

    # Boto config check
    # http://boto.cloudhackers.com/en/latest/boto_config_tut.html
    home_dir = os.getenv('HOME')
    if home_dir is None or not os.path.isfile(home_dir + '/.boto'):
        if not os.path.isfile('/etc/boto.cfg'):
            # If there is no boto config, check aws credentials
            if not os.path.isfile(home_dir + '/.aws/credentials'):
                if os.getenv('AWS_ACCESS_KEY_ID') is None:
                    print("ERROR: The environment variable AWS_ACCESS_KEY_ID must be set",
                          file=sys.stderr)
                    sys.exit(1)
                if os.getenv('AWS_SECRET_ACCESS_KEY') is None:
                    print("ERROR: The environment variable AWS_SECRET_ACCESS_KEY must be set",
                          file=sys.stderr)
                    sys.exit(1)
    return opts, action, cluster_name


def real_main():
    (opts, action, cluster_name) = parse_args()

    if opts.identity_file is not None:
        if not os.path.exists(opts.identity_file):
            print("ERROR: The identity file '{f}' doesn't exist.".format(f=opts.identity_file),
                  file=stderr)
            sys.exit(1)

        file_mode = os.stat(opts.identity_file).st_mode
        if not (file_mode & S_IRUSR) or not oct(file_mode)[-2:] == '00':
            print("ERROR: The identity file must be accessible only by you.", file=stderr)
            print('You can fix this with: chmod 400 "{f}"'.format(f=opts.identity_file),
                  file=stderr)
            sys.exit(1)

    if opts.instance_type not in EC2_INSTANCE_TYPES:
        print("Warning: Unrecognized EC2 instance type for instance-type: {t}".format(
            t=opts.instance_type), file=stderr)

    if opts.master_instance_type != "":
        if opts.master_instance_type not in EC2_INSTANCE_TYPES:
            print("Warning: Unrecognized EC2 instance type for master-instance-type: {t}".format(
                t=opts.master_instance_type), file=stderr)
        # Since we try instance types even if we can't resolve them, we check if they resolve first
        # and, if they do, see if they resolve to the same virtualization type.
        if opts.instance_type in EC2_INSTANCE_TYPES and \
                opts.master_instance_type in EC2_INSTANCE_TYPES:
            if EC2_INSTANCE_TYPES[opts.instance_type] != \
                    EC2_INSTANCE_TYPES[opts.master_instance_type]:
                print("Error: spark-ec2 currently does not support having a master and slaves "
                      "with different AMI virtualization types.", file=stderr)
                print("master instance virtualization type: {t}".format(
                    t=EC2_INSTANCE_TYPES[opts.master_instance_type]), file=stderr)
                print("slave instance virtualization type: {t}".format(
                    t=EC2_INSTANCE_TYPES[opts.instance_type]), file=stderr)
                sys.exit(1)

    try:
        conn = ec2.connect_to_region(AWS_REGION)
    except Exception as e:
        print(e, file=stderr)
        sys.exit(1)

    if action == "launch":
        if opts.slaves <= 0:
            print("ERROR: You have to start at least 1 slave", file=sys.stderr)
            sys.exit(1)
        (master_nodes, slave_nodes) = launch_cluster(conn, opts, cluster_name)
        wait_for_cluster_state(
            conn=conn,
            opts=opts,
            cluster_instances=(master_nodes + slave_nodes),
            cluster_state='ssh-ready'
        )
        setup_cluster(conn, master_nodes, slave_nodes, opts, True)

    elif action == "destroy":
        (master_nodes, slave_nodes) = get_existing_cluster(
            conn, cluster_name, die_on_error=False)

        if any(master_nodes + slave_nodes):
            print("The following instances will be terminated:")
            for inst in master_nodes + slave_nodes:
                print("> %s" % get_dns_name(inst))
            print("ALL DATA ON ALL NODES WILL BE LOST!!")

        msg = "Are you sure you want to destroy the cluster {c}? (y/N) ".format(c=cluster_name)
        response = raw_input(msg)
        if response == "y":
            print("Terminating master...")
            for inst in master_nodes:
                inst.terminate()
            print("Terminating slaves...")
            for inst in slave_nodes:
                inst.terminate()

            # Delete security groups as well
            if opts.delete_groups:
                group_names = [cluster_name + "-master", cluster_name + "-slaves"]
                wait_for_cluster_state(
                    conn=conn,
                    opts=opts,
                    cluster_instances=(master_nodes + slave_nodes),
                    cluster_state='terminated'
                )
                print("Deleting security groups (this will take some time)...")
                attempt = 1
                success = False
                while attempt <= 3:
                    print("Attempt %d" % attempt)
                    groups = [g for g in conn.get_all_security_groups() if g.name in group_names]
                    success = True
                    # Delete individual rules in all groups before deleting groups to
                    # remove dependencies between them
                    for group in groups:
                        print("Deleting rules in security group " + group.name)
                        for rule in group.rules:
                            for grant in rule.grants:
                                success &= group.revoke(ip_protocol=rule.ip_protocol,
                                                        from_port=rule.from_port,
                                                        to_port=rule.to_port,
                                                        src_group=grant)

                    # Sleep for AWS eventual-consistency to catch up, and for instances
                    # to terminate
                    time.sleep(30)  # Yes, it does have to be this long :-(
                    for group in groups:
                        try:
                            # It is needed to use group_id to make it work with VPC
                            conn.delete_security_group(group_id=group.id)
                            print("Deleted security group %s" % group.name)
                        except boto.exception.EC2ResponseError:
                            success = False
                            print("Failed to delete security group %s" % group.name)

                    # Unfortunately, group.revoke() returns True even if a rule was not
                    # deleted, so this needs to be rerun if something fails
                    if success:
                        break

                    attempt += 1

                if not success:
                    print("Failed to delete all security groups after 3 tries.")
                    print("Try re-running in a few minutes.")

    elif action == "login":
        (master_nodes, slave_nodes) = get_existing_cluster(conn, cluster_name)
        if not master_nodes[0].public_dns_name:
            print("Master has no public DNS name.  Maybe you meant to specify --private-ips?")
        else:
            master = get_dns_name(master_nodes[0])
            print("Logging into master " + master + "...")
            subprocess.check_call(
                ssh_command(opts) + ['-t', '-t', "%s@%s" % (HADOOP_USER, master)])

    elif action == "reboot-slaves":
        response = raw_input(
            "Are you sure you want to reboot the cluster " +
            cluster_name + " slaves?\n" +
            "Reboot cluster slaves " + cluster_name + " (y/N): ")
        if response == "y":
            (master_nodes, slave_nodes) = get_existing_cluster(
                conn, cluster_name, die_on_error=False)
            print("Rebooting slaves...")
            for inst in slave_nodes:
                if inst.state not in ["shutting-down", "terminated"]:
                    print("Rebooting " + inst.id)
                    inst.reboot()

    elif action == "get-master":
        (master_nodes, slave_nodes) = get_existing_cluster(conn, cluster_name)
        if not master_nodes[0].public_dns_name:
            print("Master has no public DNS name.  Maybe you meant to specify --private-ips?")
        else:
            print(get_dns_name(master_nodes[0]))

    elif action == "stop":
        response = raw_input(
            "Are you sure you want to stop the cluster " +
            cluster_name + "?\nDATA ON EPHEMERAL DISKS WILL BE LOST, " +
            "BUT THE CLUSTER WILL KEEP USING SPACE ON\n" +
            "AMAZON EBS IF IT IS EBS-BACKED!!\n" +
            "All data on spot-instance slaves will be lost.\n" +
            "Stop cluster " + cluster_name + " (y/N): ")
        if response == "y":
            (master_nodes, slave_nodes) = get_existing_cluster(
                conn, cluster_name, die_on_error=False)
            print("Stopping master...")
            for inst in master_nodes:
                if inst.state not in ["shutting-down", "terminated"]:
                    inst.stop()
            print("Stopping slaves...")
            for inst in slave_nodes:
                if inst.state not in ["shutting-down", "terminated"]:
                    if inst.spot_instance_request_id:
                        inst.terminate()
                    else:
                        inst.stop()

    elif action == "start":
        (master_nodes, slave_nodes) = get_existing_cluster(conn, cluster_name)
        print("Starting slaves...")
        for inst in slave_nodes:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        print("Starting master...")
        for inst in master_nodes:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        wait_for_cluster_state(
            conn=conn,
            opts=opts,
            cluster_instances=(master_nodes + slave_nodes),
            cluster_state='ssh-ready'
        )

        # Determine types of running instances
        existing_master_type = master_nodes[0].instance_type
        existing_slave_type = slave_nodes[0].instance_type
        # Setting opts.master_instance_type to the empty string indicates we
        # have the same instance type for the master and the slaves
        if existing_master_type == existing_slave_type:
            existing_master_type = ""
        opts.master_instance_type = existing_master_type
        opts.instance_type = existing_slave_type

        setup_cluster(conn, master_nodes, slave_nodes, opts, False)

    else:
        print("Invalid action: %s" % action, file=stderr)
        sys.exit(1)


def main():
    try:
        real_main()
    except UsageError as e:
        print("\nError:\n", e, file=stderr)
        sys.exit(1)


if __name__ == '__main__':
    logging.basicConfig()
    main()
