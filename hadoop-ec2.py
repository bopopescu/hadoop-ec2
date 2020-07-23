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

DEFAULT_AMI_ID = 'ami-08e58d39fdf619dff'
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
def get_or_make_group(conn, name, vpc_id):
    groups = conn.get_all_security_groups()
    group = [g for g in groups if g.name == name]
    if len(group) > 0:
        return group[0]
    else:
        print("Creating security group " + name)
        return conn.create_security_group(name, "Hadoop group", vpc_id)


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
    Returns a tuple of lists of EC2 instance objects for the mains and subordinates.
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

    main_instances = get_instances([cluster_name + "-main"])
    subordinate_instances = get_instances([cluster_name + "-subordinates"])

    if any((main_instances, subordinate_instances)):
        print("Found {m} main{plural_m}, {s} subordinate{plural_s}.".format(
            m=len(main_instances),
            plural_m=('' if len(main_instances) == 1 else 's'),
            s=len(subordinate_instances),
            plural_s=('' if len(subordinate_instances) == 1 else 's')))

    if not main_instances and die_on_error:
        print("ERROR: Could not find a main for cluster {c} in region {r}.".format(
            c=cluster_name, r=AWS_REGION), file=sys.stderr)
        sys.exit(1)

    return main_instances, subordinate_instances


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
# Returns a tuple of EC2 reservation objects for the main and subordinates
# Fails if there already instances running in the cluster's groups.
def launch_cluster(conn, opts, cluster_name):
    if opts.identity_file is None:
        print("ERROR: Must provide an identity file (-i) for ssh connections.", file=stderr)
        sys.exit(1)

    if opts.key_pair is None:
        print("ERROR: Must provide a key pair name (-k) to use on instances.", file=stderr)
        sys.exit(1)

    authorized_address = opts.authorized_address
    vpc_id = opts.vpc_id
    print("Setting up security groups with authorized address {}, vpc id {}...".format(authorized_address, vpc_id))
    main_group = get_or_make_group(conn, cluster_name + "-main", vpc_id)
    subordinate_group = get_or_make_group(conn, cluster_name + "-subordinates", vpc_id)
    if not main_group.rules:  # Group was just now created
        main_group.authorize(src_group=main_group, ip_protocol='tcp', from_port=0, to_port=65535)
        main_group.authorize(src_group=main_group, ip_protocol='udp', from_port=0, to_port=65535)
        main_group.authorize(src_group=subordinate_group, ip_protocol='tcp', from_port=0, to_port=65535)
        main_group.authorize(src_group=subordinate_group, ip_protocol='udp', from_port=0, to_port=65535)
        main_group.authorize('tcp', 22, 22, authorized_address)
        main_group.authorize('tcp', 8080, 8081, authorized_address)
        main_group.authorize('tcp', 18080, 18080, authorized_address)
        main_group.authorize('tcp', 19999, 19999, authorized_address)
        main_group.authorize('tcp', 50030, 50030, authorized_address)
        main_group.authorize('tcp', 50070, 50070, authorized_address)
        main_group.authorize('tcp', 60070, 60070, authorized_address)
        main_group.authorize('tcp', 4040, 4045, authorized_address)
        # HDFS NFS gateway requires 111,2049,4242 for tcp & udp
        main_group.authorize('tcp', 111, 111, authorized_address)
        main_group.authorize('udp', 111, 111, authorized_address)
        main_group.authorize('tcp', 2049, 2049, authorized_address)
        main_group.authorize('udp', 2049, 2049, authorized_address)
        main_group.authorize('tcp', 4242, 4242, authorized_address)
        main_group.authorize('udp', 4242, 4242, authorized_address)
        # RM in YARN mode uses 8088
        main_group.authorize('tcp', 8088, 8088, authorized_address)
    if not subordinate_group.rules:  # Group was just now created
        subordinate_group.authorize(src_group=main_group, ip_protocol='tcp', from_port=0, to_port=65535)
        subordinate_group.authorize(src_group=main_group, ip_protocol='udp', from_port=0, to_port=65535)
        subordinate_group.authorize(src_group=subordinate_group, ip_protocol='tcp', from_port=0, to_port=65535)
        subordinate_group.authorize(src_group=subordinate_group, ip_protocol='udp', from_port=0, to_port=65535)
        subordinate_group.authorize('tcp', 22, 22, authorized_address)
        subordinate_group.authorize('tcp', 8080, 8081, authorized_address)
        subordinate_group.authorize('tcp', 50060, 50060, authorized_address)
        subordinate_group.authorize('tcp', 50075, 50075, authorized_address)
        subordinate_group.authorize('tcp', 60060, 60060, authorized_address)
        subordinate_group.authorize('tcp', 60075, 60075, authorized_address)

    # Check if instances are already running in our groups
    existing_mains, existing_subordinates = get_existing_cluster(conn, cluster_name, die_on_error=False)
    if existing_subordinates or existing_mains:
        print("ERROR: There are already instances running in group %s or %s" %
              (main_group.name, subordinate_group.name), file=stderr)
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

    # Launch subordinates
    if opts.spot_price is not None:
        # Launch spot instances with the requested price
        print("Requesting %d subordinates as spot instances with price $%.3f" % (opts.subordinates, opts.spot_price))
        my_req_ids = []
        subordinate_reqs = conn.request_spot_instances(
            price=opts.spot_price,
            image_id=opts.ami,
            launch_group="launch-group-%s" % cluster_name,
            placement=AWS_AZ,
            count=opts.subordinates,
            key_name=opts.key_pair,
            security_group_ids=[subordinate_group.id],
            instance_type=opts.instance_type,
            subnet_id=opts.subnet_id)
        my_req_ids += [req.id for req in subordinate_reqs]

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
                if len(active_instance_ids) == opts.subordinates:
                    print("All %d subordinates granted" % opts.subordinates)
                    reservations = conn.get_all_reservations(active_instance_ids)
                    subordinate_nodes = []
                    for r in reservations:
                        subordinate_nodes += r.instances
                    break
                else:
                    print("%d of %d subordinates granted, waiting longer" % (len(active_instance_ids), opts.subordinates))
        except:
            print("Canceling spot instance requests")
            conn.cancel_spot_instance_requests(my_req_ids)
            # Log a warning if any of these requests actually launched instances:
            (main_nodes, subordinate_nodes) = get_existing_cluster(conn, cluster_name, die_on_error=False)
            running = len(main_nodes) + len(subordinate_nodes)
            if running:
                print(("WARNING: %d instances are still running" % running), file=stderr)
            sys.exit(0)
    else:
        # Launch non-spot instances
        subordinate_nodes = []
        subordinate_res = image.run(
            key_name=opts.key_pair,
            security_group_ids=[subordinate_group.id],
            instance_type=opts.instance_type,
            placement=AWS_AZ,
            min_count=opts.subordinates,
            max_count=opts.subordinates,
            subnet_id=opts.subnet_id)
        subordinate_nodes += subordinate_res.instances
        print("Launched {s} subordinate{plural_s} in {z}, regid = {r}".format(
            s=opts.subordinates,
            plural_s=('' if opts.subordinates == 1 else 's'),
            z=AWS_AZ,
            r=subordinate_res.id))

    # Launch or resume mains
    if existing_mains:
        print("Starting main...")
        for inst in existing_mains:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        main_nodes = existing_mains
    else:
        main_type = opts.main_instance_type
        if main_type == "":
            main_type = opts.instance_type
        main_res = image.run(
            key_name=opts.key_pair,
            security_group_ids=[main_group.id],
            instance_type=main_type,
            placement=AWS_AZ,
            min_count=1,
            max_count=1,
            subnet_id=opts.subnet_id)

        main_nodes = main_res.instances
        print("Launched main in %s, regid = %s" % (AWS_AZ, main_res.id))

    # This wait time corresponds to SPARK-4983
    print("Waiting for AWS to propagate instance metadata...")
    time.sleep(15)

    for main in main_nodes:
        main.add_tags(
            dict(Name='{cn}-main-{iid}'.format(cn=cluster_name, iid=main.id))
        )

    for subordinate in subordinate_nodes:
        subordinate.add_tags(
            dict(Name='{cn}-subordinate-{iid}'.format(cn=cluster_name, iid=subordinate.id))
        )

    # Return all the instances
    return main_nodes, subordinate_nodes


# Deploy the configuration file templates in a given local directory to
# a cluster, filling in any template parameters with information about the
# cluster (e.g. lists of mains and subordinates). Files are only deployed to
# the first main instance in the cluster, and we expect the setup
# script to be run on that instance to copy them to other nodes.
#
# root_dir should be an absolute path to the directory with the files we want to deploy.
def deploy_files(conn, root_dir, opts, main_nodes, subordinate_nodes, modules):
    active_main = get_dns_name(main_nodes[0])

    main_addresses = [get_dns_name(i) for i in main_nodes]
    subordinate_addresses = [get_dns_name(i) for i in subordinate_nodes]
    template_vars = {"main_list": '\n'.join(main_addresses),
                     "active_main": active_main,
                     "subordinate_list": '\n'.join(subordinate_addresses),
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
    # rsync the whole directory over to the main machine
    command = [
        'rsync', '-rv',
        '-e', stringify_command(ssh_command(opts)),
        "%s/" % tmp_dir,
        "%s@%s:/" % (HADOOP_USER, active_main)
    ]
    subprocess.check_call(command)
    # Remove the temp directory we created above
    shutil.rmtree(tmp_dir)


def setup_hadoop_cluster(main, opts):
    ssh(main, opts, "chmod u+x hadoop-ec2/setup.sh")
    ssh(main, opts, "hadoop-ec2/setup.sh")
    print("Hadoop standalone cluster started at http://%s:9000" % main)


# Deploy configuration files and run setup scripts on a newly launched
# or started EC2 cluster.
def setup_cluster(conn, main_nodes, subordinate_nodes, opts, deploy_ssh_key):
    main = get_dns_name(main_nodes[0])
    if deploy_ssh_key:
        print("Generating cluster's SSH key on main...")
        key_setup = """
          [ -f ~/.ssh/id_rsa ] ||
            (ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa &&
             cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys)
        """
        ssh(main, opts, key_setup)
        dot_ssh_tar = ssh_read(main, opts, ['tar', 'c', '.ssh'])
        print("Transferring cluster's SSH key to subordinates...")
        for subordinate in subordinate_nodes:
            subordinate_address = get_dns_name(subordinate)
            print(subordinate_address)
            ssh_write(subordinate_address, opts, ['tar', 'x'], dot_ssh_tar)

    modules = ['hadoop', 'hive']

    # NOTE: We should clone the repository before running deploy_files to
    # prevent ec2-variables.sh from being overwritten
    print("Copying hadoop-ec2 scripts from {p} on main...".format(p=HADOOP_EC2_DIR))
    scp(host=main,
        opts=opts,
        src=HADOOP_EC2_DIR)

    print("Deploying files to main...")
    deploy_files(
        conn=conn,
        root_dir=HADOOP_EC2_DIR + "/" + "deploy.generic",
        opts=opts,
        main_nodes=main_nodes,
        subordinate_nodes=subordinate_nodes,
        modules=modules
    )

    print("Running setup on main...")
    setup_hadoop_cluster(main, opts)
    print("Done!")


def parse_args():
    parser = OptionParser(
        prog="hadoop-ec2",
        version="%prog",
        usage="%prog [options] <action> <cluster_name>\n\n"
              + "<action> can be: launch, destroy, login, stop, start, get-main, reboot-subordinates")

    parser.add_option(
        "-s", "--subordinates", type="int", default=1,
        help="Number of subordinates to launch (default: %default)")
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
        "-m", "--main-instance-type", default="m4.10xlarge",
        help="Main instance type (leave empty for same as instance-type)")
    parser.add_option(
        "--spot-price", metavar="PRICE", type="float",
        help="If specified, launch subordinates as spot instances with the given " +
             "maximum price (in dollars)")
    parser.add_option(
        "-a", "--ami",
        help="Amazon Machine Image ID to use")
    parser.add_option(
        "--authorized-address", type="string", default="0.0.0.0/0",
        help="Address to authorize on created security groups (default: %default)")
    parser.add_option(
        "--vpc-id", default=None,
        help="VPC to launch instances in")
    parser.add_option(
        "--subnet-id", default=None,
        help="VPC subnet to launch instances in")

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

    if opts.main_instance_type != "":
        if opts.main_instance_type not in EC2_INSTANCE_TYPES:
            print("Warning: Unrecognized EC2 instance type for main-instance-type: {t}".format(
                t=opts.main_instance_type), file=stderr)
        # Since we try instance types even if we can't resolve them, we check if they resolve first
        # and, if they do, see if they resolve to the same virtualization type.
        if opts.instance_type in EC2_INSTANCE_TYPES and \
                opts.main_instance_type in EC2_INSTANCE_TYPES:
            if EC2_INSTANCE_TYPES[opts.instance_type] != \
                    EC2_INSTANCE_TYPES[opts.main_instance_type]:
                print("Error: spark-ec2 currently does not support having a main and subordinates "
                      "with different AMI virtualization types.", file=stderr)
                print("main instance virtualization type: {t}".format(
                    t=EC2_INSTANCE_TYPES[opts.main_instance_type]), file=stderr)
                print("subordinate instance virtualization type: {t}".format(
                    t=EC2_INSTANCE_TYPES[opts.instance_type]), file=stderr)
                sys.exit(1)

    try:
        conn = ec2.connect_to_region(AWS_REGION)
    except Exception as e:
        print(e, file=stderr)
        sys.exit(1)

    if action == "launch":
        if opts.subordinates <= 0:
            print("ERROR: You have to start at least 1 subordinate", file=sys.stderr)
            sys.exit(1)
        (main_nodes, subordinate_nodes) = launch_cluster(conn, opts, cluster_name)
        wait_for_cluster_state(
            conn=conn,
            opts=opts,
            cluster_instances=(main_nodes + subordinate_nodes),
            cluster_state='ssh-ready'
        )
        setup_cluster(conn, main_nodes, subordinate_nodes, opts, True)

    elif action == "destroy":
        (main_nodes, subordinate_nodes) = get_existing_cluster(
            conn, cluster_name, die_on_error=False)

        if any(main_nodes + subordinate_nodes):
            print("The following instances will be terminated:")
            for inst in main_nodes + subordinate_nodes:
                print("> %s" % get_dns_name(inst))
            print("ALL DATA ON ALL NODES WILL BE LOST!!")

        msg = "Are you sure you want to destroy the cluster {c}? (y/N) ".format(c=cluster_name)
        response = raw_input(msg)
        if response == "y":
            print("Terminating main...")
            for inst in main_nodes:
                inst.terminate()
            print("Terminating subordinates...")
            for inst in subordinate_nodes:
                inst.terminate()

            # Delete security groups as well
            if opts.delete_groups:
                group_names = [cluster_name + "-main", cluster_name + "-subordinates"]
                wait_for_cluster_state(
                    conn=conn,
                    opts=opts,
                    cluster_instances=(main_nodes + subordinate_nodes),
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
        (main_nodes, subordinate_nodes) = get_existing_cluster(conn, cluster_name)
        if not main_nodes[0].public_dns_name:
            print("Main has no public DNS name.  Maybe you meant to specify --private-ips?")
        else:
            main = get_dns_name(main_nodes[0])
            print("Logging into main " + main + "...")
            subprocess.check_call(
                ssh_command(opts) + ['-t', '-t', "%s@%s" % (HADOOP_USER, main)])

    elif action == "reboot-subordinates":
        response = raw_input(
            "Are you sure you want to reboot the cluster " +
            cluster_name + " subordinates?\n" +
            "Reboot cluster subordinates " + cluster_name + " (y/N): ")
        if response == "y":
            (main_nodes, subordinate_nodes) = get_existing_cluster(
                conn, cluster_name, die_on_error=False)
            print("Rebooting subordinates...")
            for inst in subordinate_nodes:
                if inst.state not in ["shutting-down", "terminated"]:
                    print("Rebooting " + inst.id)
                    inst.reboot()

    elif action == "get-main":
        (main_nodes, subordinate_nodes) = get_existing_cluster(conn, cluster_name)
        if not main_nodes[0].public_dns_name:
            print("Main has no public DNS name.  Maybe you meant to specify --private-ips?")
        else:
            print(get_dns_name(main_nodes[0]))

    elif action == "stop":
        response = raw_input(
            "Are you sure you want to stop the cluster " +
            cluster_name + "?\nDATA ON EPHEMERAL DISKS WILL BE LOST, " +
            "BUT THE CLUSTER WILL KEEP USING SPACE ON\n" +
            "AMAZON EBS IF IT IS EBS-BACKED!!\n" +
            "All data on spot-instance subordinates will be lost.\n" +
            "Stop cluster " + cluster_name + " (y/N): ")
        if response == "y":
            (main_nodes, subordinate_nodes) = get_existing_cluster(
                conn, cluster_name, die_on_error=False)
            print("Stopping main...")
            for inst in main_nodes:
                if inst.state not in ["shutting-down", "terminated"]:
                    inst.stop()
            print("Stopping subordinates...")
            for inst in subordinate_nodes:
                if inst.state not in ["shutting-down", "terminated"]:
                    if inst.spot_instance_request_id:
                        inst.terminate()
                    else:
                        inst.stop()

    elif action == "start":
        (main_nodes, subordinate_nodes) = get_existing_cluster(conn, cluster_name)
        print("Starting subordinates...")
        for inst in subordinate_nodes:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        print("Starting main...")
        for inst in main_nodes:
            if inst.state not in ["shutting-down", "terminated"]:
                inst.start()
        wait_for_cluster_state(
            conn=conn,
            opts=opts,
            cluster_instances=(main_nodes + subordinate_nodes),
            cluster_state='ssh-ready'
        )

        # Determine types of running instances
        existing_main_type = main_nodes[0].instance_type
        existing_subordinate_type = subordinate_nodes[0].instance_type
        # Setting opts.main_instance_type to the empty string indicates we
        # have the same instance type for the main and the subordinates
        if existing_main_type == existing_subordinate_type:
            existing_main_type = ""
        opts.main_instance_type = existing_main_type
        opts.instance_type = existing_subordinate_type

        setup_cluster(conn, main_nodes, subordinate_nodes, opts, False)

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
