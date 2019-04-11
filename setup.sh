#!/bin/bash

# usage: echo_time_diff name start_time end_time
echo_time_diff () {
  local format='%Hh %Mm %Ss'

  local diff_secs="$(($3-$2))"
  echo "[timing] $1: " "$(date -u -d@"$diff_secs" +"$format")"
}

# Make sure we are in the hadoop-ec2 directory
pushd ${HOME}/hadoop-ec2 > /dev/null

# Load the environment variables specific to this AMI
source ${HOME}/.bashrc
source ${HOME}/.bash_profile

# Load the cluster variables set by the deploy script
source ec2-variables.sh

echo "Master: ${MASTERS}"
echo "Slaves: ${SLAVES}"

# Set hostname based on EC2 private DNS name, so that it is set correctly
# even if the instance is restarted with a different private DNS name
PRIVATE_DNS=`wget -q -O - http://169.254.169.254/latest/meta-data/local-hostname`
PUBLIC_DNS=`wget -q -O - http://169.254.169.254/latest/meta-data/hostname`
sudo hostname ${PRIVATE_DNS}
echo ${PRIVATE_DNS} | sudo tee /etc/hostname
export HOSTNAME=${PRIVATE_DNS}  # Fix the bash built-in hostname variable too

echo "Setting up Hadoop on `hostname`..."

# Set up the masters, slaves, etc files based on cluster env variables
echo "${MASTERS}" > masters
echo "${SLAVES}" > slaves

MASTERS=`cat masters`
NUM_MASTERS=`cat masters | wc -l`
OTHER_MASTERS=`cat masters | sed '1d'`
SLAVES=`cat slaves`
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=5"

if [[ "x${JAVA_HOME}" == "x" ]] ; then
    echo "Expected JAVA_HOME to be set in .bash_profile!"
    exit 1
fi

if [[ `tty` == "not a tty" ]] ; then
    echo "Expecting a tty or pty! (use the ssh -t option)."
    exit 1
fi

echo "Setting executable permissions on scripts..."
find . -regex "^.+.\(sh\|py\)" | xargs chmod a+x

echo "RSYNC'ing ${HOME}/hadoop-ec2 to other cluster nodes..."
rsync_start_time="$(date +'%s')"
for node in ${SLAVES} ${OTHER_MASTERS}; do
  echo ${node}
  rsync -e "ssh ${SSH_OPTS}" -az ${HOME}/hadoop-ec2 ${node}:${HOME} &
  scp ${SSH_OPTS} ~/.ssh/id_rsa ${node}:.ssh &
  sleep 0.1
done
wait
rsync_end_time="$(date +'%s')"
echo_time_diff "rsync ${HOME}/hadoop-ec2" "$rsync_start_time" "$rsync_end_time"

# Install / Init module
for module in ${MODULES}; do
  echo "Initializing $module"
  module_init_start_time="$(date +'%s')"
  if [[ -e ${module}/init.sh ]]; then
    source ${module}/init.sh
  fi
  module_init_end_time="$(date +'%s')"
  echo_time_diff "$module init" "$module_init_start_time" "$module_init_end_time"
  cd ${HOME}/hadoop-ec2  # guard against init.sh changing the cwd
done

# Setup each module
for module in ${MODULES}; do
  echo "Setting up $module"
  module_setup_start_time="$(date +'%s')"
  source ./${module}/setup.sh
  sleep 0.1
  module_setup_end_time="$(date +'%s')"
  echo_time_diff "$module setup" "$module_setup_start_time" "$module_setup_end_time"
  cd ${HOME}/hadoop-ec2  # guard against setup.sh changing the cwd
done

popd > /dev/null