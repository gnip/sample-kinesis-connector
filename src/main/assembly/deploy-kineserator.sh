#!/bin/sh

INSTANCE_ID=${1}
KEY_FILE="Kinesis-Deploy-2.pem"

if [[ $# -eq 0 ]] ; then
    echo 'Please specify an instance id'
    exit 0
fi

ADMIN_USER=root
SSH_PARAMS="-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"

SERVER_HOST=`ec2-describe-instances $INSTANCE_ID | grep INSTANCE | cut -f17`

COMMON="unzip -o target.zip -d /; \
        rm target.zip; \
        chmod 744 /opt/gnip/connector/bin/connector; \
        chown -R gnip:gnip /opt/gnip /home/gnip"

if [[ "$2" == "dev" ]] ; then
    AMI_TARGET_FILE="../../../target/connector-1.0-ami-dev.zip "
    SSH_SCRIPT=$COMMON
    ON_DONE="ssh -i ${KEY_FILE} ${ADMIN_USER}@${SERVER_HOST}"

else
    AMI_TARGET_FILE="../../../target/connector-1.0-ami.zip "
    SSH_SCRIPT="${COMMON}; \
        rm -rf .ssh; \
        rm /etc/ssh/ssh_host*; \
        rm .bash_history"
    ON_DONE="
###################################################
###################################################
Now:
 1. Go to the EC 2 console
 2. Wait for the initialization checks on the instance to finish
 3. Either reboot or create an Image
 4. If you reboot you should now be able to ssh as gnip
 5. If not... then you can create an instance of the image and
 ssh to it as gnip
 ##################################################

 You'll be able to connect using :

 ssh -i ${KEY_FILE} gnip@${SERVER_HOST}
"
fi

echo $AMI_TARGET_FILE
echo $SSH_SCRIPT

if [[ -z "$SERVER_HOST" ]]; then
    echo "Could not find instance with id $INSTANCE_ID"
    exit 0
fi

echo "Host: ${SERVER_HOST}"
echo scp ${SSH_PARAMS} -i ${KEY_FILE} ${AMI_TARGET_FILE} ${ADMIN_USER}@${SERVER_HOST}:~/target.zip

scp ${SSH_PARAMS} -i ${KEY_FILE} ${AMI_TARGET_FILE} ${ADMIN_USER}@${SERVER_HOST}:~/target.zip

echo ssh ${SSH_PARAMS} -i ${KEY_FILE} ${ADMIN_USER}@${SERVER_HOST} ${SSH_SCRIPT}
ssh ${SSH_PARAMS} -i ${KEY_FILE} ${ADMIN_USER}@${SERVER_HOST} ${SSH_SCRIPT}

echo "${ON_DONE}"
