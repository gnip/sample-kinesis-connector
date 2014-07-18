#!/bin/bash

pushd `dirname $0` > /dev/null

AMI_ID=ami-60759008

INSTANCE_NAME=${1}

if [[ $# -eq 0 ]] ; then
    echo 'Please specify an instance name'
    exit 0
fi

echo "Creating instance: ${INSTANCE_NAME}"

INSTANCE_ID=`ec2-run-instances ${AMI_ID} \
    -k Kinesis-Deploy-2 \
    -p kinesis-all \
    -t m1.large \
    --availability-zone us-east-1b \
    -g sg-46ac1423 | grep INSTANCE | cut -f2`

# Wait until the instance has started
echo Starting instance $INSTANCE_ID

source ./wait-until-instance-running.incl.sh

ec2-create-tags $INSTANCE_ID \
  --tag Name=$INSTANCE_NAME \

echo -n "Waiting for instance to be up.."
for (( i=1; i <= 40; i++ )); do
    echo -n "."
    sleep 1
done
echo ""

bash deploy-kineserator.sh $INSTANCE_ID ${2}

popd > /dev/null
