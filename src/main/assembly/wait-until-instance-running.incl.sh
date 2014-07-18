#!/bin/sh
# Wait until the instance has started
DONE="false"
while [ "$DONE" != "true" ]
do
   STATUS=`ec2-describe-instances $INSTANCE_ID | grep INSTANCE | cut -f6`
   if [ "$STATUS" = "running" ]; then
      DONE="true"
   else
      echo Waiting...$STATUS
      sleep 10
   fi
done
echo $INSTANCE_ID Is Running