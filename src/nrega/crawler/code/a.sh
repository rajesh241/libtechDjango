#!/bin/bash
#First we will kill the process if it is older than 3 hours
cd /home/crawler/django/venv/
source bin/activate
cmd="python /home/crawler/django/src/nrega/crawler/code/debug.py -se -ti $1"
#echo $cmd
#$cmd
sleep $1
myPID=$(pgrep -f "$cmd")

echo $myPID
if [ -z "$myPID" ]
then
  echo "Variable is empty"
else
  echo "Variable is not empty"
  myTime=`ps -o etimes= -p "$myPID"`
  echo $myTime
  if [ $myTime -gt 9000 ]
    then 
      echo "Time is about 3 hours"
      kill -9 $myPID
  fi
fi
pgrep -f "$cmd" || $cmd &> /tmp/debug_$1.log
