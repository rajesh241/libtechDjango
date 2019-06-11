#!/bin/bash
#First we will kill the process if it is older than 3 hours
cd /home/$1/repo/venv/
source bin/activate
cmd="python /home/$1/repo/libtechDjango/src/nrega/crawler/code/jharkhandPDS.py -se -limit $2"
#echo $cmd
#$cmd
myPID=$(pgrep -f "$cmd")

echo $myPID
if [ -z "$myPID" ]
then
  echo "Variable is empty"
else
  echo "Variable is not empty"
  myTime=`ps -o etimes= -p "$myPID"`
  echo $myTime
  if [ $myTime -gt 10000 ]
    then 
      echo "Time is about 3 hours"
      kill -9 $myPID
  fi
fi
pgrep -f "$cmd" || $cmd &> /tmp/jharkhandPDSCron_$1_$2.log
