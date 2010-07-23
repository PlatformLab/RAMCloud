#!/bin/bash
set -e
(cd ..; make ./obj.fasttransport/FastEcho)
TIME=5
CAP_FILE=/tmp/x.cap
sudo rm -f $CAP_FILE
sudo dumpcap -a duration:$TIME -i lo -w $CAP_FILE &
sleep .1
../obj.fasttransport/FastEcho &
SERVER_PID=$!
sleep .1
python client.py &
CLIENT_PID=$!
trap "kill $SERVER_PID; kill $CLIENT_PID" exit
sleep $TIME
kill $SERVER_PID || true
kill $CLIENT_PID || true
trap - exit
sudo chmod 666 $CAP_FILE
wireshark -Xlua_script:wireshark_rpc.lua $CAP_FILE
