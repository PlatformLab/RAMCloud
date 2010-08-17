#!/bin/bash
set -e

SERVER="python server.py"
CLIENT="python client.py"
while getopts "sc" OPT; do
    case $OPT in
        s)
            SERVER="../obj.fasttransport/FastEcho -c 0"
            ;;
        c)
            CLIENT="../obj.fasttransport/FastTelnet -c 1 -g"
            ;;
    esac
done

(cd ..; make ./obj.fasttransport/Fast{Echo,Telnet})
TIME=1
CAP_FILE=/tmp/x.cap
sudo rm -f $CAP_FILE
sudo dumpcap -a duration:$TIME -i lo -w $CAP_FILE &
sleep .1
$SERVER &
SERVER_PID=$!
sleep .1
$CLIENT &
CLIENT_PID=$!
trap "kill $SERVER_PID; kill $CLIENT_PID" exit
sleep $TIME
kill $SERVER_PID || true
kill $CLIENT_PID || true
trap - exit
sudo chmod 666 $CAP_FILE
wireshark -Xlua_script:wireshark_rpc.lua $CAP_FILE
