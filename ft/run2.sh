#!/bin/bash
set -e

SERVER="python server.py"
CLIENT="python client.py"
while getopts "sc" OPT; do
    case $OPT in
        s)
            SERVER="../obj.master/Echo"
            ;;
        c)
            CLIENT="../obj.master/Telnet -g -s fast+udp:host=127.0.0.1,port=12242"
            ;;
    esac
done

(cd ..; make ./obj.master/{Echo,Telnet})
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
