#!/bin/bash

cd ${0%/*}
STR="infrc:host=192.168.1.126,port=12246"
if [ $# -ne 0 ]
then
    STR="$@"
fi
java -Xverify:none -Xmx1024M -Xms1024M -cp "bin:lib/*" -Djava.library.path=lib edu.stanford.ramcloud.TestClient $STR
