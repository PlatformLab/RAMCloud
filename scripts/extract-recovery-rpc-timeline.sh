#!/bin/bash

# ReplicatedSegment::LOG_RECOVERY_REPLICATION_RPC_TIMING. Used to visualize
# log replication progress during recovery using log messages.

if [[ "$1" == "" ]]; then
    echo "Usage: $0 [-h] <recovery_master_log_file>"
    echo "  -h Human readable. Otherwise output for plot with timeline.gnuplot"
    echo
    exit -1
fi

if [[ "$1" == "-h" ]]; then
    HUMAN="true"
    shift
fi
RECOVERY_MASTER_LOG_FILE=$1

if [[ "$HUMAN" == "true" ]]; then
    egrep -r '@[ 0-9]+:' $RECOVERY_MASTER_LOG_FILE | \
        sed 's/^.*]: //'
else
    egrep -r '@[ 0-9]+:' $RECOVERY_MASTER_LOG_FILE | \
        sed 's/^.*]: //' | \
        grep '<-' | \
        perl -pe 's|@(.*):.*?,([0-9]+).*<- *?([0-9]+).*$|\1 \2 \3|' | \
        awk '{print($1 " "  $2 * 1024 * 1024 * 8 + $3);}'
fi

