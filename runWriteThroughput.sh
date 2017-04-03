 #!/bin/bash

#Repeat writeThroughput benchmark to reduce variance.
((REP = 5))

#Sizes of the indexes as a power of 10
 SIZES="20"
# SIZES=$(seq 50 -1 1)

LOGFILE="$(date +%Y%m%d%H%M%S).data"
eval $1 | tee "$LOGFILE"

for SIZE in $SIZES; do
#  for CLISIZE in $(seq $SIZE -4 3); do
    ((CLISIZE = 20))
    servlist=""
    count=0
    clilist=""
    clientCount=0;
    echo "batchSize: $SIZE" >> $LOGFILE

    for ITER in $(seq $REP -1 1); do
#        ./scripts/clusterperf.py writeThroughput -T infrc --asyncReplication 1 -n $CLISIZE --masterArgs "-t 4000 --syncBatchSize $SIZE" >> $LOGFILE
        ./scripts/clusterperf.py writeThroughput -T infrc --asyncReplication 0 -n $CLISIZE --masterArgs "-t 4000 --syncBatchSize $SIZE" >> $LOGFILE
    done
    sleep 4
#  done
done

sleep 10

./scripts/parseWriteThroughputByBatchSize.py $LOGFILE
