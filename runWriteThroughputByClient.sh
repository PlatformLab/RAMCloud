 #!/bin/bash

#Repeat writeThroughput benchmark to reduce variance.
((REP = 1))

#Sizes of the indexes as a power of 10
# SIZES="3 5 10 20 50"
 SIZES="20"
# SIZES=$(seq 50 -1 1)

for SIZE in $SIZES; do
  for CLISIZE in $(seq 6 5 26); do
#    ((CLISIZE = 20))
    servlist=""
    count=0
    clilist=""
    clientCount=0;
    echo "batchSize: $SIZE outputFile: ~/resultRAMCloud/writeThroughputByClients-$1-batch$SIZE.data"

    for ITER in $(seq $REP -1 1); do
#        ./scripts/clusterperf.py writeThroughput -T infrc --asyncReplication 1 -n $CLISIZE --masterArgs "-t 4000 --syncBatchSize $SIZE" >> ~/resultRAMCloud/writeThroughputByClients-$1-batch$SIZE.data
        ./scripts/clusterperf.py writeThroughput -T infrc --asyncReplication 0 -n $CLISIZE --masterArgs "-t 4000 --syncBatchSize $SIZE" >> ~/resultRAMCloud/writeThroughputByClients-sync.data
    done
    sleep 4
  done
done

sleep 10

#./scripts/parseWriteThroughputByBatchSize.py $LOGFILE
