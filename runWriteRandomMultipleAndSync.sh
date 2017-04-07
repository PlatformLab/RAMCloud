 #!/bin/bash

#Repeat writeThroughput benchmark to reduce variance.
((REP = 1))

#Sizes of the indexes as a power of 10
 SIZES="20"
# SIZES=$(seq 50 -1 1)

for SIZE in $SIZES; do
  for SPAN in $(seq 1 1 5); do
    ((CLISIZE = 20))
    servlist=""
    count=0
    clilist=""
    clientCount=0;

    for ITER in $(seq $REP -1 1); do
        ./scripts/clusterperf.py writeRandomMultipleAndSync -T infrc --txSpan $SPAN --asyncReplication 1 --masterArgs "-t 4000 --syncBatchSize $SIZE" -n 1 > ~/resultRAMCloud/writeRandomMultipleAndSync-cgarc-span$SPAN.data
        ./scripts/clusterperf.py writeRandomMultipleAndSync -T infrc --txSpan $SPAN --asyncReplication 0 --masterArgs "-t 4000 --syncBatchSize $SIZE" -n 1 > ~/resultRAMCloud/writeRandomMultipleAndSync-sync-span$SPAN.data
    done
    sleep 4
  done
done
