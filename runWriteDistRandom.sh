 #!/bin/bash

# Do not repeat latency benchmark.
((REP = 1))

#Sizes of the indexes as a power of 10
 SIZES="3 5 10 20 50"
# SIZES=$(seq 50 -1 1)

#LOGFILE="$(date +%Y%m%d%H%M%S).data"
#eval $1 | tee "$LOGFILE"

OUTPUTPREFIX=$1"-batch"

for SIZE in $SIZES; do
#  for CLISIZE in $(seq $SIZE -4 3); do
#    ((CLISIZE = 20))
    servlist=""
    count=0
    clilist=""
    clientCount=0;
    echo "batchSize: $SIZE outputFile: ~/resultRAMCloud/$OUTPUTPREFIX$SIZE.rcdf"

    for ITER in $(seq $REP -1 1); do
        ./scripts/clusterperf.py writeDistRandom -T infrc --asyncReplication 1 --count 1000000 --servers 0 --masters 2 --backups 3 --witnesses 2 --rcdf --masterArgs "-t 2000 --syncBatchSize $SIZE" > ~/resultRAMCloud/$OUTPUTPREFIX$SIZE.rcdf
        ./scripts/clusterperf.py writeDistRandom -T infrc --asyncReplication 1 --count 1000000 --servers 0 --masters 2 --backups 3 --witnesses 2 --masterArgs "-t 2000 --syncBatchSize $SIZE" > ~/resultRAMCloud/$OUTPUTPREFIX$SIZE.cdf
    done
    sleep 4
#  done
done

#sleep 10
#./scripts/parseWriteThroughputByBatchSize.py $LOGFILE
