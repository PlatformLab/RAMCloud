 #!/bin/bash

# Do not repeat latency benchmark.
((REP = 1))

#Sizes of the indexes as a power of 10
SYNCTIMES="0 3 10 30 100 300 1000 3000 10000 30000 100000"
#SYNCTIMES="100000"
#SYNCTIMES=$(seq 0 2 300)
#SYNCTIMES="100000"

#LOGFILE="$(date +%Y%m%d%H%M%S).data"
#eval $1 | tee "$LOGFILE"

OUTPUTPREFIX=$1"-batch"

for SYNCTIME in $SYNCTIMES; do
    echo "batchSize: $SIZE outputFile: ~/resultRAMCloud/writeDistBySyncLatency-$SYNCTIME-w2.cdf"

    for ITER in $(seq $REP -1 1); do
#        ./scripts/clusterperf.py writeDistRandom -T infrc --asyncReplication 1 --count 100000 --masterArgs "-t 2000 --syncBatchSize 50 --maxCores 6 --segmentFrames 2048 --syncMinUsec $SYNCTIME" --servers 0 --masters 1 --backups 3 --witnesses 2 --timeout 3000 > ~/resultRAMCloud/writeDistBySyncLatency-$SYNCTIME.cdf
        ./scripts/clusterperf.py writeDistRandom -T infrc --asyncReplication 1 --count 100000 --masterArgs "-t 2000 --syncBatchSize 50 --maxCores 7 --segmentFrames 2048 --syncMinUsec $SYNCTIME" --servers 0 --masters 1 --backups 3 --witnesses 2 --timeout 300 > ~/resultRAMCloud/writeDistBySyncLatency-$SYNCTIME-w2.cdf
    done
    sleep 4
done

#sleep 10
#./scripts/parseWriteThroughputByBatchSize.py $LOGFILE
