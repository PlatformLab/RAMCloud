 #!/bin/bash

#Repeat writeThroughput benchmark to reduce variance.
((REP = 5))

#Sizes of the indexes as a power of 10
# SIZES="20"
 SIZES=$(seq 50 -1 1)

#WITNESSNUM = "2 1 0"
WITNESSNUM=$(seq 2 -1 0)
for WF in $WITNESSNUM; do
  cp src/Minimal_w$WF.h src/Minimal.h
  make clean
  make -j16 DEBUG=no

  LOGFILE="$(date +%Y%m%d%H%M%S).data"
  eval $1 | tee "$LOGFILE"

  echo "Built RAMCloud with $WF witnesses."

  for SIZE in $SIZES; do
    ((CLISIZE = 20))
    echo "Running benchmark for WF: $WF and BatchSize: $SIZE. Logfile: $LOGFILE"
    echo "batchSize: $SIZE" >> $LOGFILE

    for ITER in $(seq $REP -1 1); do
        ./scripts/clusterperf.py writeThroughput -T infrc --asyncReplication 1 -n $CLISIZE --masterArgs "-t 4000 --syncBatchSize $SIZE --maxCores 7" >> $LOGFILE
    done
    sleep 4
  done
  sleep 10

  ./scripts/parseWriteThroughputByBatchSize.py $LOGFILE > ~/resultRAMCloud/writeThroughputByBatchSize-witness$WF.data
done

mv ~/resultRAMCloud/writeThroughputByBatchSize-witness0.data ~/resultRAMCloud/writeThroughputByBatchSize-cgarc.data

#####################################
# RUN synchronous writes
#####################################
  cp src/Minimal_w0.h src/Minimal.h
  make clean
  make -j16 DEBUG=no

  LOGFILE="$(date +%Y%m%d%H%M%S).data"
  eval $1 | tee "$LOGFILE"

  echo "Running synchronous writes. RAMCloud with 0 witnesses."

  for SIZE in $SIZES; do
    ((CLISIZE = 20))
    echo "Running benchmark for WF: $WF and BatchSize: $SIZE. Logfile: $LOGFILE"
    echo "batchSize: $SIZE" >> $LOGFILE

    for ITER in $(seq $REP -1 1); do
        ./scripts/clusterperf.py writeThroughput -T infrc --asyncReplication 0 -n $CLISIZE --masterArgs "-t 4000 --syncBatchSize $SIZE --maxCores 7" >> $LOGFILE
    done
    sleep 4
  done
  sleep 10

  ./scripts/parseWriteThroughputByBatchSize.py $LOGFILE > ~/resultRAMCloud/writeThroughputByBatchSize-sync.data
