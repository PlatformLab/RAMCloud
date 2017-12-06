 #!/bin/bash

#Repeat writeThroughput benchmark to reduce variance.
((REP = 3))

#Sizes of the indexes as a power of 10
# SIZES="20"
 SIZES=$(seq 50 -2 1)

WITNESSNUM=$(seq 3 -1 0)
for WF in $WITNESSNUM; do
  cp src/Minimal_w$WF.h src/Minimal.h
  make clean
  make -j16 DEBUG=no

  RF=$WF
  if [[ $WF -eq 0 ]]; then
    RF=3
  fi

  LOGFILE="$(date +%Y%m%d%H%M%S).data"
  eval $1 | tee "$LOGFILE"

  echo "Built RAMCloud with $WF witnesses."

  for SIZE in $SIZES; do
    ((CLISIZE = 25))
    echo "Running benchmark for WF: $WF and BatchSize: $SIZE. Logfile: $LOGFILE"
    echo "batchSize: $SIZE" >> $LOGFILE

    for ITER in $(seq $REP -1 1); do
#        ./scripts/clusterperf.py writeThroughput -T infrc --asyncReplication 1 -n $CLISIZE --masterArgs "-t 4000 --syncBatchSize $SIZE --maxCores 7" >> $LOGFILE
        ./scripts/clusterperf.py writeThroughput -T infrc --asyncReplication 1 -r $RF  --servers 0 --masters 2 --backups 3 --witnesses 3 -n $CLISIZE --masterArgs "-t 4000 --syncBatchSize $SIZE --maxCores 7" >> $LOGFILE
    done
    sleep 4
  done
  sleep 10

  ./scripts/parseWriteThroughputByBatchSize.py $LOGFILE > ~/resultRAMCloud/writeThroughputByBatchSize-witness$WF.data
done

mv ~/resultRAMCloud/writeThroughputByBatchSize-witness0.data ~/resultRAMCloud/writeThroughputByBatchSize-async.data

#####################################
# RUN synchronous and unreplicated writes
#####################################
  cp src/Minimal_w0.h src/Minimal.h
  make clean
  make -j16 DEBUG=no

  LOGFILE="$(date +%Y%m%d%H%M%S).data"
  eval $1 | tee "$LOGFILE"

  echo "Running synchronous writes. RAMCloud with 0 witnesses."

  ((CLISIZE = 25))
  echo "Running benchmark for WF: $WF and BatchSize: $SIZE. Logfile: $LOGFILE"
  echo "batchSize: $SIZE" >> $LOGFILE

  for ITER in $(seq $REP -1 1); do
        ./scripts/clusterperf.py writeThroughput -T infrc --asyncReplication 0 -n $CLISIZE --masterArgs "-t 4000 --maxCores 7" >> $LOGFILE
  done
  sleep 10

  ./scripts/parseWriteThroughputByBatchSize.py $LOGFILE > ~/resultRAMCloud/writeThroughputByBatchSize-sync.data

# RUN unreplicated writes
########################
  LOGFILE="$(date +%Y%m%d%H%M%S).data"
  eval $1 | tee "$LOGFILE"

  echo "Running unreplicated writes. RAMCloud with no backups."

  ((CLISIZE = 25))
  echo "Running benchmark for WF: $WF and BatchSize: $SIZE. Logfile: $LOGFILE"
  echo "batchSize: $SIZE" >> $LOGFILE

  for ITER in $(seq $REP -1 1); do
        ./scripts/clusterperf.py writeThroughput -T infrc -r 0 --asyncReplication 1 -n $CLISIZE --masterArgs "-t 4000 --maxCores 7" >> $LOGFILE
  done
  sleep 10

  ./scripts/parseWriteThroughputByBatchSize.py $LOGFILE > ~/resultRAMCloud/writeThroughputByBatchSize-unreplicated.data
