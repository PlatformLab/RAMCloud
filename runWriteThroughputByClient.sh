 #!/bin/bash

#Repeat writeThroughput benchmark to reduce variance.
((REP = 15))

#Sizes of the indexes as a power of 10
# SIZES="3 5 10 20 30 50"
 SIZES="20 30 40 50"
# SIZES=$(seq 50 -1 1)
CLISIZES=$(seq 6 5 31)

WITNESSNUM=$(seq 2 -1 0)
for WF in $WITNESSNUM; do
  cp src/Minimal_w$WF.h src/Minimal.h
  make clean
  make -j16 DEBUG=no

  for SIZE in $SIZES; do
    echo "# RAMCloud benchmark for batch size: $SIZE" > ~/resultRAMCloud/writeThroughputByClients-witness$WF-batch$SIZE.data
    for CLISIZE in $CLISIZES; do
      echo "batchSize: $SIZE clisize: $CLISIZE outputFile: ~/resultRAMCloud/writeThroughputByClients-witness$WF-batch$SIZE.data"

      for ITER in $(seq $REP -1 1); do
        ./scripts/clusterperf.py writeThroughput -T infrc --asyncReplication 1 -n $CLISIZE --masterArgs "-t 4000 --syncBatchSize $SIZE --maxCores 7" >> ~/resultRAMCloud/writeThroughputByClients-witness$WF-batch$SIZE.data
      done
      sleep 4
    done

    # Take median and only save the median.
    ./scripts/parseWriteThroughputByClient.py ~/resultRAMCloud/writeThroughputByClients-witness$WF-batch$SIZE.data > tempdata && mv tempdata ~/resultRAMCloud/writeThroughputByClients-witness$WF-batch$SIZE.data

  done
done


for SIZE in $SIZES; do
  mv ~/resultRAMCloud/writeThroughputByClients-witness0-batch$SIZE.data ~/resultRAMCloud/writeThroughputByClients-cgarc-batch$SIZE.data
  sleep 4
done



#####################################
# RUN synchronous writes
#####################################
cp src/Minimal_w0.h src/Minimal.h
make clean
make -j16 DEBUG=no

echo "Running synchronous writes. RAMCloud with 0 witnesses."

echo "# RAMCloud benchmark for batch size: $SIZE" > ~/resultRAMCloud/writeThroughputByClients-sync.data
for CLISIZE in $CLISIZES; do
  echo "batchSize: $SIZE clisize: $CLISIZE outputFile: ~/resultRAMCloud/writeThroughputByClients-sync.data"

  for ITER in $(seq $REP -1 1); do
    ./scripts/clusterperf.py writeThroughput -T infrc --asyncReplication 0 -n $CLISIZE --masterArgs "-t 4000 --syncBatchSize $SIZE --maxCores 7" >> ~/resultRAMCloud/writeThroughputByClients-sync.data
  done
  sleep 4
done

# Take median and only save the median.
./scripts/parseWriteThroughputByClient.py ~/resultRAMCloud/writeThroughputByClients-sync.data > tempdata && mv tempdata ~/resultRAMCloud/writeThroughputByClients-sync.data
