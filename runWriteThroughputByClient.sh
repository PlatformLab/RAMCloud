 #!/bin/bash

#Repeat writeThroughput benchmark to reduce variance.
((REP = 15))
#((REP = 5))

#Sizes of the indexes as a power of 10
# SIZES="3 5 10 20 30 50"
 SIZES="50"
# SIZES=$(seq 50 -1 1)
CLISIZES=$(seq 6 5 31)

WITNESSNUM=$(seq 3 -1 0)
for WF in $WITNESSNUM; do
  cp src/Minimal_w$WF.h src/Minimal.h
  make clean
  make -j12 DEBUG=no

  RF=$WF
  if [[ $WF -eq 0 ]]; then
    RF=3
  fi

  for SIZE in $SIZES; do
    echo "# RAMCloud benchmark for batch size: $SIZE" > ~/resultRAMCloud/writeThroughputByClients-witness$WF-batch$SIZE.data
    for CLISIZE in $CLISIZES; do
      echo "batchSize: $SIZE clisize: $CLISIZE RF: $RF WF: $WF outputFile: ~/resultRAMCloud/writeThroughputByClients-witness$WF-batch$SIZE.data"

      for ITER in $(seq $REP -1 1); do
        ./scripts/clusterperf.py writeThroughput -T infrc --asyncReplication 1 -r $RF  --servers 0 --masters 2 --backups 3 --witnesses 3 -n $CLISIZE --masterArgs "-t 4000 --syncBatchSize $SIZE --maxCores 7" >> ~/resultRAMCloud/writeThroughputByClients-witness$WF-batch$SIZE.data
      done
      sleep 4
    done

    # Take median and only save the median.
    ./scripts/parseWriteThroughputByClient.py ~/resultRAMCloud/writeThroughputByClients-witness$WF-batch$SIZE.data > tempdata && mv tempdata ~/resultRAMCloud/writeThroughputByClients-witness$WF-batch$SIZE.data

  done
done


for SIZE in $SIZES; do
  mv ~/resultRAMCloud/writeThroughputByClients-witness0-batch$SIZE.data ~/resultRAMCloud/writeThroughputByClients-async-batch$SIZE.data
  sleep 4
done



#####################################
# RUN synchronous writes
#####################################
cp src/Minimal_w0.h src/Minimal.h
make clean
make -j12 DEBUG=no

echo "Running unreplicated writes. RAMCloud with 0 witnesses."

echo "# RAMCloud benchmark with no backups" > ~/resultRAMCloud/writeThroughputByClients-unreplicated.data
for CLISIZE in $CLISIZES; do
  echo "batchSize: 0 clisize: $CLISIZE outputFile: ~/resultRAMCloud/writeThroughputByClients-unreplicated.data"

  for ITER in $(seq $REP -1 1); do
    ./scripts/clusterperf.py writeThroughput -T infrc --asyncReplication 1 -r 0 -n $CLISIZE --masterArgs "-t 4000 --maxCores 7" >> ~/resultRAMCloud/writeThroughputByClients-unreplicated.data
  done
  sleep 4
done

# Take median and only save the median.
./scripts/parseWriteThroughputByClient.py ~/resultRAMCloud/writeThroughputByClients-unreplicated.data > tempdata && mv tempdata ~/resultRAMCloud/writeThroughputByClients-unreplicated.data

echo "Running synchronous writes. RAMCloud with 0 witnesses."

echo "# RAMCloud benchmark with original backups" > ~/resultRAMCloud/writeThroughputByClients-sync.data
for CLISIZE in $CLISIZES; do
  echo "batchSize: 0 clisize: $CLISIZE outputFile: ~/resultRAMCloud/writeThroughputByClients-sync.data"

  for ITER in $(seq $REP -1 1); do
    ./scripts/clusterperf.py writeThroughput -T infrc --asyncReplication 0 -n $CLISIZE --masterArgs "-t 4000 --maxCores 7" >> ~/resultRAMCloud/writeThroughputByClients-sync.data
  done
  sleep 4
done

# Take median and only save the median.
./scripts/parseWriteThroughputByClient.py ~/resultRAMCloud/writeThroughputByClients-sync.data > tempdata && mv tempdata ~/resultRAMCloud/writeThroughputByClients-sync.data
