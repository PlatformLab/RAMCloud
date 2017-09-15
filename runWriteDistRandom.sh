 #!/bin/bash

# Do not repeat latency benchmark.
((REP = 1))

#SYNC batch size.
 SIZES="1 3 5 10 20 50"
# SIZES=$(seq 50 -1 1)

((MEMSIZE = 4000))

WITNESSNUM=$(seq 2 -1 0)
for WF in $WITNESSNUM; do
  cp src/Minimal_w$WF.h src/Minimal.h
  make clean
  make -j16 DEBUG=no

  LOGFILE="$(date +%Y%m%d%H%M%S).data"
  eval $1 | tee "$LOGFILE"

  echo "Built RAMCloud with $WF witnesses."

  for SIZE in $SIZES; do
    echo "Running benchmark for WF: $WF and BatchSize: $SIZE. outputFile: ~/resultRAMCloud/writeDistRandom-witness$WF-batch$SIZE.rcdf"

    for ITER in $(seq $REP -1 1); do
        ./scripts/clusterperf.py writeDistRandom -T infrc --asyncReplication 1 --count 1000000 --servers 0 --masters 2 --backups 3 --witnesses 2 --rcdf --masterArgs "-t $MEMSIZE --syncBatchSize $SIZE --maxCores 7" > ~/resultRAMCloud/writeDistRandom-witness$WF-batch$SIZE.rcdf
#        ./scripts/clusterperf.py writeDistRandom -T infrc --asyncReplication 1 --count 1000000 --servers 0 --masters 2 --backups 3 --witnesses 2 --masterArgs "-t 4000 --syncBatchSize $SIZE --maxCores 7" > ~/resultRAMCloud/$OUTPUTPREFIX$SIZE.cdf
    done
    sleep 4
  done
  sleep 4
done

for SIZE in $SIZES; do
    mv ~/resultRAMCloud/writeDistRandom-witness0-batch$SIZE.rcdf ~/resultRAMCloud/writeDistRandom-cgarc-batch$SIZE.rcdf
    sleep 4
done

#####################################
# RUN synchronous writes
#####################################
  cp src/Minimal_w0.h src/Minimal.h
  make clean
  make -j16 DEBUG=no
  echo "Running synchronous writes. RAMCloud with 0 witnesses."

./scripts/clusterperf.py writeDistRandom -T infrc --asyncReplication 1 --count 1000000 --servers 0 --masters 2 --backups 3 --witnesses 2 --rcdf --masterArgs "-t $MEMSIZE --syncBatchSize $SIZE --maxCores 7" > ~/resultRAMCloud/writeDistRandom-sync.rcdf

#        ./scripts/clusterperf.py writeDistRandom -T infrc --asyncReplication 1 --count 1000000 --servers 0 --masters 2 --backups 3 --witnesses 2 --masterArgs "-t 4000 --syncBatchSize $SIZE --maxCores 7" > ~/resultRAMCloud/$OUTPUTPREFIX$SIZE.cdf
