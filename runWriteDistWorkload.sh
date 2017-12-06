 #!/bin/bash

# Do not repeat latency benchmark.
((REP = 1))

#Sizes of the indexes as a power of 10
 SIZES="20 50"
# SIZES=$(seq 50 -1 1)

WITNESSNUM=$(seq 3 -1 0)
for WF in $WITNESSNUM; do
  cp src/Minimal_w$WF.h src/Minimal.h
  make clean
  make -j16 DEBUG=no

  RF=$WF
  if [[ $WF -eq 0 ]]; then
    RF=3
  fi

  for SIZE in $SIZES; do
      echo "batchSize: $SIZE outputFile: ~/resultRAMCloud/writeDistWorkloadA-witness$RF-batch$SIZE.rcdf"

      for ITER in $(seq $REP -1 1); do
#          ./scripts/clusterperf.py writeDistWorkload -T infrc --asyncReplication 1 -r $RF --seconds 20 --servers 0 --masters 2 --backups 3 --witnesses 3 --workload "YCSB-A" --rcdf --masterArgs "-t 4000 --syncBatchSize $SIZE" > ~/resultRAMCloud/writeDistWorkloadA-witness$WF-batch$SIZE.rcdf
#          ./scripts/clusterperf.py writeDistWorkload -T infrc --asyncReplication 1 -r $RF --seconds 20 --servers 0 --masters 2 --backups 3 --witnesses 3 --workload "YCSB-A" --masterArgs "-t 4000 --syncBatchSize $SIZE" > ~/resultRAMCloud/writeDistWorkloadA-witness$WF-batch$SIZE.cdf
          ./scripts/clusterperf.py writeDistWorkload -T infrc --asyncReplication 1 -r $RF --seconds 20 --servers 0 --masters 2 --backups 3 --witnesses 3 --workload "YCSB-B" --rcdf --masterArgs "-t 4000 --syncBatchSize $SIZE" > ~/resultRAMCloud/writeDistWorkloadB-witness$WF-batch$SIZE.rcdf
          ./scripts/clusterperf.py writeDistWorkload -T infrc --asyncReplication 1 -r $RF --seconds 20 --servers 0 --masters 2 --backups 3 --witnesses 3 --workload "YCSB-B" --masterArgs "-t 4000 --syncBatchSize $SIZE" > ~/resultRAMCloud/writeDistWorkloadB-witness$WF-batch$SIZE.cdf
      done
      sleep 4
  done
done

for SIZE in $SIZES; do
#    mv ~/resultRAMCloud/writeDistWorkloadA-witness0-batch$SIZE.rcdf ~/resultRAMCloud/writeDistWorkloadA-async-batch$SIZE.rcdf
    sleep 1
#    mv ~/resultRAMCloud/writeDistWorkloadA-witness0-batch$SIZE.cdf ~/resultRAMCloud/writeDistWorkloadA-async-batch$SIZE.cdf
    sleep 1
    mv ~/resultRAMCloud/writeDistWorkloadB-witness0-batch$SIZE.rcdf ~/resultRAMCloud/writeDistWorkloadB-async-batch$SIZE.rcdf
    sleep 1
    mv ~/resultRAMCloud/writeDistWorkloadB-witness0-batch$SIZE.cdf ~/resultRAMCloud/writeDistWorkloadB-async-batch$SIZE.cdf
    sleep 1
done

#####################################
# RUN unreplicated and synchronous writes
#####################################
  cp src/Minimal_w0.h src/Minimal.h
  make clean
  make -j12 DEBUG=no

  echo "Running unreplicated writes. RAMCloud with 0 witnesses, 0 backups."

  RF=0

#  ./scripts/clusterperf.py writeDistWorkload -T infrc --asyncReplication 1 -r $RF --seconds 20 --servers 0 --masters 2 --backups 3 --witnesses 3 --workload "YCSB-A" --rcdf --masterArgs "-t 4000" > ~/resultRAMCloud/writeDistWorkloadA-unreplicated.rcdf
#  ./scripts/clusterperf.py writeDistWorkload -T infrc --asyncReplication 1 -r $RF --seconds 20 --servers 0 --masters 2 --backups 3 --witnesses 3 --workload "YCSB-A" --masterArgs "-t 4000" > ~/resultRAMCloud/writeDistWorkloadA-unreplicated.cdf
  ./scripts/clusterperf.py writeDistWorkload -T infrc --asyncReplication 1 -r $RF --seconds 20 --servers 0 --masters 2 --backups 3 --witnesses 3 --workload "YCSB-B" --rcdf --masterArgs "-t 4000" > ~/resultRAMCloud/writeDistWorkloadB-unreplicated.rcdf
  ./scripts/clusterperf.py writeDistWorkload -T infrc --asyncReplication 1 -r $RF --seconds 20 --servers 0 --masters 2 --backups 3 --witnesses 3 --workload "YCSB-B" --masterArgs "-t 4000" > ~/resultRAMCloud/writeDistWorkloadB-unreplicated.cdf

  echo "Running synchronous writes. RAMCloud with 0 witnesses, 0 backups."

  RF=3

#  ./scripts/clusterperf.py writeDistWorkload -T infrc --asyncReplication 0 -r $RF --seconds 20 --servers 0 --masters 2 --backups 3 --witnesses 3 --workload "YCSB-A" --rcdf --masterArgs "-t 4000" > ~/resultRAMCloud/writeDistWorkloadA-sync.rcdf
#  ./scripts/clusterperf.py writeDistWorkload -T infrc --asyncReplication 0 -r $RF --seconds 20 --servers 0 --masters 2 --backups 3 --witnesses 3 --workload "YCSB-A" --masterArgs "-t 4000" > ~/resultRAMCloud/writeDistWorkloadA-sync.cdf
  ./scripts/clusterperf.py writeDistWorkload -T infrc --asyncReplication 0 -r $RF --seconds 20 --servers 0 --masters 2 --backups 3 --witnesses 3 --workload "YCSB-B" --rcdf --masterArgs "-t 4000" > ~/resultRAMCloud/writeDistWorkloadB-sync.rcdf
  ./scripts/clusterperf.py writeDistWorkload -T infrc --asyncReplication 0 -r $RF --seconds 20 --servers 0 --masters 2 --backups 3 --witnesses 3 --workload "YCSB-B" --masterArgs "-t 4000" > ~/resultRAMCloud/writeDistWorkloadB-sync.cdf

