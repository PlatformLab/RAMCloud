#! /bin/bash


LOG_SUFFIX=""
if [ -n "$1" ]; then
  LOG_SUFFIX="_${1}"
fi

LOG_DIR="$(pwd)/results/$(date +%Y%m%d%H%M%S)_${LOG_SUFFIX}"
VERBOSE_LOG_DIR="${LOG_DIR}/details"
mkdir -p $LOG_DIR $VERBOSE_LOG_DIR

SERVER_LOG_DIR="/tmp/"

BENCHMARKS="yes no"
# BENCHMARK_WORKER_TRACINGS="yes no"
# BENCHMARK_DISPATCH_TRACINGS="yes no"

# Note: NANOLOG=yes and SPDLOG=yes are invalid configurations and will
# be skipped
NANOLOGS="no yes"
SPDLOGS="yes no"
LOG_LEVELS="DEBUG NOTICE"
CLUSTERPERF_TESTS="readThroughput writeThroughput writeDistRandom readDistRandom readDist"
((ITTERATIONS=3))
((COUNT=2000000))
((TIMEOUT=600))

grep -P "^[^#].*(BENCHMARK_LOG|DISPATCH_LOG)" -a1 -n src/*.* src/*.* > "${LOG_DIR}/logs.txt"

for SPDLOG in $SPDLOGS;
do
  for BENCHMARK in $BENCHMARKS
    BENCHMARK_DISPATCH_TRACING="$BENCHMARK"
    BENCHMARK_WORKER_TRACING="$BENCHMARK"
    # for BENCHMARK_WORKER_TRACING in $BENCHMARK_WORKER_TRACINGS;
    # do
    #   for BENCHMARK_DISPATCH_TRACING in $BENCHMARK_DISPATCH_TRACINGS;
    #   do
        for NANOLOG in $NANOLOGS;
        do
        if [ "$SPDLOG" == "yes" ] && [ "$NANOLOG" == "yes" ]; then
            echo "Skipping SPDLOG=yes NANOLOG=yes"
            echo ""
            continue
        fi

        echo "Building NANOLOG=${NANOLOG} SPDLOG=${SPDLOG} BENCHMARK_DISPATCH_TRACING=${BENCHMARK_DISPATCH_TRACING} BENCHMARK_WORKER_TRACING=${BENCHMARK_WORKER_TRACING}"
        make clean-all > /dev/null && make DEBUG=NO NANOLOG=${NANOLOG} SPDLOG=${SPDLOG} BENCHMARK_DISPATCH_TRACING=${BENCHMARK_DISPATCH_TRACING} BENCHMARK_WORKER_TRACING=${BENCHMARK_WORKER_TRACING} -j17 > /dev/null && clear
        for LOG_LEVEL in $LOG_LEVELS;
        do
          LOG_NAME="LL_${LOG_LEVEL}_NL_${NANOLOG}_SPDLOG_${SPDLOG}_DISPATCH_${BENCHMARK_DISPATCH_TRACING}_WORKER_${BENCHMARK_WORKER_TRACING}"

          DETAILED_LOG_DIR="${LOG_DIR}/details/${LOG_NAME}"
          mkdir -p $DETAILED_LOG_DIR

          VERBOSE_LOG_FILE="${DETAILED_LOG_DIR}/details.txt"
          touch $VERBOSE_LOG_FILE

          for TEST in $CLUSTERPERF_TESTS;
          do
            for ((i=1; i <= ITTERATIONS; ++i))
            do
              # Log file keeps track of statistics for iteration of tests
              RUN_LOG_FILE="${DETAILED_LOG_DIR}/run${i}.txt"
              touch $RUN_LOG_FILE

              CMD="rm -f /tmp/*"
              rcdo "${CMD}"
              scripts/clusterperf.py -l ${LOG_LEVEL} ${TEST} --serverLogDir=${SERVER_LOG_DIR} -v --rcdf --count=${COUNT} --timeout=${TIMEOUT} | tee -a $VERBOSE_LOG_FILE $RUN_LOG_FILE

              # Spaces to separate tests
              echo " " >> $VERBOSE_LOG_FILE
              echo " " >> $VERBOSE_LOG_FILE

              echo " " >> $RUN_LOG_FILE
              echo " " >> $RUN_LOG_FILE

              # Get log sizes
              CMD='ls -lah /tmp/logFile /tmp/*'
              rcdo "hostname && ${CMD}" | tee -a $VERBOSE_LOG_FILE $RUN_LOG_FILE

              # Get a sample of their logs
              if [ "$NANOLOG" == "yes" ]; then
                CMD="$(pwd)/obj.nanolog_benchmark/decompressor /tmp/*.compressed | head -n 100000 | tail -n 1000 > ${DETAILED_LOG_DIR}/${TEST}_\$(hostname).log.txt"
                rcdo "hostname && ${CMD}"
              elif [ "$SPDLOG" == "yes" ]; then
                CMD="head -n 100000 /tmp/*.spdlog | tail -n 1000 > $(pwd)/${DETAILED_LOG_DIR}/\$(hostname).log.txt"
                rcdo "hostname && ${CMD}"
              else
                CMD="head -n 100000 /tmp/*.log | tail -n 1000 > $(pwd)/${DETAILED_LOG_DIR}/\$(hostname).log.txt"
                rcdo "hostname && ${CMD}"
              fi

              cp -R $(pwd)/logs/latest/*.log ${DETAILED_LOG_DIR}
            done
          done

          for ((i=1; i <= ITTERATIONS; ++i))
          do
            LOG_FILE="${LOG_DIR}/${LOG_NAME}_run${i}.txt"
            RUN_LOG_FILE="${DETAILED_LOG_DIR}/run${i}.txt"
            grep -P "^ |#" ${RUN_LOG_FILE} > ${LOG_FILE}
          done
        done
      done
  #   done
  # done
  done
done

# Note: Benchmark logs are on NOTICE LEVEL
# make clean-all && make DEBUG=NO NANOLOG=yes BENCHMARK_TRACING=yes -j32 && scripts/clusterperf.py readDistRandom
# scripts/clusterperf.py readDistRandom -v --serverLogDir=/tmp/
# make clean-all && make DEBUG=NO NANOLOG=no BENCHMARK_TRACING=yes -j32 && clear && scripts/clusterperf.py readDistRandom -v --serverLogDir=/tmp/
