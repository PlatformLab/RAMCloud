#! /bin/bash

####
# To use this file simply just invoke it and it will run all tests.
#
# *  If you wanted to run a subset of the clusterperf tests,
#    edit the CLUSTERPERF_TESTS variable
# *  If you wanted to run a subset of the system configurations (i.e. w/ NanoLog, snappy, tracing, etc),
#    edit the runTest invocations at the end of the file
####

CLUSTERPERF_TESTS="readThroughput writeThroughput writeDistRandom readDistRandom readDist"

# Allows you to specify a suffix in the results folder for easy identification
LOG_SUFFIX=""
if [ -n "$1" ]; then
  LOG_SUFFIX="_${1}"
fi

# Where results will be placed
LOG_DIR="$(pwd)/results/$(date +%Y%m%d%H%M%S)${LOG_SUFFIX}"

# Where server logs will be placed
SERVER_LOG_DIR="/tmp/$(whoami)/"

# Param 1 is log directory (i.e. where to place results)
# Param 2 is LogLevel (i.e. "DEBUG")
# param 3 is NanoLog (i.e. "yes" or "no")
# param 4 is Spdlog (i.e. "yes" or "no")
# Param 5 is Replace Traces in NanoLog with regular logger ("yes"/"no")
function runTest() {
  LOG_DIR="$1"
  LOG_LEVELS="$2"
  NANOLOGS="$3"
  SPDLOGS="$4"
  TRACE_REPLACES="$5"

  if [ "" = "$LOG_DIR" ] || [ "" = "$LOG_LEVELS" ] || [ "" = "$NANOLOGS" ] || [ "" = "SPDLOGS" ] || [ "" = "TRACE_REPLACES" ]; then
    echo "INVALID ARGUMENTS to runTest"
    exit 1
  fi

  VERBOSE_LOG_DIR="${LOG_DIR}/details"
  mkdir -p $LOG_DIR $VERBOSE_LOG_DIR
  ((ITTERATIONS=5))
  ((COUNT=2000000))
  ((TIMEOUT=600))
  grep -P "^[^#].*(BENCHMARK_LOG|DISPATCH_LOG)" -a1 -n src/*.* src/*.* > "${LOG_DIR}/logs.txt"

  for SPDLOG in $SPDLOGS;
  do
    for NANOLOG in $NANOLOGS;
    do
      if [ "$SPDLOG" == "yes" ] && [ "$NANOLOG" == "yes" ]; then
          echo "Skipping SPDLOG=yes NANOLOG=yes"
          echo ""
          continue
      fi

      for TRACE_REPLACE in $TRACE_REPLACES
      do

        echo "Building NANOLOG=${NANOLOG} SPDLOG=${SPDLOG} TRACE_REPLACE=${TRACE_REPLACE}"
        make clean-all > /dev/null && make DEBUG=NO NANOLOG=${NANOLOG} SPDLOG=${SPDLOG} TRACE_REPLACE=${TRACE_REPLACE} -j17 > /dev/null && clear
        for LOG_LEVEL in "$LOG_LEVELS";
        do
          LOG_NAME="LL_${LOG_LEVEL}_NL_${NANOLOG}_SPDLOG_${SPDLOG}_TRACE_REPLACE=${TRACE_REPLACE}"

          DETAILED_LOG_DIR="${LOG_DIR}/details/${LOG_NAME}"
          mkdir -p $DETAILED_LOG_DIR

          VERBOSE_LOG_FILE="${DETAILED_LOG_DIR}/details.txt"
          touch $VERBOSE_LOG_FILE


          for ((i=1; i <= ITTERATIONS; ++i))
          do
            for TEST in $CLUSTERPERF_TESTS;
            do
              # Log file keeps track of statistics for iteration of tests
              RUN_LOG_FILE="${DETAILED_LOG_DIR}/run${i}.txt"
              touch $RUN_LOG_FILE

              CMD="mkdir -p ${SERVER_LOG_DIR}; rm -f ${SERVER_LOG_DIR}/* /tmp/compressedLog > /dev/null 2>&1"
              rcdo "${CMD}"
              scripts/clusterperf.py -l ${LOG_LEVEL} --serverLogDir=${SERVER_LOG_DIR} --rcdf --count=${COUNT} --timeout=${TIMEOUT} -v ${TEST} | tee -a $VERBOSE_LOG_FILE $RUN_LOG_FILE

              # Spaces to separate tests
              echo " " >> $VERBOSE_LOG_FILE
              echo " " >> $VERBOSE_LOG_FILE

              echo " " >> $RUN_LOG_FILE
              echo " " >> $RUN_LOG_FILE

              # Get log sizes
              rcdo "hostname && ls -lah ${SERVER_LOG_DIR}" | tee -a $VERBOSE_LOG_FILE $RUN_LOG_FILE

              # Get a sample of their logs
              if [ "$NANOLOG" == "yes" ]; then
                CMD="$(pwd)/obj.nanolog_benchmark/decompressor ${SERVER_LOG_DIR}/*.compressed | head -n 100000 | tail -n 1000 > ${DETAILED_LOG_DIR}/\$(hostname).nanolog.txt"
                rcdo "hostname && ${CMD}"
              elif [ "$SPDLOG" == "yes" ]; then
                CMD="head -n 100000 ${SERVER_LOG_DIR}/*.spdlog | tail -n 1000 > ${DETAILED_LOG_DIR}/\$(hostname).spdlog.txt"
                rcdo "hostname && ${CMD}"
              else
                CMD="head -n 100000 ${SERVER_LOG_DIR}/*.log | tail -n 1000 > ${DETAILED_LOG_DIR}/\$(hostname).log.txt"
                rcdo "hostname && ${CMD}"
              fi

              cp -R $(pwd)/logs/latest/*.log ${DETAILED_LOG_DIR}
              sleep 5
            done


            LOG_FILE="${LOG_DIR}/${LOG_NAME}_run${i}.txt"
            RUN_LOG_FILE="${DETAILED_LOG_DIR}/run${i}.txt"
            grep -P "^ |#" ${RUN_LOG_FILE} > ${LOG_FILE}
          done
        done
      done
    done
  done
}

function cleanup() {
  read -p "Would you like to remove the /tmp/ files on the rcXX machines (logs may live there)? (y/n)" -n 1 -r
  echo    # (optional) move to a new line
  if [[ $REPLY =~ ^[Yy]$ ]]
  then
      CMD="rm -f ${SERVER_LOG_DIR}/* /tmp/compressedLog > /dev/null 2>&1"
      rcdo "${CMD}"
  fi
}

# Key is  LogDir    LogLevel  NanoLog    Spdlog  Tracing
runTest "$LOG_DIR"  "DEBUG"    "no"      "yes"    "yes"
runTest "$LOG_DIR"  "DEBUG"    "yes"     "no"     "yes"
runTest "$LOG_DIR"  "DEBUG"    "no"      "no"     "yes"
runTest "$LOG_DIR"  "DEBUG"    "no"      "no"     "no"
runTest "$LOG_DIR"  "NOTICE"   "no"      "no"     "no"


cleanup