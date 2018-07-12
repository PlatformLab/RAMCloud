#!/bin/bash
# Usage: [RAMCLOUD_DIR=rc_dir] compute_baseline.sh {homa+dpdk|basic+dpdk|infrc|tcp|...} {W1|W2|...}
#
# Compute the best-case echo RPC times for all message sizes in some workload.
# The first argument is the transport to use. The second argument is the name
# of the workload. In addition, the RAMCloud's top directory can be specified
# via environment variable RAMCLOUD_DIR; otherwise, it is assumed that the
# current working directory is RAMCloud's top directory.

transport=$1
workload=$2
echo "Computing best-case RPC times for workload $workload using $transport:"

# Run the experiment from the top directory of RAMCloud.
[[ -v RAMCLOUD_DIR ]] && cd $RAMCLOUD_DIR
cmd="scripts/clusterperf.py --superuser --dpdkPort 1 --replicas 0 --disjunct --transport $transport --servers 1 --clients 1 --messageSizeCDF benchmarks/homa/messageSizeCDFs/$workload.txt --timeout 10000 --verbose echo_basic"
echo $cmd
eval $cmd

# Extract the best-case RPC time for each message size from the client's log.
# Example log format:
#     echo3.min    3.3 us    send 3B message, receive 3B message minimum
result=benchmarks/homa/${transport}_${workload}_baseline.txt
grep "minimum" logs/latest/client1.*.log | awk '{print $5+0, $2, $3}' > $result
echo "Results are written to $result"
