#!/usr/bin/python
# Usage: compute_slowdown.py baseline_data experiment_data
#
# Compute echo RPC slowdowns by normalizing the actual RPC times aginst the
# best-case RPC times. The two arguments specify the names of the two files
# that contain the best-case and actual RPC times, respectively.
#
# Note: This script is only designed to be invoked by `run_workload.sh` from
# the directory where the two files containing RPC times reside.

from sys import argv

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def main():
    baseline_data = argv[1]
    experiment_data = argv[2]
    experiment_name, workload, load_factor = experiment_data.split("_")[:3]

    # Read best-case RPC times of all message sizes from file
    rpc_time_min = {}
    with open(baseline_data) as f:
        for line in f.readlines():
            size, time, unit = line.strip().split(" ");
            size = int(size)
            time = float(time)
            if unit == "ms":
                time *= 1000
            elif unit == "s":
                time *= 1000000
            rpc_time_min[size] = time

    # Read median and 99%-tile tail RPC time of all message sizes from file
    # Example format:
    #    Size  Samples  Min  Average  Median  90%  99%  99.9%  Max
    num_samples = {}
    average = {}
    median = {}
    tail = {}
    total_samples = 0.0
    with open(experiment_data) as f:
        for line in f.readlines():
            data = line.strip(' ').split()
            if len(data) != 9:
                continue
            data = [float(x) for x in data]

            size, num_samples[size], _, average[size], median[size], _, tail[size] = data[:7]
            total_samples += num_samples[size]

    # Print out RPC slowdowns
    for size in sorted(num_samples.iterkeys()):
        print("%s %s %s %8d %5d %10.7f %8.2f %8.2f %8.2f" % (experiment_name,
                load_factor, workload, size, num_samples[size],
                num_samples[size] / total_samples,
                average[size] / rpc_time_min[size],
                median[size] / rpc_time_min[size],
                tail[size] / rpc_time_min[size]))

if __name__ == '__main__':
    main()
