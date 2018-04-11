#!/usr/bin/python

import re
from sys import argv

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

if len(argv) != 4:
    print "Usage: transport workload_type loadFactor"
    quit()

transport = argv[1]
workloadType = argv[2]
loadFactor = argv[3]

regex = re.compile("echo(\d+).min\s+(\d+\.\d+) (us|ms|s)")

# Example: echo50.min             4.7 us     send 50B message, receive 50B message minimum
baseline = "%s_baseline.txt" % workloadType
#print "Reading baseline performance file %s" % baseline
minRTT = {}
with open(baseline) as f:
    for line in f.readlines():
        matchObj = regex.match(line)
        if matchObj is None:
            continue

        size, time, unit = matchObj.groups()
        size = int(size)
        time = float(time)
        if unit == "ms":
            time *= 1000
        elif unit == "s":
            time *= 1000000
        minRTT[size] = time

# Example: Size   Samples       Min    Median       90%       99%     99.9%       Max
experiment = "%s_%s_%s_experiment.txt" % (transport, workloadType, loadFactor)
#print "Reading workload performance file %s" % experiment
numSamples = {}
medianRTT = {}
tailRTT = {}
totalSamples = 0.0
with open(experiment) as f:
    for line in f.readlines():
        data = [float(x) for x in line.strip(' ').split() if is_number(x)]
        if len(data) != 8:
            continue

        size = int(data[0])
        samples = int(data[1])
        medianRTT[size] = data[3]
        tailRTT[size] = data[5]

        numSamples[size] = samples
        totalSamples += samples

for size in sorted(numSamples.iterkeys()):
    print "%s %s %s %8d %5d %10.7f %8.2f %8.2f" % (transport, loadFactor,
            workloadType, size, numSamples[size],
            numSamples[size]/totalSamples, medianRTT[size]/minRTT[size],
            tailRTT[size]/minRTT[size])

