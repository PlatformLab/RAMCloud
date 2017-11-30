/* Copyright (c) 2009-2016 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "ShortMacros.h"
#include "Cycles.h"
#include "Arachne/Arachne.h"
#include "Arachne/CorePolicy.h"
#include "CorePolicyRamCloud.h"
#include "FileLogger.h"

using namespace RAMCloud;

/**
  * Arachne will attempt to increase the number of cores if the idle core
  * fraction (computed as idlePercentage * numSharedCores) is less than this
  * number.
  */
const double maxIdleCoreFraction = 0.1;

/**
  * Arachne will attempt to increase the number of cores if the load factor
  * increases beyond this threshold.
  */
const double loadFactorThreshold = 1.0;

/**
  * Save the core fraction at which we ramped up based on load factor, so we
  * can decide whether to ramp down.  Allocated in bootstrapLoadEstimator.
  */
double *RAMCloudutilizationThresholds;

/**
  * The difference in load, expressed as a fraction of a core, between a
  * ramp-down threshold and the corresponding ramp-up threshold (i.e., we
  * wait to ramp down until the load gets a bit below the point at 
  * which we ramped up).
  */
const double idleCoreFractionHysteresis = 0.2;

/**
  * Do not ramp down if the percentage of occupied threadContext slots is
  * above this threshold.
  */
const double SLOT_OCCUPANCY_THRESHOLD = 0.5;

/**
  * The period in ns over which we measure before deciding to reduce the number
  * of cores we use.
  */
const uint64_t MEASUREMENT_PERIOD = 50 * 1000 * 1000;

void coreLoadEstimator(CorePolicyRamCloud* corePolicy);
void blockForever();

/**
  * Bootstrap the core load estimator thread.  This must be done separately
  * from the CorePolicy constructor because the constructor must run before
  * Arachne adds any cores, but the core load estimator must run after
  * Arachne has cores.
  */
void CorePolicyRamCloud::bootstrapLoadEstimator() {
    RAMCloudutilizationThresholds = new double[Arachne::maxNumCores];
    Arachne::createThread(baseClass, coreLoadEstimator, this);
}


/**
  * Update threadCoreMap when a new core is added.  Takes in the coreId
  * of the new core.  Assigns the first core to the dispatch class and
  * all others to the base class.
  */
void CorePolicyRamCloud::addCore(int coreId) {
    ThreadCoreMapEntry* dispatchEntry = threadCoreMap[dispatchClass];
    if (dispatchEntry->numFilled == 0) {
        dispatchEntry->map[0] = coreId;
        dispatchEntry->numFilled++;
        numNecessaryCores++;
        dispatchHyperTwin = getHyperTwin(sched_getcpu());
        LOG(NOTICE, "Dispatch thread on core %d with coreId %d",
            sched_getcpu(), coreId);
        return;
    } else if (sched_getcpu() == dispatchHyperTwin) {
        LOG(NOTICE, "Dispatch thread hypertwin added on core %d with coreId %d",
            sched_getcpu(), coreId);
        ThreadCoreMapEntry* dispatchHTEntry = threadCoreMap[dispatchHTClass];
        dispatchHTEntry->map[0] = coreId;
        dispatchHTEntry->numFilled++;
        numNecessaryCores++;
        Arachne::createThread(dispatchHTClass, blockForever);
        return;
    }
    LOG(NOTICE, "New core %d with coreId %d",
        sched_getcpu(), coreId);
    ThreadCoreMapEntry* entry = threadCoreMap[baseClass];
    entry->map[entry->numFilled] = coreId;
    entry->numFilled++;
}

/* 
 * Return the hypertwin of coreId.  Return -1 if there is none.
 */
int CorePolicyRamCloud::getHyperTwin(int coreId) {
    // This file contains the siblings of core coreId.
    std::string siblingFilePath = "/sys/devices/system/cpu/cpu"
     + std::to_string(coreId) + "/topology/thread_siblings_list";
    FILE* siblingFile = fopen(siblingFilePath.c_str(), "r");
    int cpu1;
    int cpu2;
    // If there is a hypertwin, the file is of the form "int1,int2", where
    // int1 < int2 and one is coreId and the other is the hypertwin's ID.
    // Return -1 if no hypertwin found.
    if (fscanf(siblingFile, "%d,%d", &cpu1, &cpu2) < 2)
        return -1;
    if (coreId == cpu1)
        return cpu2;
    else
        return cpu1;
}

void blockForever() {
    while (1) {
        sleep(1000);
    }
}

/**
  * Periodically wake up and observe the current load in Arachne to determine
  * whether it is necessary to increase or reduce the number of cores used by
  * Arachne.  Runs as the top-level method in an Arachne thread.
  */
void coreLoadEstimator(CorePolicyRamCloud* corePolicy) {
    Arachne::PerfStats previousStats;
    Arachne::PerfStats::collectStats(&previousStats);

    for (Arachne::PerfStats currentStats; ; previousStats = currentStats) {
        Arachne::sleep(MEASUREMENT_PERIOD);
        Arachne::PerfStats::collectStats(&currentStats);

        // Take a snapshot of currently active cores before performing
        // estimation to avoid races between estimation and the fulfillment of
        // a previous core request.
        uint32_t curActiveCores = Arachne::numActiveCores;

        // Necessary cores should contribute nothing to statistics relevant to
        // core estimation during the period over which they are exclusive.
        int numSharedCores = curActiveCores - corePolicy->numNecessaryCores;

        // Evaluate idle time precentage multiplied by number of cores to
        // determine whether we need to decrease the number of cores.
        uint64_t idleCycles =
            currentStats.idleCycles - previousStats.idleCycles;
        uint64_t totalCycles =
            currentStats.totalCycles - previousStats.totalCycles;
        uint64_t utilizedCycles = totalCycles - idleCycles;
        uint64_t totalMeasurementCycles =
            Cycles::fromNanoseconds(MEASUREMENT_PERIOD);
        double totalUtilizedCores =
            static_cast<double>(utilizedCycles) /
            static_cast<double>(totalMeasurementCycles);

        // Estimate load to determine whether we need to increment the number
        // of cores.
        uint64_t weightedLoadedCycles =
            currentStats.weightedLoadedCycles -
            previousStats.weightedLoadedCycles;
        double averageLoadFactor =
            static_cast<double>(weightedLoadedCycles) /
            static_cast<double>(totalCycles);
        if (curActiveCores < Arachne::maxNumCores &&
                averageLoadFactor > loadFactorThreshold) {
            // Record our current totalUtilizedCores, so we will only ramp down
            // if utilization would drop below this level.
            RAMCloudutilizationThresholds[numSharedCores] = totalUtilizedCores;
            Arachne::incrementCoreCount();
            continue;
        }

        // We should not ramp down if we have high occupancy of slots.
        double averageNumSlotsUsed = static_cast<double>(
                currentStats.numThreadsCreated -
                currentStats.numThreadsFinished) /
                numSharedCores / Arachne::maxThreadsPerCore;

        // Scale down if the idle time after scale down is greater than the
        // time at which we scaled up, plus a hysteresis threshold.
        if (totalUtilizedCores < RAMCloudutilizationThresholds[numSharedCores - 1]
                - idleCoreFractionHysteresis &&
                averageNumSlotsUsed < SLOT_OCCUPANCY_THRESHOLD) {
            Arachne::decrementCoreCount();
        }
    }
}