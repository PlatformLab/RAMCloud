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

/* Is the core load estimator running? */
bool ramCloudLoadEstimatorRunning = false;

/**
  * Update threadClassCoreMap when a new core is added.  Takes in the coreId
  * of the new core.  Assigns the first core to the dispatch class and
  * all others to the base class.
  */
void CorePolicyRamCloud::addCore(int coreId) {
    CoreList* dispatchEntry = threadClassCoreMap[dispatchClass];
    if (dispatchEntry->numFilled == 0) {
        dispatchEntry->map[0] = coreId;
        dispatchEntry->numFilled++;
        dispatchHyperTwin = getHyperTwin(sched_getcpu());
        LOG(NOTICE, "Dispatch thread on core %d with coreId %d",
            sched_getcpu(), coreId);
        return;
    } else if (sched_getcpu() == dispatchHyperTwin) {
        LOG(NOTICE, "Dispatch thread hypertwin on core %d with coreId %d",
            sched_getcpu(), coreId);
        CoreList* dispatchHTEntry = threadClassCoreMap[dispatchHTClass];
        dispatchHTEntry->map[0] = coreId;
        dispatchHTEntry->numFilled++;
        Arachne::createThread(dispatchHTClass, &CoreBlocker::blockCore,
            ramCloudCoreBlocker, sched_getcpu());
        return;
    }
    LOG(NOTICE, "New core %d with coreId %d",
        sched_getcpu(), coreId);
    CoreList* entry = threadClassCoreMap[defaultClass];
    entry->map[entry->numFilled] = coreId;
    entry->numFilled++;
    if (!ramCloudLoadEstimatorRunning &&
      !Arachne::disableLoadEstimation) {
        ramCloudLoadEstimatorRunning = true;
        runLoadEstimator();
    }
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
