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
#include "RamCloudCorePolicy.h"
#include "FileLogger.h"
#include "Util.h"

using namespace RAMCloud;

/* Is the core load estimator running? */
bool ramCloudLoadEstimatorRunning = false;

/*
 * Handles core assignment for RamCloud.  The first core scheduled
 * is always given to the dispatch thread.  When the dispatch thread's
 * hypertwin appears, it is assigned to the dispatchHTClass and blocks
 * forever to prevent work on the hypertwin from slowing down the dispatch
 * thread.  All other threads are assigned to the default class.
 *
 * By design, the cores occupied by the dispatch thread and its hypertwin
 * can never be lost or taken away, so they do not need to be reassigned.
 *
 * \param coreId
 *     The coreId of the new core.
 */
void RamCloudCorePolicy::addCore(int coreId) {
    CoreList* dispatchEntry = threadClassCoreMap[dispatchClass];
    // Assign the dispatch thread to the first core that comes up.
    if (dispatchEntry->numFilled == 0) {
        dispatchEntry->map[0] = coreId;
        dispatchEntry->numFilled++;
        dispatchHyperTwin = Util::getHyperTwin(sched_getcpu());
        LOG(NOTICE, "Dispatch thread on core %d with coreId %d",
            sched_getcpu(), coreId);
        return;
    // Assign the dispatch thread's hypertwin when it shows up.
    } else if (sched_getcpu() == dispatchHyperTwin) {
        LOG(NOTICE, "Dispatch thread hypertwin on core %d with coreId %d",
            sched_getcpu(), coreId);
        CoreList* dispatchHTEntry = threadClassCoreMap[dispatchHTClass];
        dispatchHTEntry->map[0] = coreId;
        dispatchHTEntry->numFilled++;
        Arachne::createThread(dispatchHTClass, &Arachne::CoreBlocker::blockCore,
            ramCloudCoreBlocker, sched_getcpu());
        return;
    }
    // Everything else gets assigned to the default class.
    LOG(NOTICE, "New core %d with coreId %d",
        sched_getcpu(), coreId);
    CoreList* entry = threadClassCoreMap[defaultClass];
    entry->map[entry->numFilled] = coreId;
    entry->numFilled++;
    // Boostrap the coreLoadEstimator as soon as possible.
    if (!ramCloudLoadEstimatorRunning &&
      !Arachne::disableLoadEstimation) {
        ramCloudLoadEstimatorRunning = true;
        Arachne::createThread(defaultClass,
            &RamCloudCorePolicy::coreLoadEstimator, this);
    }
}
