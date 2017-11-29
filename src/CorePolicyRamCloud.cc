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

#include "CorePolicyRamCloud.h"
#include "Arachne/CorePolicy.h"
#include "Arachne/Arachne.h"

/**
  * Update threadCoreMap when a new core is added.  Takes in the coreId
  * of the new core.  Assigns the first core to the dispatch class and
  * all others to the base class.
**/
void CorePolicyRamCloud::addCore(int coreId) {
  threadCoreMapEntry* dispatchEntry = threadCoreMap[dispatchClass];
  if (dispatchEntry->numFilled == 0) {
    dispatchEntry->map[0] = coreId;
    dispatchEntry->numFilled++;
    Arachne::numExclusiveCores++;
    return;
  }
  threadCoreMapEntry* entry = threadCoreMap[baseClass];
  entry->map[entry->numFilled] = coreId;
  entry->numFilled++;
}
