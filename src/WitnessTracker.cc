/* Copyright (c) 2017 Stanford University
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

#include "WitnessTracker.h"
#include "RamCloud.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Default constructor
 */
WitnessTracker::WitnessTracker()
    : deletable()
    , mutex()
{
}

/**
 * Default destructor
 */
WitnessTracker::~WitnessTracker()
{
}

/**
 * Notify an entry at hashIndex in Witness table can now be deleted since master
 * replicated the outcome of the main RPC to backups.
 *
 * \param witnessServerId
 *      ServerId of witness server.
 * \param targetMasterId
 *      ServerId of the master that is main RPC's target.
 * \param hashIndex
 *      HashIndex of witness table that can be reset.
 */
void
WitnessTracker::free(uint64_t witnessServerId,
                     uint64_t targetMasterId,
                     int16_t hashIndex)
{
    Lock lock(mutex);
    WitnessTableId key = {witnessServerId, targetMasterId};
    deletable[key].push(hashIndex);
}

/**
 * Provides the hashIndices that can be reset in Witness table.
 *
 * \param witnessServerId
 *      ServerId of witness server.
 * \param targetMasterId
 *      ServerId of the master that is main RPC's target.
 * \param[out] deletableIndices
 *      HashIndices of witness table that can be reset. Up to three indices at
 *      a time (the max number that can be piggybacked by WitnessRecordRpc.)
 *      Invoker must provide the array of size 3.
 *      If we have less than three deletable indices, rest of array will be
 *      populated with -1.
 */
void
WitnessTracker::getDeletable(uint64_t witnessServerId,
                             uint64_t targetMasterId,
                             int16_t deletableIndices[])
{
    Lock lock(mutex);
    WitnessTableId key = {witnessServerId, targetMasterId};
    for (int i = 0; i < 3; ++i) {
        if (deletable[key].empty()) {
            deletableIndices[i] = -1;
        } else {
            deletableIndices[i] = deletable[key].top();
            deletable[key].pop();
        }
    }
}

} // namespace RAMCloud
