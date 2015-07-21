/* Copyright (c) 2013-2015 Stanford University
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

#ifndef RAMCLOUD_COORDINATORUPDATEMANAGER_H
#define RAMCLOUD_COORDINATORUPDATEMANAGER_H

#include <mutex>
#include <string>
#include <deque>
#include "ExternalStorage.h"

namespace RAMCloud {

/**
 * This class keeps track of complex operations on the coordinator that
 * require recovery actions if the coordinator crashes in the middle of
 * executing them. This class is used to assign a unique sequence number to
 * each operation ("update"). The sequence number is stored in external
 * storage along with information about the update before the coordinator
 * executes the operation. Once the operation has completed, the coordinator
 * notifies this class, which (eventually) stores information on external
 * storage about which updates have completed.  When the coordinator crashes
 * and a new coordinator starts up, it retrieves the information about
 * completed updates. Then it scans all of the update records in external
 * storage to see which ones are not known to be completed; those operations
 * it must complete. The role of this class is to manage sequence numbers
 * and keep track of which ones are known to be complete.
 */
class CoordinatorUpdateManager {
  PUBLIC:
    explicit CoordinatorUpdateManager(ExternalStorage* storage);
    ~CoordinatorUpdateManager();

    uint64_t init();
    uint64_t nextSequenceNumber();
    void recoveryFinished();
    void updateFinished(uint64_t sequenceNumber);

  PRIVATE:
    /// Monitor-style lock: acquired by all externally visible methods.
    std::mutex mutex;
    typedef std::unique_lock<std::mutex> Lock;

    /// The following object is used to read and write configuration
    /// information from the durable storage system outside RAMCloud,
    /// such as ZooKeeper.
    ExternalStorage* storage;

    /// This object keeps track of the sequence numbers that are still
    /// "in play". Each element corresponds to a particular sequence number;
    /// its value is true if that sequence number has finished (updateFinished
    /// has been invoked for it) and false otherwise. The contents of the
    /// deque represent a contiguous range of sequence numbers, in increasing
    /// order (smallest sequence number is at the front).
    std::deque<bool> activeUpdates;

    /// Sequence number corresponding to the first entry in activeUpdates.
    /// This is the sequence number of the oldest operation (smallest sequence
    /// number) that has not yet completed.
    uint64_t smallestUnfinished;

    /// The highest sequence number returned by nextSequenceNumber.
    uint64_t lastAssigned;

    /// This is a copy of the data stored in a CoordinatorUpdateInfo protocol
    /// buffer on external storage. All ones means that we are just getting
    /// started and haven't yet read the existing values from external storage.
    uint64_t externalLastFinished;
    uint64_t externalFirstAvailable;

    /// This flag is set when the recoveryFinished method is invoked; if
    /// this flag is false, it's not safe to update externalLastFinished
    /// (see note in the sync method).
    bool recoveryComplete;

    void reset();
    void sync(Lock& lock);

    DISALLOW_COPY_AND_ASSIGN(CoordinatorUpdateManager);
};

} // namespace RAMCloud

#endif // RAMCLOUD_COORDINATORUPDATEMANAGER_H

