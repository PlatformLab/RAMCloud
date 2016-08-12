/* Copyright (c) 2010-2013 Stanford University
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

#ifndef RAMCLOUD_INMEMORYSTORAGE_H
#define RAMCLOUD_INMEMORYSTORAGE_H

#include <deque>

#include "Common.h"
#include "BackupStorage.h"
#include "Memory.h"
#include "MultiFileStorage.h"

namespace RAMCloud {

/**
 * A BackupStorage backend which stores replica data in memory. Replicas will
 * not survive across crashes or restarts of the backups. Useful for unit
 * testing and exposing bottlenecks.
 */
class InMemoryStorage : public BackupStorage {
  public:
    /**
     * Represents a chunk of memory from storage which holds a single replica.
     * Frames manage the details of moving replica data to and from storage.
     * InMemoryStorage keeps exactly one frame for each space on storage where
     * it holds (or could hold) a replica.
     * Frames get reused for different replicas making a frame something of a
     * state machine.
     *
     * Backups open() frames, append() data, and then close() them. When the
     * replica is no longer needed free() releases the frame for reuse by
     * another replica, for which, the same cycle will be repeated.
     * See InMemoryStorage::open() to allocate and open a Frame.
     */
    class Frame : public BackupStorage::Frame {
      PUBLIC:
        typedef std::unique_lock<std::mutex> Lock;

        Frame(InMemoryStorage* storage, size_t frameIndex);

        bool wasAppendedToByCurrentProcess();

        void loadMetadata();
        const void* getMetadata();

        void startLoading();
        bool isLoaded();
        bool currentlyOpen() {return isOpen;}
        void* load();
        void unload();

        void append(Buffer& source,
                    size_t sourceOffset,
                    size_t length,
                    size_t destinationOffset,
                    const void* metadata,
                    size_t metadataLength);
        void close();
        void reopen(size_t length);
        void free();

        void performTask();

      PRIVATE:
        void open();

        bool isSynced() const;

        /// Storage where this frame resides.
        InMemoryStorage* storage;

        /// Index of the frame in #storage.frames. Used to mark the frame free.
        const size_t frameIndex;

        /**
         * Buffer where replica data is stored.
         * Reset on free() to release memory when the frame isn't in use.
         */
        std::unique_ptr<char[]> buffer;

        /**
         * Tracks whether a replica has been opened (either initially or
         * since the time of the last free). False if #isClosed.
         */
        bool isOpen;

        /**
         * Tracks whether a replica has been closed (either initially or
         * since the time of the last free). False if #isOpen.
         */
        bool isClosed;

        /**
         * Tracks whether append has been called on this frame during the
         * life of this process. This includes across free()/open() cycles.
         * This is used by the backup replica garbage collector to determine
         * whether it might have missed a BackupFree rpc from a master, in
         * which case it has to query the master for the replica status.
         * This is "appended to" rather than "opened by" because of benchmark();
         * benchmark "opens" replicas and loads them but doesn't do any.
         * Masters open and append data in a single rpc, so this is a fine
         * proxy.
         */
        bool appendedToByCurrentProcess;

        /**
         * True if the replica data has been requested. Only used to reject
         * appends after load requests in InMemoryStorage.
         */
        bool loadRequested;

        /**
         * Metadata given on the most recent call to append. Starts zeroed
         * on construction.
         */
        std::unique_ptr<char[]> metadata;

        // ONLY for open(); please try not to touch other
        // details of frames in InMemoryStorage (or elsewhere).
        friend class InMemoryStorage;
        DISALLOW_COPY_AND_ASSIGN(Frame);
    };

    InMemoryStorage(size_t segmentSize,
                    size_t frameCount,
                    size_t writeRateLimit);

    FrameRef open(bool sync, ServerId masterId, uint64_t segmentId);
    size_t getMetadataSize();
    std::vector<FrameRef> loadAllMetadata();
    void resetSuperblock(ServerId serverId,
                         const string& clusterName,
                         uint32_t frameSkipMask = 0);
    Superblock loadSuperblock();
    void quiesce();
    void fry();

  PRIVATE:
    /// Maximum size of metadata for each frame.
    enum { METADATA_SIZE = MultiFileStorage::METADATA_SIZE };

    /// Protects concurrent operations on storage and all of its frames.
    std::mutex mutex;
    typedef std::unique_lock<std::mutex> Lock;

    /**
     * Frame for each a chunk of memory in the storage which can hold a replica.
     * Frames get reused for different replicas making a frame something of a
     * state machine, but are all created and destroyed along with the
     * storage instance.
     */
    std::deque<Frame> frames;

    /// The number of replicas this storage can store simultaneously.
    const size_t frameCount;

    /// Type of the freeMap.  A bitmap.
    typedef boost::dynamic_bitset<> FreeMap;
    /// Keeps a bit set for each frame in frames indicating if it is free.
    FreeMap freeMap;

    /**
     * Track the last used segment frame so they can be used in FIFO.
     * This gives recovery dump tools a much better chance at recovering
     * data since old data is destroyed from storage first rather than new.
     */
    FreeMap::size_type lastAllocatedFrame;

    DISALLOW_COPY_AND_ASSIGN(InMemoryStorage);
};

} // namespace RAMCloud

#endif
