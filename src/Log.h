/* Copyright (c) 2009-2012 Stanford University
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

#ifndef RAMCLOUD_LOG_H
#define RAMCLOUD_LOG_H

#include <stdint.h>
#include <unordered_map>
#include <vector>

#include "BoostIntrusive.h"
#include "LogCleaner.h"
#include "LogEntryTypes.h"
#include "Segment.h"
#include "SegmentManager.h"
#include "LogSegment.h"
#include "SpinLock.h"
#include "ReplicaManager.h"
#include "HashTable.h"

namespace RAMCloud {

/**
 * An exception that is thrown when the Log class is provided invalid
 * method arguments.
 */
struct LogException : public Exception {
    LogException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
};

/**
 * The log provides a replicated store for immutable and relocatable data in
 * a master server's memory. Data is stored by appending typed "entries" to the
 * log. Entries are simply <type, length> tuples and associated opaque data
 * blobs. Once written, they may not be later modified. However, they may be
 * freed and the space later reclaimed by a special garbage collection mechanism
 * called the "cleaner".
 *
 * The cleaner requires that entries be relocatable to deal with fragmentation.
 * That is, it may decide to copy an entry to another location in memory and
 * tell the module that appended it to update any references and stop using the
 * old location. A set of callbacks are invoked by the cleaner to test if
 * entries are still alive to and notify the user of the log when an entry has
 * been moved to another log location. See the Log::EntryHandlers interface for
 * more details.
 *
 * This particular class provides a simple, thin interface for users of logs.
 * Much of the internals, most of which have to deal with replication and
 * cleaning, are handled by a suite of related classes such as Segment,
 * SegmentManager, LogCleaner, ReplicaManager, and BackupFailureMonitor.
 */
class Log {
  public:
    /**
     * This class specifies an interface that must be implemented for handling
     * various callbacks on entries appended to the log. An instance of a class
     * implementing this interface is provided to the log constructor.
     */
    class EntryHandlers {
      public:
        virtual ~EntryHandlers() { }

        /**
         * This method extracts a uint32_t timestamp from the given entry.
         * If the entry does not support a timestamp, 0 should be returned.
         */
        virtual uint32_t getTimestamp(LogEntryType type, Buffer& buffer) = 0;

        /**
         * This method returns true if the given entry is still being used,
         * in which case the cleaner will eventually relocate it and invoke
         * another callback to indicate the new location. If the entry is
         * no longer being used and may be garbage collected, this method
         * should return false.
         *
         * After returning false, the entry may disappear at any future time.
         */
        virtual bool checkLiveness(LogEntryType type, Buffer& buffer) = 0;

        /**
         * This method is called after an entry has been copied to a new
         * location. If the caller wants to retain the data, it should make
         * note of the new location (via the newReference). If it does not
         * need the data anymore, it should return false.
         *
         * After returning false, the entry may disappear at any future time.
         */
        virtual bool relocate(LogEntryType type,
                              Buffer& oldBuffer,
                              HashTable::Reference newReference) = 0;
    };

    /**
     * Position is a (Segment Id, Segment Offset) tuple that represents a
     * position in the log. For example, it can be considered the logical time
     * at which something was appended to the Log. It can be used for things like
     * computing table partitions and obtaining a master's current log position.
     */
    class Position {
      public:
        /**
         * Default constructor that creates a zeroed position. This refers to
         * the very beginning of a log.
         */
        Position()
            : pos(0, 0)
        {
        }

        /**
         * Construct a position given a segment identifier and offset within
         * the segment.
         */
        Position(uint64_t segmentId, uint64_t segmentOffset)
            : pos(segmentId, downCast<uint32_t>(segmentOffset))
        {
        }

        bool operator==(const Position& other) const {return pos == other.pos;}
        bool operator!=(const Position& other) const {return pos != other.pos;}
        bool operator< (const Position& other) const {return pos <  other.pos;}
        bool operator<=(const Position& other) const {return pos <= other.pos;}
        bool operator> (const Position& other) const {return pos >  other.pos;}
        bool operator>=(const Position& other) const {return pos >= other.pos;}

        /**
         * Return the segment identifier component of this position object.
         */
        uint64_t getSegmentId() const { return pos.first; }

        /**
         * Return the offset component of this position object.
         */
        uint32_t getSegmentOffset() const { return pos.second; }

      PRIVATE:
        std::pair<uint64_t, uint32_t> pos;
    };

    Log(Context& context,
        EntryHandlers& entryHandlers,
        SegmentManager& segmentManager,
        ReplicaManager& replicaManager,
        bool disableCleaner = false);
    ~Log();

    bool append(LogEntryType type,
                Buffer& buffer,
                uint32_t offset,
                uint32_t length,
                bool sync,
                HashTable::Reference& outReference);
    bool append(LogEntryType type,
                Buffer& buffer,
                bool sync,
                HashTable::Reference& outReference);
    bool append(LogEntryType type,
                const void* data,
                uint32_t length,
                bool sync);
    void free(HashTable::Reference reference);
    LogEntryType getEntry(HashTable::Reference reference,
                          Buffer& outBuffer);
    void sync();
    Position getHeadPosition();
    uint64_t getSegmentId(HashTable::Reference reference);
    void allocateHeadIfStillOn(Tub<uint64_t> segmentId);
    bool containsSegment(uint64_t segmentId);

  PRIVATE:
    INTRUSIVE_LIST_TYPEDEF(LogSegment, listEntries) SegmentList;
    typedef std::lock_guard<SpinLock> Lock;

    HashTable::Reference buildReference(uint32_t slot, uint32_t offset);
    uint32_t referenceToSlot(HashTable::Reference reference);
    uint32_t referenceToOffset(HashTable::Reference reference);

    /// Shared RAMCloud information.
    Context& context;

    /// Various handlers for entries appended to this log. Used to obtain
    /// timestamps, check liveness, and notify of entry relocation during
    /// cleaning.
    EntryHandlers& entryHandlers;

    /// The SegmentManager allocates and keeps track of our segments. It
    /// also mediates mutation of the log between this class and the
    /// LogCleaner.
    SegmentManager& segmentManager;

    /// Class responsible for handling the durability of segments. Segment
    /// objects don't themselves have any concept of replication, but the
    /// Log and SegmentManager classes ensure that the data is replicated
    /// consistently nonetheless.
    ReplicaManager& replicaManager;

    /// If cleaning is enabled, this contains an instance of the garbage
    /// collector that will remove dead entries from the log.
    Tub<LogCleaner> cleaner;

    /// Current head of the log. Whatever this points to is owned by
    /// SegmentManager, which is responsible for its eventual deallocation.
    /// This pointer should never be NULL.
    LogSegment* head;

    /// Lock taken around log append operations. This is currently only used
    /// to delay appends to the log head while migration is underway.
    SpinLock appendLock;

    friend class LogIterator;

    DISALLOW_COPY_AND_ASSIGN(Log);
};

} // namespace

#endif // !RAMCLOUD_LOG_H
