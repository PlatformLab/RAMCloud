/* Copyright (c) 2009, 2010 Stanford University
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
#include <boost/unordered_map.hpp>
#include <vector>

#include "BoostIntrusive.h"
#include "LargeBlockOfMemory.h"
#include "LogCleaner.h"
#include "LogTypes.h"
#include "Segment.h"
#include "SpinLock.h"
#include "BackupManager.h"

namespace RAMCloud {

/**
 * An exception that is thrown when the Log class is provided invalid
 * method arguments.
 */
struct LogException : public Exception {
    explicit LogException(const CodeLocation& where)
        : Exception(where) {}
    LogException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
    LogException(const CodeLocation& where, int errNo)
        : Exception(where, errNo) {}
    LogException(const CodeLocation& where, string msg, int errNo)
        : Exception(where, msg, errNo) {}
};

/**
 * Special exception for when the log runs out of memory entirely.
 */
struct LogOutOfMemoryException : public Exception {
    LogOutOfMemoryException(const CodeLocation& where, std::string msg)
            : Exception(where, msg) {}
};

// Use the same handle for Segments and the Log.
typedef SegmentEntryHandle LogEntryHandle;

typedef SegmentMultiAppendVector LogMultiAppendVector;
typedef SegmentEntryHandleVector LogEntryHandleVector;

typedef bool (*log_liveness_cb_t)(LogEntryHandle, void *);
typedef bool (*log_relocation_cb_t)(LogEntryHandle, LogEntryHandle, void *);
typedef uint32_t (*log_timestamp_cb_t)(LogEntryHandle);
typedef void (*log_scan_cb_t)(LogEntryHandle, void *);

/**
 * Each append operation on a Log writes a typed blob. Types must
 * be registered with the Log before appending. This class describes
 * these types.
 */
class LogTypeInfo {
  public:
    LogTypeInfo(LogEntryType type,
                bool explicitlyFreed,
                log_liveness_cb_t livenessCB,
                void *livenessArg,
                log_relocation_cb_t relocationCB,
                void *relocationArg,
                log_timestamp_cb_t timestampCB,
                log_scan_cb_t scanCB,
                void *scanArg)
        : type(type),
          explicitlyFreed(explicitlyFreed),
          livenessCB(livenessCB),
          livenessArg(livenessArg),
          relocationCB(relocationCB),
          relocationArg(relocationArg),
          timestampCB(timestampCB),
          scanCB(scanCB),
          scanArg(scanArg)
    {
    }

    /// Unique type identifier (see LogTypes.h). These are enums for
    /// type safety, but they are assumed by the Segment code to fit
    /// in 8 bits.
    const LogEntryType        type;

    /// If true, whenever an entry of this type is no longer needed the
    /// user of the Log will explicitly call #free on it. If false, it
    /// will be up to the LogCleaner to determine if entries of this type
    /// are free or not.
    const bool                explicitlyFreed;

    /// Callback used to determine if a specific entry of this type if
    /// still alive or not. Used primarily by the LogCleaner.
    const log_liveness_cb_t   livenessCB;

    /// Opaque cookie passed to the liveness callback.
    void                     *livenessArg;

    /// Callback used to notify the log's user when the entry is being
    /// moved by the LogCleaner. The user must either update any pointers
    /// to the entry's new location if they want to continue referencing
    /// it.
    const log_relocation_cb_t relocationCB;

    /// Opaque cookie passed to the relocation callback.
    void                     *relocationArg;

    /// Callback used to obtain the timestamp of the entry. Not all entries
    /// have timestamps so they're not kept internally in the log.
    const log_timestamp_cb_t  timestampCB;

    /// Callback used to iterate over entries in log order. Every time an
    /// entry is written to the log, this callback is eventually fired on
    /// it. If an entry is moved by the cleaner to a new segment, this callback
    /// will fire again. This is currently used to maintain the TabletProfilers
    /// asynchronously.
    const log_scan_cb_t       scanCB;

    /// Opaque cookie passed to the scan callback.
    void                     *scanArg;

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(LogTypeInfo);
};

/**
 * The Log class provides an immutable object store. Each object stored is
 * called an 'entry' and all entries are typed. Once a type has been
 * registered with the Log, one can then append any entries of that type
 * to it.
 *
 * When objects are no longer needed, they may either be freed explicitly or
 * discovered automatically. That is, on a per-type basis, the user can choose
 * to either explicitly free old entries or have the Log's special garabage
 * collector (the LogCleaner) determine which are free.
 *
 * A Log can be thought of as an infinitely long stretch of memory. New
 * entries are written to the current end of the log (the head). Since we don't
 * have infinite memory, the Log is divided into fixed-sized Segments. The
 * LogCleaner actively defragments old Segments, providing fresh ones to use at
 * the head.
 *
 * This class is essentially just a manager of Segments, threading them together
 * to form the logical Log.
 */
class Log {
  public:
    /// We have various options for cleaning. We can disable it entirely,
    /// run the cleaner in a separate thread, or run the cleaner on demand
    /// in the same thread as the Log (i.e. inlined). In the future, we may
    /// want to support choosing different cleaning policies entirely, in
    /// addition to concurrent/inlined operation.
    typedef enum {
        CLEANER_DISABLED   = 0,
        CONCURRENT_CLEANER = 1,
        INLINED_CLEANER    = 2
    } CleanerOption;

    Log(const Tub<uint64_t>& logId,
        uint64_t logCapacity,
        uint32_t segmentCapacity,
        uint32_t maximumBytesPerAppend,
        BackupManager *backup = NULL,
        CleanerOption cleanerOption = CONCURRENT_CLEANER);
    ~Log();
    void           allocateHead();
    LogEntryHandle append(LogEntryType type,
                          const void *buffer,
                          uint32_t length,
                          bool sync = true,
                          Tub<SegmentChecksum::ResultType> expectedChecksum =
                            Tub<SegmentChecksum::ResultType>());
    LogEntryHandleVector
                   multiAppend(LogMultiAppendVector& appends,
                               bool sync = true);
    void           free(LogEntryHandle entry);
    void           registerType(LogEntryType type,
                                bool explicitlyFreed,
                                log_liveness_cb_t livenessCB,
                                void *livenessArg,
                                log_relocation_cb_t relocationCB,
                                void *relocationArg,
                                log_timestamp_cb_t timestampCB,
                                log_scan_cb_t scanCB,
                                void* scanArg);
    const LogTypeInfo* getTypeInfo(LogEntryType type);
    void           sync();
    uint64_t       getSegmentId(const void *p);
    bool           isSegmentLive(uint64_t segmentId);
    uint64_t       getBytesAppended() const;
    uint64_t       getBytesFreed() const;
    uint64_t       getId() const;
    uint64_t       getCapacity() const;
    uint32_t       getSegmentCapacity() const;
    size_t         getNumberOfSegments() const;

    // These public methods are only to be externally used by the cleaner.
    void           getNewCleanableSegments(SegmentVector& out);
    void           cleaningInto(Segment* newSegment);
    void           cleaningComplete(SegmentVector &clean,
                                    std::vector<void*>& unusedSegmentMemory);
    void          *getSegmentMemoryForCleaning();
    uint64_t       allocateSegmentId();

    // This class is shared between the Log and its consituent Segments
    // to maintain various counters.
    class LogStats {
      public:
        LogStats();
        uint64_t getBytesAppended() const;
        uint64_t getAppends() const;
        uint64_t getBytesFreed() const;
        uint64_t getFrees() const;

      PRIVATE:
        uint64_t totalBytesAppended;
        uint64_t totalAppends;
        uint64_t totalBytesFreed;
        uint64_t totalFrees;

        friend class Log;
    };

    LogStats stats;

     // Total log capacity in bytes.
    const uint64_t logCapacity;

    // Capacity of each log segment in bytes.
    const uint32_t segmentCapacity;

    // The largest entry that can be appended via the #append method.
    const uint32_t maximumBytesPerAppend;

  PRIVATE:
    /**
     * Class used when destroying boost intrusive lists and destroying/freeing
     * all linked elements. See the intrusive list #clear_and_dispose method.
     */
    class SegmentDisposer {
      public:
        void
        operator()(Segment* segment)
        {
            delete segment;
        }
    };

    // typedefs for various lists and maps
    INTRUSIVE_LIST_TYPEDEF(Segment, listEntries) SegmentList;
    typedef std::vector<void*> FreeList;
    typedef boost::unordered_map<LogEntryType, LogTypeInfo*> LogTypeMap;
    typedef boost::unordered_map<uint64_t, Segment *> ActiveIdMap;
    typedef boost::unordered_map<const void *, Segment *> BaseAddressMap;

    void        dumpListStats();
    void        addSegmentMemory(void *p);
    void*       getFromFreeList(bool mayUseLastSegment);
    void        markActive(Segment *s);
    Segment*    getSegmentFromAddress(const void*);

    const Tub<uint64_t>& logId;

    /// A very large memory allocation that backs all segments.
    LargeBlockOfMemory<> segmentMemory;

    uint64_t       nextSegmentId;

    /*
     * The following members track all Segments in the system. At any point
     * when #listInterlock is not held, every Segment is in one and only
     * one of the below states. In addition, any free Segment backing
     * memory is on the freeList.
     */

    /// Current head of the log.
    Segment *head;

    /// List of free #segmentCapacity blocks of memory that aren't associated
    /// with a Segment object. These can be used to create new Segments by
    /// either the Log or the LogCleaner.
    FreeList freeList;

    /// List of Segments the log has just written to and closed, but which
    /// the cleaner is not yet aware of. The cleaner obtains a list of
    /// newly closed head Segments when updating its internal structures in
    /// preparation for a cleaning pass.
    SegmentList cleanableNewList;

    /// List of closed Segments that are part of the Log. This includes all
    /// Segments the LogCleaner is aware of (Segments the Log has written,
    /// as well as Segments the LogCleaner has generated during cleaning).
    /// Every Segment on this list was previously on the #cleanableNewList.
    /// Segments transition to this list when the LogCleaner learns about
    /// them via the #getNewCleanableSegments() method.
    SegmentList cleanableList;

    /// List of new Segments that the cleaner is currently cleaning to (placing
    /// live entries in). This is needed in case deletions of those entries
    /// occur before cleaning has finished (the Log code must be able to look
    /// up the Segment* from the pointer into the Segment to update the
    /// statistics). When #cleaningComplete() is called, this list is drained
    /// into the #cleanablePendingDigestList.
    SegmentList cleaningIntoList;

    /// List of new Segments generated by the cleaner that need to be added
    /// to the Log. Once a LogDigest containing these Segment's identifiers
    /// has been persisted, these are transferred to the cleanableNewList.
    SegmentList cleanablePendingDigestList;

    /// List of Segments with no live data (any live data has been written
    /// to Segments and added to the #cleanablePendingDigestList). Once this
    //  list and the #cleanablePendingDigestList have been emptied and applied
    //  to a LogDigest, these Segments are moved to the
    //  #freePendingReferenceList.
    SegmentList freePendingDigestAndReferenceList;

    /// List of Segments that contain no live data, but which may have
    /// outstanding references (e.g. in Buffers deep within the various
    /// Transports). Once it has been determined that no data is referenced
    /// in a Segment of this list it can be destroyed and the backing memory
    /// returned to the freeList.
    SegmentList freePendingReferenceList;

    /// Segment Id -> Segment * lookup within the active Segments (any Segment
    /// that exists in the system, including the log head). This is used
    /// during cleaning to check if a given SegmentId is currently valid in
    /// the system (e.g. does the Segment a Tombstone refers to still exist?).
    ActiveIdMap activeIdMap;

    /// Segment base address -> Segment * lookup within the active Segments (any
    /// Segment that exists in the system, including the log head). This is used
    /// during object deletion to obtain a reference to the associated Segment
    /// so that utilisation statistics can be updated.

    // XXX- We can do away with this at least two different ways. The first is
    //      to not update stats as we go, but have the LogCleaner do so by
    //      checking if each object is live. Alternatively, we can just ensure
    //      that Segment objects are allocated contiguously and compute the
    //      offset. If we do that, we can then just add space to the backing
    //      memory for a Segment and do placement new on that.
    BaseAddressMap activeBaseAddressMap;

    /// Per-LogEntryType callbacks (e.g. for relocation).
    LogTypeMap logTypeMap;

    /// Used to serialise access between the Log code and the LogCleaner
    /// (which only interacts with the Log vi Log methods). The interlock
    /// protects Segments are they are added to and removed from the
    /// various lists and maps that represent their current state.
    SpinLock listLock;

    /// Given to Segments to make them durable
    BackupManager *backup;

    /// If true, never run the cleaner. Only ever used for testing.
    CleanerOption  cleanerOption;

    /// Cleaner. Must come after backup so that it can create its own
    /// BackupManager from the Log's.
    LogCleaner     cleaner;

    DISALLOW_COPY_AND_ASSIGN(Log);
};

/**
 * The LogDigest is a special entry that is written to the front of every new
 * head Segment. It simply contains a list of all Segment IDs that are part
 * of the Log as of that head's creation. This is used during recovery to
 * discover all needed Segments and determine when data loss has occurred.
 * That is, once the latest head Segment is found, the recovery process need
 * only find copies of all Segments referenced by the head's LogDigest. If it
 * finds them all (and they pass checksums), it knows it has the complete Log.
 */
class LogDigest {
  public:
    typedef uint64_t SegmentId;
    /**
     * Create a LogDigest that will contain ``segmentCount''
     * SegmentIDs and serialise it to the given buffer. This
     * is the method to call when creating a new LogDigest,
     * i.e. when addSegment() will be called.
     *
     * \param[in] segmentCount
     *      The number of SegmentIDs that are to be stored in
     *      this LogDigest.
     * \param[in] base
     *      Base address of a buffer in which to serialise this
     *      LogDigest.
     * \param[in] length
     *      Length of the buffer pointed to by ``base'' in bytes.
     */
    LogDigest(size_t segmentCount, void* base, size_t length)
        : ldd(static_cast<LogDigestData*>(base)),
          currentSegment(0)
    {
        assert(length >= getBytesFromCount(segmentCount));
        ldd->segmentCount = downCast<uint32_t>(segmentCount);
        for (size_t i = 0; i < segmentCount; i++)
            ldd->segmentIds[i] = static_cast<SegmentId>(
                                            Segment::INVALID_SEGMENT_ID);
    }

    /**
     * Create a LogDigest object from a previous one that was
     * serialised in the given buffer. This is the method to
     * call when accessing a previously-constructed and
     * serialised LogDigest. 
     *
     * \param[in] base
     *      Base address of a buffer that contains a serialised
     *      LogDigest. 
     * \param[in] length
     *      Length of the buffer pointed to by ``base'' in bytes.
     */
    LogDigest(const void* base, size_t length)
        : ldd(static_cast<LogDigestData*>(const_cast<void*>(base))),
          currentSegment(downCast<uint32_t>(ldd->segmentCount))
    {
    }

    /**
     * Add a SegmentID to this LogDigest.
     */
    void
    addSegment(SegmentId id)
    {
        assert(currentSegment < ldd->segmentCount);
        ldd->segmentIds[currentSegment++] = id;
    }

    /**
     * Get the number of SegmentIDs in this LogDigest.
     */
    int getSegmentCount() { return ldd->segmentCount; }

    /**
     * Get an array of SegmentIDs in this LogDigest. There
     * will be getSegmentCount() elements in the array.
     */
    const SegmentId* getSegmentIds() { return ldd->segmentIds; }

    /**
     * Return the number of bytes needed to store a LogDigest
     * that contains ``segmentCount'' Segment IDs.
     */
    static size_t
    getBytesFromCount(size_t segmentCount)
    {
        return sizeof(LogDigestData) + (segmentCount * sizeof(SegmentId));
    }

    /**
     * Return a raw pointer to the memory passed in to the constructor.
     */
    const void* getRawPointer() { return static_cast<void*>(ldd); }

    /**
     * Return the number of bytes this LogDigest uses.
     */
    size_t getBytes() { return getBytesFromCount(ldd->segmentCount); }

  PRIVATE:
    struct LogDigestData {
        uint32_t segmentCount;
        SegmentId segmentIds[0];
    } __attribute__((__packed__));

    LogDigestData* ldd;
    uint32_t       currentSegment;
};

} // namespace

#endif // !RAMCLOUD_LOG_H
