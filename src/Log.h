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

#include "LogCleaner.h"
#include "LogTypes.h"
#include "Segment.h"
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

// Use the same handle for Segments and the Log.
typedef SegmentEntryHandle LogEntryHandle;

/**
 * LogTime is a (Segment #, Segment Offset) tuple that represents the logical
 * time at which something was appended to the Log. It is currently only used
 * for computing table partitions.
 */
typedef std::pair<uint64_t, uint64_t> LogTime;

typedef void (*log_eviction_cb_t)(LogEntryHandle, const LogTime, void *);

class LogTypeCallback {
  public:
    LogTypeCallback(LogEntryType type,
                     log_eviction_cb_t evictionCB, void *evictionArg)
        : type(type),
          evictionCB(evictionCB),
          evictionArg(evictionArg)
    {
    }

    const LogEntryType        type;
    const log_eviction_cb_t   evictionCB;
    void                     *evictionArg;

  private:
    DISALLOW_COPY_AND_ASSIGN(LogTypeCallback);
};

class Log {
  public:
    Log(uint64_t logId, uint64_t logCapacity, uint64_t segmentCapacity,
            BackupManager *backup = NULL);
    ~Log();
    Segment*       allocateHead();
    LogEntryHandle append(LogEntryType type,
                          const void *buffer,
                          uint64_t length,
                          uint64_t *lengthInLog = NULL,
                          LogTime *logTime = NULL,
                          bool sync = true);
    void           free(LogEntryHandle entry);
    void           registerType(LogEntryType type,
                                log_eviction_cb_t evictionCB,
                                void *evictionArg);
    void           sync();
    uint64_t       getSegmentId(const void *p);
    bool           isSegmentLive(uint64_t segmentId) const;
    uint64_t       getMaximumAppendableBytes() const;
    uint64_t       getBytesAppended() const;
    uint64_t       getId() const;
    uint64_t       getCapacity() const;

    // This class is shared between the Log and its consituent Segments
    // to maintain various counters.
    class LogStats {
      public:
        LogStats();
        uint64_t getBytesAppended() const;
        uint64_t getAppends() const;
        uint64_t getFrees() const;

      private:
        uint64_t totalBytesAppended;
        uint64_t totalAppends;
        uint64_t totalFrees;

        // permit direct twiddling of counters by authorised classes
        friend class Log;
        friend class LogTest;
        friend class Segment;
    };

    LogStats    stats;

  private:
    void        addSegmentMemory(void *p);
    void        addToActiveMaps(Segment *s);
    void        eraseFromActiveMaps(Segment *s);
    void        addToFreeList(void *p);
    void       *getFromFreeList();
    uint64_t    allocateSegmentId();
    const void *getSegmentBaseAddress(const void *p);

    uint64_t       logId;
    uint64_t       logCapacity;
    uint64_t       segmentCapacity;
    vector<void *> segmentFreeList;
    uint64_t       nextSegmentId;
    uint64_t       maximumAppendableBytes;
    bool           useCleaner;      // allow tests to disable cleaner
    LogCleaner     cleaner;

    /// Current head of the log
    Segment *head;

    typedef boost::unordered_map<LogEntryType, LogTypeCallback *> CallbackMap;
    /// Per-LogEntryType callbacks (e.g. for eviction)
    CallbackMap callbackMap;

    typedef boost::unordered_map<uint64_t, Segment *> ActiveIdMap;
    /// Segment Id -> Segment * lookup within the active list
    ActiveIdMap activeIdMap;

    typedef boost::unordered_map<const void *, Segment *> BaseAddressMap;
    /// Segment base address -> Segment * lookup within the active list
    BaseAddressMap activeBaseAddressMap;

    /// Given to Segments to make them durable
    BackupManager *backup;

    friend class LogTest;
    friend class LogCleaner;

    DISALLOW_COPY_AND_ASSIGN(Log);
};

class LogDigest {
  public:
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
    LogDigest(uint32_t segmentCount, void* base, uint32_t length)
        : ldd(static_cast<LogDigestData*>(base)),
          currentSegment(0)
    {
        assert(length >= getBytesFromCount(segmentCount));
        ldd->segmentCount = segmentCount;
        for (uint32_t i = 0; i < segmentCount; i++)
            ldd->segmentIds[i] = Segment::INVALID_SEGMENT_ID;
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
    LogDigest(const void* base, uint32_t length)
        : ldd(static_cast<LogDigestData*>(const_cast<void*>(base))),
          currentSegment(ldd->segmentCount)
    {
    }

    /**
     * Add a SegmentID to this LogDigest.
     */
    void
    addSegment(uint64_t id)
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
    const uint64_t* getSegmentIds() { return ldd->segmentIds; }

    /**
     * Return the number of bytes needed to store a LogDigest
     * that contains ``segmentCount'' Segment IDs.
     */
    static uint32_t
    getBytesFromCount(uint32_t segmentCount)
    {
        return sizeof(LogDigestData) + segmentCount * sizeof(uint64_t);
    }

    /**
     * Return a raw pointer to the memory passed in to the constructor.
     */
    const void* getRawPointer() { return static_cast<void*>(ldd); }

    /**
     * Return the number of bytes this LogDigest uses.
     */
    uint32_t getBytes() { return getBytesFromCount(ldd->segmentCount); }

  private:
    struct LogDigestData {
        uint32_t segmentCount;
        uint64_t segmentIds[0];
    } __attribute__((__packed__));

    LogDigestData* ldd;
    uint32_t       currentSegment;

    friend class LogTest;
    friend class LogDigestTest;
};

} // namespace

#endif // !RAMCLOUD_LOG_H
