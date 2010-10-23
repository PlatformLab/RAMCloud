/* Copyright (c) 2009 Stanford University
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

#include "Log.h"

static const uint64_t cleanerHiWater = 20;
static const uint64_t cleanerLowWater = 10;

namespace RAMCloud {

/**
 * Simple iterator for running through a Segment's (LogEntry, blob) pairs.
 */
LogEntryIterator::LogEntryIterator(const Segment *s)
    : segment(s), next(0)
{
    assert(s != NULL);
    next = (const struct LogEntry *)s->getBase();
    assert(next != NULL);
    if (next->type != LOG_ENTRY_TYPE_SEGMENT_HEADER) {
        printf("next type is not seg hdr, but %llx\n", next->type);
    }
    assert(next->type == LOG_ENTRY_TYPE_SEGMENT_HEADER);
    assert(next->length == sizeof(struct SegmentHeader));
}

/**
 * Get the next (LogEntry, blob) pair from the Segment, returning immutable
 * pointers to the LogEntry structure and the object iself, as well as the
 * byte offset of the LogEntry structure in the Segment.
 *
 * \param[out]   le     Immutable LogEntry structure pointer
 * \param[out]   p      Immutable blob pointer
 * \param[out]   offset If non-NULL, store the offset of the LogEntry in the
 *                      Segment.
 * \return True or False
 * \retval True if there was another (LogEntry, blob) pair.
 * \retval False if the Segment has been iterated through.
 */
bool
LogEntryIterator::getNextAndOffset(const struct LogEntry **le,
                                   const void **p,
                                   uint64_t *offset)
{
    if (next == NULL)
        return false;

    if (le != NULL)
        *le = next;
    if (p != NULL)
        *p = reinterpret_cast<const uint8_t*>(next) + sizeof(*next);

    const struct LogEntry *nle =
            (struct LogEntry *)(reinterpret_cast<const uint8_t*>(next) +
            sizeof(*next) + next->length);
    if (next->length != 0 &&
            next->type != LOG_ENTRY_TYPE_SEGMENT_CHECKSUM &&
            segment->checkRange(nle, sizeof(*nle))) {
        assert(segment->checkRange(nle, sizeof(*nle) + nle->length));
        next = nle;
    } else {
        next = NULL;
    }
    if (next && offset)
        *offset = reinterpret_cast<uintptr_t>(next) -
                reinterpret_cast<uintptr_t>(segment->getBase());

    return true;
}

/**
 * Get the next (LogEntry, blob) pair from the Segment, returning immutable
 * pointers to the LogEntry structure and the object iself.
 *
 * \param[out]   le     Immutable LogEntry structure pointer
 * \param[out]   p      Immutable blob pointer
 * \return True or False
 * \retval True if there was another (LogEntry, blob) pair.
 * \retval False if the Segment has been iterated through.
 */
bool
LogEntryIterator::getNext(const struct LogEntry **le, const void **p)
{
    return getNextAndOffset(le, p, 0);
}

/**************************/
/**** Public Interface ****/
/**************************/

Log::Log(const uint64_t segsize,
         void *buf,
         const uint64_t len,
         BackupClient *backup_client)
    : numCallbacks(0),
      nextSegmentId(SEGMENT_INVALID_ID + 1),
      maxAppend(0),
      segmentSize(segsize),
      base(buf),
      segments(0),
      head(0),
      freeList(0),
      nsegments(len / segmentSize),
      nFreeList(0),
      bytesStored(0),
      cleaning(false),
      backup(backup_client)
{
    // at a minimum, we'll have a segment header, one object, and a segment
    // checksum
    uint64_t minMeta = (3 * sizeof(struct LogEntry)) +
            sizeof(struct SegmentHeader) + sizeof(struct SegmentChecksum);
    assert(minMeta <= len);
    maxAppend = segmentSize - minMeta;

    segments = static_cast<Segment**>(xmalloc(nsegments * sizeof(segments[0])));
    for (uint64_t i = 0; i < nsegments; i++) {
        void *base  = static_cast<uint8_t*>(buf) + (i * segmentSize);
        segments[i] = new Segment(base, segmentSize, backup);
        freeList   = segments[i]->link(freeList);
        nFreeList++;
    }

    assert(nsegments == nFreeList);
    assert(nFreeList > 0);
}

/**
 * Initialise the Log for use.
 *
 * XXX- Do we have a use for this, or should it move into the constructor?
 */
void
Log::init()
{
    head = freeList;
    freeList = freeList->unlink();
    nFreeList--;
    head->ready(allocateSegmentId());

    struct SegmentHeader sh;
    sh.id = 0;

    const void *r = appendAnyType(LOG_ENTRY_TYPE_SEGMENT_HEADER,
                                  &sh, sizeof(sh));
    assert(r != NULL);
}

/**
 * Given a Segment identifier, return whether or not it is active (i.e. it
 * contains valid data used by %RAMCloud).
 *
 * \param[in]   segmentId   The Segment identifier to check.
 * \return True or False 
 * \retval True if the provided Segment identifier is valid.
 * \retval False if the identifier is invalid.
 */
bool
Log::isSegmentLive(uint64_t segmentId) const
{
    if (segmentId == SEGMENT_INVALID_ID)
        return false;

    // TODO(rumble) inefficient
    for (uint64_t i = 0; i < nsegments; i++) {
        if (segments[i]->getId() == segmentId)
            return true;
    }

    return false;
}

/**
 * Given a pointer into the Log, return the Segment identifier of the Segment
 * it exists in, as well as the byte offset into that Segment. 
 *
 * \param[in]   p       Pointer into the Log.
 * \param[out]  id      Segment Identifier to return to caller.
 * \param[out]  offset  Segment offset to return to caller.
 */
void
Log::getSegmentIdOffset(const void *p, uint64_t *id, uint32_t *offset) const
{
    Segment *s = getSegment(p, 1);
    assert(s != NULL);
    assert(id != NULL);
    assert(offset != NULL);

    *id = s->getId();
    *offset = (uint32_t)((uintptr_t)p - (uintptr_t)s->getBase());
}

/**
 * Append data to the head of the log. The data is marked with the provided
 * type.
 *
 * \param[in]   type    Type of the data added to the log.
 * \param[in]   buf     Opaque data to be written to the log.
 * \param[in]   len     Byte length of the data to be written.
 * \return An immutable pointer to the data in the log, or NULL on failure.
 * \retval NULL if the specified type was invalid (i.e. an internally-used Log
 *         entry type) or insufficient space remains.
 * \retval A valid pointer into the Log if the append succeeded.
 */
const void *
Log::append(LogEntryType type, const void *buf, const uint64_t len)
{
    if (type == LOG_ENTRY_TYPE_SEGMENT_HEADER ||
        type == LOG_ENTRY_TYPE_SEGMENT_CHECKSUM)
        return NULL;

    return appendAnyType(type, buf, len);
}

/**
 * Free a previously-appended, typed blob in the Log.
 *
 * \param[in]   type    Type of the blob being freed.
 * \param[in]   buf     Log pointer to the blob being freed.
 * \param[in]   len     Length of the blob being freed.
 */
void
Log::free(LogEntryType type, const void *buf, const uint64_t len)
{
    struct LogEntry *le = (struct LogEntry *)((uintptr_t)buf -
                                                    sizeof(*le));
    Segment *s = getSegment(le, len + sizeof(*le));

    assert(le->type == static_cast<uint32_t>(type));
    assert(le->length == len);
    assert(type != LOG_ENTRY_TYPE_SEGMENT_HEADER &&
           type != LOG_ENTRY_TYPE_SEGMENT_CHECKSUM);
    assert(len <= bytesStored);

    // Note that we do not adjust the 'bytesStored' count here,
    // but do so only during cleaning.

    s->free(len + sizeof(*le));
    if (s->getUtilization() == 0) {
        // TODO(rumble) need to handle mutability,
        // inc. segment #, etc
        // TODO(rumble) good idea may be
        // to do this only when segment pulled from free list
        assert(!cleaning);

        assert(s->unlink() == NULL);
        freeList = s->link(freeList);
        assert(freeList->getUtilization() == 0);
        nFreeList++;
        if (s == head) {
            retireHead();
            assert(head == NULL);
        }
        s->reset();
    }
}

/**
 * Register a new blob type with the Log. This involves providing a callback
 * for use when Log entries are evicted.
 *
 * \param[in]   type        Type of the new Log entry being registered.
 * \param[in]   evict_cb    The eviction callback to use for this type.
 * \param[in]   cookie      An opaque cookie to be provided to the callback.
 */
void
Log::registerType(LogEntryType type,
                  LogEvictionCallback evict_cb,
                  void *cookie)
{
    assert(evict_cb != NULL);
    assert(numCallbacks < static_cast<int>(sizeof(callbacks)/
                sizeof(callbacks[0])));

    callbacks[numCallbacks].cb = evict_cb;
    callbacks[numCallbacks].type = type;
    callbacks[numCallbacks].cookie = cookie;
    numCallbacks++;
}

/**
 * Print various interesting log stats to stdout. This should be generalised
 * in the future. 
 */
void
Log::printStats()
{
    printf("::LOG STATS::\n");
    printf("  segment len:           %" PRIu64 " (%.3fMB)\n",
           segmentSize, static_cast<float>(segmentSize) / (1024*1024));
    printf("  num segments:          %" PRIu64 "\n",
           nsegments);
    printf("  cumulative storage:    %" PRIu64 " (%.3fMB)\n",
           (nsegments * segmentSize),
           static_cast<float>(nsegments * segmentSize) / (1024*1024));
    printf("  non-meta bytes stored: %" PRIu64 " (%.3fMB)\n",
           bytesStored, static_cast<float>(bytesStored) / (1024*1024));
    printf("  non-meta utilization:  %.3f%%\n",
           static_cast<float>(bytesStored) / (nsegments * segmentSize) *
           100.0);
    printf("  non-meta efficiency:   %.3f%%\n",
           static_cast<float>(bytesStored) / ((nsegments - nFreeList) *
           segmentSize) * 100.0);
}

/**
 * Return the maximum number of bytes that can be appended to the Log in one
 * sequential operation.
 *
 * \return An integer number of bytes.
 */
uint64_t
Log::getMaximumAppend()
{
    return maxAppend;
}

/*************************/
/**** Private Methods ****/
/*************************/

/**
 * Allocate a new Segment id that is unique in the history of this Log instance.
 *
 * \return An opaque Segment identifier.
 */
uint64_t
Log::allocateSegmentId()
{
    return (nextSegmentId++);
}

/**
 * Obtain the eviction callback that was registered for the given type.
 *
 * \param[in]   type    The type whose callback should be searched for.
 * \param[out]  cookie  Opaque cookie that was registered with the type.
 * \return The associated eviction callback pointer or NULL.
 * \retval NULL if the given entry type was not registered.
 * \retval A valid callback function pointer if the type was registered.
 */
LogEvictionCallback
Log::getEvictionCallback(LogEntryType type, void **cookie)
{
    for (int i = 0; i < numCallbacks; i++) {
        if (callbacks[i].type == type) {
            if (cookie != NULL)
                *cookie = callbacks[i].cookie;
            return callbacks[i].cb;
        }
    }

    return NULL;
}

/**
 * Given a pointer into the Log and a length bound on the entry it refers to,
 * return the Segment to which it was written. For %RAMCloud consistency, this
 * function presently may not fail.
 *
 * \param[in]   p   A pointer into the Log.
 * \param[in]   len The length of the entry 'p' corresponds to.
 * \return A pointer to the Segment corresponding to the provided parameters. 
 */
Segment *
Log::getSegment(const void *p, uint64_t len) const
{
    uintptr_t up  = (uintptr_t)p;
    uintptr_t ub  = (uintptr_t)base;
    uintptr_t max = (uintptr_t)base + (segmentSize * nsegments);

    assert(up >= ub && (up + len) <= max);
    uintptr_t segno = (up - ub) / segmentSize;
    assert(segno < nsegments);

    Segment *s = segments[segno];
    uintptr_t sb = (uintptr_t)s->getBase();
    assert(up >= sb && (up + len) <= (sb + segmentSize));

    return (s);
}

/**
 * Clean the Log. We do this by finding good candidate Segments to clean and
 * walk the entries in each one, calling the eviction callback on each entry.
 *
 * The callback will either do nothing, or re-write to the head of the log.
 * It's very important that this re-writing be guaranteed to succeed and that
 * we do not recurse into cleaning!
 *
 * Segments are cleaned until the number of Segments on the free list reaches
 * 'cleanerHiWater'.
 */
void
Log::clean()
{
    assert(head == NULL);

    // we may come back here due to a log append when our eviction callback
    // results in an object being written out once again. This is ok, but when
    // we're finished cleaning, we may toss an underutilized segment and
    // reallocate a new head in our caller, newHead(). it's probably
    // unimportant, but we may want to preserve enough state to detect and
    // avoid that case.
    if (cleaning)
        return;
    cleaning = true;

    for (uint64_t i = 0; i < nsegments; i++) {
        Segment *s = segments[i];

        // The eviction callbacks may allocate and write to a new head.
        // Ensure that we don't try to clean it.
        if (s == head)
            continue;

        if (nFreeList >= cleanerHiWater)
            break;

        uint64_t util = s->getUtilization();
        if (util != 0 && util < (3 * segmentSize / 4)) {
            // Note that since our segments are immutable (when not writing to
            // the head) we may certainly iterate over objects for which
            // log_free() has already been called. That's fine - it's up to the
            // callback to determine that its stale data and take no action.
            //
            // For this reason, we do _not_ want to do s->free() on that space,
            // since for already freed space, we'll double-count.

            LogEntryIterator lei(s);
            const struct LogEntry *le;
            const void *p;

            while (lei.getNext(&le, &p)) {
                void *cookie;
                LogEvictionCallback cb =
                    getEvictionCallback((LogEntryType)le->type, &cookie);

                if (le->type != LOG_ENTRY_TYPE_SEGMENT_HEADER &&
                    le->type != LOG_ENTRY_TYPE_SEGMENT_CHECKSUM) {
                    assert(le->length <= bytesStored);
                    bytesStored -= le->length;
                }

                if (cb != NULL)
                    cb((LogEntryType)le->type, p, le->length, cookie);
            }

            assert(s->unlink() == NULL);
            s->reset();
            freeList = s->link(freeList);
            nFreeList++;
        }
    }

    cleaning = false;
}

/**
 * Allocate a new head of the Log. If there is already a head, retire it first.
 * An empty Segment, if one if available, is chosen as the new head.
 *
 * \return True or False
 * \retval True if a new head was installed.
 * \retval False if the free list has run out and no new head could be found.
 */
bool
Log::newHead()
{
    if (head != NULL)
        retireHead();

    assert(head == NULL);

    if (nFreeList < cleanerLowWater) {
        clean();

        // while cleaning, we may have had to re-write live
        // objects to the log, which would have allocated
        // another head.
        //
        // it'd be nice if we didn't waste a mostly-unutilized
        // segment, but that's for another day

        if (head != NULL)
            retireHead();
    }

    assert(head == NULL);

    if (freeList == NULL) {
        assert(nFreeList == 0);
        return false;
    }

    head = freeList;
    freeList = head->unlink();
    nFreeList--;
    head->ready(allocateSegmentId());

    assert(head->getUtilization() == 0);

    struct SegmentHeader sh;
    sh.id = head->getId();

    const void *r = appendAnyType(LOG_ENTRY_TYPE_SEGMENT_HEADER,
                                  &sh, sizeof(sh));
    assert(r != NULL);

    return true;
}

/**
 * Checksum the Log's head segment in the Log and append the value to the Log.
 */
void
Log::checksumHead()
{
    assert(head != NULL);
    assert(head->getFreeTail() >=
           (sizeof(struct LogEntry) + sizeof(struct SegmentChecksum)));

    struct SegmentChecksum sc;
    sc.checksum = 0xbeefcafebeefcafeULL;
    appendAnyType(LOG_ENTRY_TYPE_SEGMENT_CHECKSUM, &sc, sizeof(sc));
}

/**
 * Retire the present head Segment. This involves applying the checksum footer
 * and finalising the Segment.
 *
 * Upon return, the Log head will be NULL and a new one must be allocated.
 */
void
Log::retireHead()
{
    assert(head != NULL);
    checksumHead();
    head->finalize();
    head = NULL;
}

/**
 * Append data to the head of the log. The data is marked with the provided
 * type. This internal interface makes no restriction on the type being
 * appended and is therefore safe to use with Log-internal types.
 *
 * \param[in]   type    Type of the data added to the log.
 * \param[in]   buf     Opaque data to be written to the log.
 * \param[in]   len     Byte length of the data to be written.
 * \return An immutable pointer to the data in the log, or NULL on failure.
 * \retval NULL if no more space is available. 
 * \retval A valid pointer into the Log if the append succeeded.
 */
const void *
Log::appendAnyType(LogEntryType type, const void *buf, const uint64_t len)
{
    assert(len <= maxAppend);

    if (head == NULL && !newHead())
        return NULL;

    assert(len < segmentSize);

    // ensure enough room exists to write the log entry meta, the object, and
    // reserve space for the checksum & meta at the end
    struct LogEntry le;
    uint64_t needed = len + (2 * sizeof(le)) + sizeof(struct SegmentChecksum);
    if (head->getFreeTail() < needed) {
        if (type == LOG_ENTRY_TYPE_SEGMENT_CHECKSUM) {
            assert(head->getFreeTail() >=
                   (sizeof(le) + sizeof(struct SegmentChecksum)));
        } else {
            if (!newHead())
                return NULL;
            assert(head != NULL);
            assert(head->getFreeTail() > needed);
        }
    }

    assert(type != LOG_ENTRY_TYPE_SEGMENT_HEADER ||
           head->getUtilization() == 0);

    le.type   = type;
    le.length = len;

    // append the header
    const void *r = head->append(&le, sizeof(le));
    assert(r != NULL);

    // append the actual data
    r = head->append(buf, len);
    assert(r != NULL);

    if (type != LOG_ENTRY_TYPE_SEGMENT_HEADER &&
        type != LOG_ENTRY_TYPE_SEGMENT_CHECKSUM) {
        bytesStored += len;
        assert(bytesStored <= (nsegments * segmentSize));
    }

    return r;
}

} // namespace
