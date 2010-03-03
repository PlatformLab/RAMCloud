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

// RAMCloud pragma [GCCWARN=5]

#include <Log.h>

static const uint64_t cleaner_hiwat = 20;
static const uint64_t cleaner_lowat = 10;

namespace RAMCloud {

/**
 * Simple iterator for running through a Segment's (log_entry, blob) pairs.
 */
LogEntryIterator::LogEntryIterator(const Segment *s)
    : segment(s), next(0)
{
    assert(s != NULL);
    next = (const struct log_entry *)s->getBase();
    assert(next != NULL);
    if (next->type != LOG_ENTRY_TYPE_SEGMENT_HEADER) {
        printf("next type is not seg hdr, but %llx\n", next->type);
    }
    assert(next->type == LOG_ENTRY_TYPE_SEGMENT_HEADER);
    assert(next->length == sizeof(struct segment_header));
}

/**
 * Get the next (log_entry, blob) pair from the Segment, returning immutable
 * pointers to the log_entry structure and the object iself, as well as the
 * byte offset of the log_entry structure in the Segment.
 *
 * \param[out]   le     Immutable log_entry structure pointer
 * \param[out]   p      Immutable blob pointer
 * \param[out]   offset If non-NULL, store the offset of the log_entry in the
 *                      Segment.
 * \return True or False
 * \retval True if there was another (log_entry, blob) pair.
 * \retval False if the Segment has been iterated through.
 */
bool
LogEntryIterator::getNextAndOffset(const struct log_entry **le,
                                   const void **p,
                                   uint64_t *offset)
{
    if (next == NULL)
        return false;

    if (le != NULL)
        *le = next;
    if (p != NULL)
        *p = (uint8_t *)next + sizeof(*next);

    const struct log_entry *nle =
        (struct log_entry *)((uint8_t *)next + sizeof(*next) + next->length);
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
 * Get the next (log_entry, blob) pair from the Segment, returning immutable
 * pointers to the log_entry structure and the object iself.
 *
 * \param[out]   le     Immutable log_entry structure pointer
 * \param[out]   p      Immutable blob pointer
 * \return True or False
 * \retval True if there was another (log_entry, blob) pair.
 * \retval False if the Segment has been iterated through.
 */
bool
LogEntryIterator::getNext(const struct log_entry **le, const void **p)
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
      max_append(0),
      segment_size(segsize),
      base(buf),
      segments(0),
      head(0),
      free_list(0),
      nsegments(len / segment_size),
      nfree_list(0),
      bytes_stored(0),
      cleaning(false),
      backup(backup_client)
{
    // at a minimum, we'll have a segment header, one object, and a segment
    // checksum
    uint64_t min_meta = (3 * sizeof(struct log_entry)) +
        sizeof(struct segment_header) + sizeof(struct segment_checksum);
    assert(min_meta <= len);
    max_append = segment_size - min_meta;

    segments = (Segment **)xmalloc(nsegments * sizeof(segments[0]));
    for (uint64_t i = 0; i < nsegments; i++) {
        void *base  = (uint8_t *)buf + (i * segment_size);
        segments[i] = new Segment(base, segment_size, backup);
        free_list   = segments[i]->link(free_list);
        nfree_list++;
    }

    assert(nsegments == nfree_list);
    assert(nfree_list > 0);
}

/**
 * Iterate over each entry in the given Segment and provide it to the given
 * callback.
 *
 * \param[in]   seg     Segment to iterate over.
 * \param[in]   cb      Callback to use.
 * \param[in]   cookie  Opaque cookie to provide to the callback.
 */
void
Log::forEachEntry(const Segment *seg, log_entry_cb_t cb, void *cookie)
{
    assert(cb);

    LogEntryIterator lei(seg);
    const log_entry *le;
    const void *p;

    while (lei.getNext(&le, &p)) {
        cb((log_entry_type_t)le->type, p, le->length, cookie);
        // TODO(stutsman) maybe return amount to add to bytes stored?
    }
}

/**
 * Iterate over each Segment in the log and provide it to the given
 * callback. Note that all Segments are iterated, whether they contain active
 * data or not.
 *
 * \param[in]   cb      Callback to use.
 * \param[in]   limit   Maximum number of Segments to run the callback on.
 * \param[in]   cookie  Opaque cookie to provide to the callback.
 */
void
Log::forEachSegment(log_segment_cb_t cb, uint64_t limit, void *cookie)
{
    for (uint64_t i = 0; i < nsegments && i < limit; i++)
        cb(segments[i], cookie);
}

/**
 * Restore the Log based on backups.
 *
 * \return The number of segments restored from backup
 */
uint64_t
Log::restore()
{
    // TODO Sort this list?
    printf("Restoring from backup before service\n");
    // TODO this is wrong for now - how do we want to determine the
    // number (or max num) of segment frames on backups?
    uint64_t list[nsegments];
    size_t count = backup->getSegmentList(&list[0], nsegments);

    printf("Got segment list from backup (%llu):\n", count);
    for (uint64_t i = 0; i < count; i++)
        printf("\t%llu\n", list[i]);

    // The segments condsidered active by the backup had better fit
    assert(count <= nsegments);
    for (uint64_t i = 0; i < count; i++) {
        printf("Restoring %llu:\n", i);
        head      = free_list;
        free_list = free_list->unlink();
        nfree_list--;
        head->restore(list[i]);
    }

    return count;
}

/**
 * Initialise the Log for use.
 *
 * XXX- Do we have a use for this, or should it move into the constructor?
 */
void
Log::init()
{
    head = free_list;
    free_list = free_list->unlink();
    nfree_list--;
    head->ready(allocateSegmentId());

    struct segment_header sh;
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
Log::append(log_entry_type_t type, const void *buf, const uint64_t len)
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
Log::free(log_entry_type_t type, const void *buf, const uint64_t len)
{
    struct log_entry *le = (struct log_entry *)((uintptr_t)buf -
                                                    sizeof(*le));
    Segment *s = getSegment(le, len + sizeof(*le));

    assert(le->type == type);
    assert(le->length == len);
    assert(type != LOG_ENTRY_TYPE_SEGMENT_HEADER &&
           type != LOG_ENTRY_TYPE_SEGMENT_CHECKSUM);
    assert(len <= bytes_stored);

    // Note that we do not adjust the 'bytes_stored' count here,
    // but do so only during cleaning.

    s->free(len + sizeof(*le));
    if (s->getUtilization() == 0) {
        // TODO(rumble) need to handle mutability,
        // inc. segment #, etc
        // TODO(rumble) good idea may be
        // to do this only when segment pulled from free list
        assert(!cleaning);

        assert(s->unlink() == NULL);
        free_list = s->link(free_list);
        assert(free_list->getUtilization() == 0);
        nfree_list++;
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
Log::registerType(log_entry_type_t type,
                  log_eviction_cb_t evict_cb,
                  void *cookie)
{
    assert(evict_cb != NULL);
    assert(numCallbacks < sizeof(callbacks)/sizeof(callbacks[0]));

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
           segment_size, (float)segment_size / (1024*1024));
    printf("  num segments:          %" PRIu64 "\n",
           nsegments);
    printf("  cumulative storage:    %" PRIu64 " (%.3fMB)\n",
           (nsegments * segment_size),
           (float)(nsegments * segment_size) / (1024*1024));
    printf("  non-meta bytes stored: %" PRIu64 " (%.3fMB)\n",
           bytes_stored, (float)bytes_stored / (1024*1024));
    printf("  non-meta utilization:  %.3f%%\n",
           (float)bytes_stored / (nsegments * segment_size) * 100.0);
    printf("  non-meta efficiency:   %.3f%%\n",
           (float)bytes_stored / ((nsegments - nfree_list) *
                                  segment_size) * 100.0);
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
    return max_append;
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
log_eviction_cb_t
Log::getEvictionCallback(log_entry_type_t type, void **cookie)
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
    uintptr_t max = (uintptr_t)base + (segment_size * nsegments);

    assert(up >= ub && (up + len) <= max);
    uintptr_t segno = (up - ub) / segment_size;
    assert(segno < nsegments);

    Segment *s = segments[segno];
    uintptr_t sb = (uintptr_t)s->getBase();
    assert(up >= sb && (up + len) <= (sb + segment_size));

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
 * 'cleaner_hiwat'.
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

        if (nfree_list >= cleaner_hiwat)
            break;

        uint64_t util = s->getUtilization();
        if (util != 0 && util < (3 * segment_size / 4)) {
            // Note that since our segments are immutable (when not writing to
            // the head) we may certainly iterate over objects for which
            // log_free() has already been called. That's fine - it's up to the
            // callback to determine that its stale data and take no action.
            //
            // For this reason, we do _not_ want to do s->free() on that space,
            // since for already freed space, we'll double-count.

            LogEntryIterator lei(s);
            const struct log_entry *le;
            const void *p;

            while (lei.getNext(&le, &p)) {
                void *cookie;
                log_eviction_cb_t cb =
                    getEvictionCallback((log_entry_type_t)le->type, &cookie);

                if (le->type != LOG_ENTRY_TYPE_SEGMENT_HEADER &&
                    le->type != LOG_ENTRY_TYPE_SEGMENT_CHECKSUM) {
                    assert(le->length <= bytes_stored);
                    bytes_stored -= le->length;
                }

                if (cb != NULL)
                    cb((log_entry_type_t)le->type, p, le->length, cookie);
            }

            assert(s->unlink() == NULL);
            s->reset();
            free_list = s->link(free_list);
            nfree_list++;
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

    if (nfree_list < cleaner_lowat) {
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

    if (free_list == NULL) {
        assert(nfree_list == 0);
        return false;
    }

    head = free_list;
    free_list = head->unlink();
    nfree_list--;
    head->ready(allocateSegmentId());

    assert(head->getUtilization() == 0);

    struct segment_header sh;
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
           (sizeof(struct log_entry) + sizeof(struct segment_checksum)));

    struct segment_checksum sc;
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
Log::appendAnyType(log_entry_type_t type, const void *buf, const uint64_t len)
{
    assert(len <= max_append);

    if (head == NULL && !newHead())
        return NULL;

    assert(len < segment_size);

    // ensure enough room exists to write the log entry meta, the object, and
    // reserve space for the checksum & meta at the end
    struct log_entry le;
    uint64_t needed = len + (2 * sizeof(le)) + sizeof(struct segment_checksum);
    if (head->getFreeTail() < needed) {
        if (type == LOG_ENTRY_TYPE_SEGMENT_CHECKSUM) {
            assert(head->getFreeTail() >=
                   (sizeof(le) + sizeof(struct segment_checksum)));
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
        bytes_stored += len;
        assert(bytes_stored <= (nsegments * segment_size));
    }

    return r;
}

} // namespace
