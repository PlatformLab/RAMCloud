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

#include <assert.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#define __STDC_FORMAT_MACROS    // needed for c++
#include <inttypes.h>
#include <shared/Segment.h>
#include <shared/LogTypes.h>
#include <shared/Log.h>
#include <shared/common.h>
#include <shared/backup_client.h>

static const uint64_t cleaner_hiwat = 20;
static const uint64_t cleaner_lowat = 10;

struct log_entry {
	uint32_t  type;
	uint32_t  length;
};

struct segment_header {
	uint64_t id;
};

struct segment_checksum {
	uint64_t checksum;
};

namespace RAMCloud {

// Simple iterator for running through a segment's (log_entry, blob) pairs.
class LogEntryIterator {
  public:
	LogEntryIterator(const Segment *s) : segment(s), next(0)
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

	bool
	getNext(const struct log_entry **le, const void **p)
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

		return true;
	}

  private:
	const Segment *segment;
	const struct log_entry *next;
};

  /**************************/
 /**** Public Interface ****/
/**************************/

Log::Log(const uint64_t segsize,
         void *buf,
         const uint64_t len,
         BackupClient *backup_client)
    : callback(0),
      callback_type(LOG_ENTRY_TYPE_SEGMENT_HEADER),
      callback_cookie(0),
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

	// Warning: this is a huge hack - the segments are constructed
	// in reverse so that the free_list will be in the right order
	// this is the only thing (for the moment) preserving the
	// invariant that segment numbers on the backup are strictly
	// increasing.
	// The restore also counts on it because it simply populates
	// the head of the free list with active data and expects that
	// that corresponds to the beginning of the log when it
	// iterates to restore the hashtable
	segments = (Segment **)malloc(nsegments * sizeof(segments[0]));
	for (int64_t i = nsegments - 1; i >= 0; i--) {
		void *base  = (uint8_t *)buf + (i * segment_size);
		segments[i] = new Segment(i, base, segment_size, backup);
		free_list   = segments[i]->link(free_list);
		nfree_list++;
	}

	assert(nsegments == nfree_list);
	assert(nfree_list > 0);
}

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

void
Log::forEachSegment(log_segment_cb_t cb, uint64_t limit, void *cookie)
{
    for (uint64_t i = 0;
         i < nsegments && i < limit;
         i++) {
        cb(segments[i], cookie);
    }
}

// Returns the number of segments restored from backup
uint64_t
Log::restore()
{
    // TODO Sort this list?
    printf("Restoring from backup before service\n");
    // TODO this is wrong for now - how do we want to determine the
    // number (or max num) of segment frames on backups?
    uint64_t list[nsegments];
    uint64_t count = nsegments;
    backup->GetSegmentList(&list[0], &count);

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

void
Log::init()
{
	head      = free_list;
	free_list = free_list->unlink();
	nfree_list--;

	struct segment_header sh;
	sh.id = 0;

	const void *r = appendAnyType(LOG_ENTRY_TYPE_SEGMENT_HEADER, &sh, sizeof(sh));
	assert(r != NULL);
}

const void *
Log::append(log_entry_type_t type, const void *buf, const uint64_t len)
{
	if (type == LOG_ENTRY_TYPE_SEGMENT_HEADER ||
	    type == LOG_ENTRY_TYPE_SEGMENT_CHECKSUM)
		return NULL;

	return appendAnyType(type, buf, len);
}

void
Log::free(log_entry_type_t type, const void *buf, const uint64_t len)
{
	struct log_entry *le = (struct log_entry *)((uintptr_t)buf - sizeof(*le));
	Segment *s = getSegment(le, len + sizeof(*le));

	assert(le->type == type);
	assert(le->length == len);
	assert(type != LOG_ENTRY_TYPE_SEGMENT_HEADER &&
	       type != LOG_ENTRY_TYPE_SEGMENT_CHECKSUM);
	assert(len <= bytes_stored);

	// Note that we do not adjust the 'bytes_stored' count here, but do so only
	// during cleaning.

	s->free(len + sizeof(*le));
	if (s->getUtilization() == 0) {
		// XXX- need to handle mutability, inc. segment #, etc
		// XXX- good idea may be to do this only when segment pulled from free list
		assert(!cleaning);

		assert(s->unlink() == NULL);
		free_list = s->link(free_list);
		assert(free_list->getUtilization() == 0);
		nfree_list++;
		if (s == head) {
			retireHead();
			assert(head == NULL);
		}
		s->reset(s->getId() + nsegments);
	}
}

bool
Log::registerType(log_entry_type_t type, log_eviction_cb_t evict_cb, void *cookie)
{
	assert(evict_cb != NULL);

	// only one callback for now (it's easy to extend, but I've more fun stuff to write).
	assert(callback == NULL);
	callback = evict_cb;
	callback_type = type;
	callback_cookie = cookie;

	return false;
}

void
Log::printStats()
{
	printf("::LOG STATS::\n");
	printf("  segment len:           %" PRIu64 " (%.3fMB)\n", segment_size, (float)segment_size / (1024*1024));
	printf("  num segments:          %" PRIu64 "\n", nsegments);
	printf("  cumulative storage:    %" PRIu64 " (%.3fMB)\n", (nsegments * segment_size), (float)(nsegments * segment_size) / (1024*1024));
	printf("  non-meta bytes stored: %" PRIu64 " (%.3fMB)\n", bytes_stored, (float)bytes_stored / (1024*1024));
	printf("  non-meta utilization:  %.3f%%\n", (float)bytes_stored / (nsegments * segment_size) * 100.0);
	printf("  non-meta efficiency:   %.3f%%\n", (float)bytes_stored / ((nsegments - nfree_list) * segment_size) * 100.0);
}

uint64_t
Log::getMaximumAppend()
{
	return max_append;
}

  /*************************/
 /**** Private Methods ****/
/*************************/

log_eviction_cb_t
Log::getEvictionCallback(log_entry_type_t type, void **cookie)
{
	if (callback_type == type)
		return callback;
	if (cookie != NULL)
		*cookie = callback_cookie;
	return NULL;
}

Segment *
Log::getSegment(const void *p, uint64_t len)
{
	uintptr_t up  = (uintptr_t)p;
	uintptr_t ub  = (uintptr_t)base;
	uintptr_t max = (uintptr_t)base + (segment_size * nsegments); 

	assert(up >= ub && (up + len) < max);
	uintptr_t segno = (up - ub) / segment_size;
	assert(segno < nsegments);

	Segment *s = segments[segno];
	uintptr_t sb = (uintptr_t)s->getBase();
	assert(up >= sb && (up + len) < (sb + segment_size));

	return (s);
}

//walk objects, calling the eviction cb on each
//the callback will either do nothing, or re-write to the head of
//the log. it's very important that this re-writing be guaranteed to
//succeed and that we do not recurse into cleaning!!
void
Log::clean()
{
	assert(head == NULL);

	// we may come back here due to a log append when our eviction callback results
	// in an object being written out once again.
	// this is ok, but when we're finished cleaning, we may toss an underutilized segment
	// and reallocate a new head in our caller, newHead(). it's probably unimportant, but we
	// may want to preserve enough state to detect and avoid that case.
	if (cleaning)
		return;
	cleaning = true;

	for (uint64_t i = 0; i < nsegments; i++) {
		Segment *s = segments[i];

		if (nfree_list >= cleaner_hiwat)
			break;

		uint64_t util = s->getUtilization();
		if (util != 0 && util < (3 * segment_size / 4)) {
			// Note that since our segments are immutable (when not writing to the head)
			// we may certainly iterate over objects for which log_free() has already
			// been called. That's fine - it's up to the callback to determine that its
			// stale data and take no action.
			//
			// For this reason, we do _not_ want to do s->free() on that space, since for
			// already freed space, we'll double-count.

			LogEntryIterator lei(s);
			const struct log_entry *le;
			const void *p;

			while (lei.getNext(&le, &p)) {
				void *cookie;
				log_eviction_cb_t cb = getEvictionCallback((log_entry_type_t)le->type, &cookie);

				if (le->type != LOG_ENTRY_TYPE_SEGMENT_HEADER &&
				    le->type != LOG_ENTRY_TYPE_SEGMENT_CHECKSUM) {
					assert(le->length <= bytes_stored);
					bytes_stored -= le->length;
				}

				if (cb != NULL)
					cb((log_entry_type_t)le->type, p, le->length, cookie);
			}

			assert(s->unlink() == NULL);
			s->reset(s->getId() + nsegments);
			free_list = s->link(free_list);
			nfree_list++;
		}
	}

	cleaning = false;
}

// Head of the Log is done. Retire it and get a new head.
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

	assert(head->getUtilization() == 0);

	struct segment_header sh;
	sh.id = head->getId();

	const void *r = appendAnyType(LOG_ENTRY_TYPE_SEGMENT_HEADER, &sh, sizeof(sh));
	assert(r != NULL);

	return true;
}

void
Log::checksumHead()
{
	assert(head != NULL);
	assert(head->getFreeTail() >=
	    (sizeof(struct log_entry) + sizeof(struct segment_checksum)));

	struct segment_checksum sc;
	sc.checksum = 0xbeefcafebeefcafe;
	appendAnyType(LOG_ENTRY_TYPE_SEGMENT_CHECKSUM, &sc, sizeof(sc));
}

void
Log::retireHead()
{
	assert(head != NULL);
	checksumHead();
	head->finalize();
	head = NULL;
}

const void *
Log::appendAnyType(log_entry_type_t type, const void *buf, const uint64_t len)
{
	assert(len <= max_append);

	if (head == NULL && !newHead())
		return NULL;

	assert(len < segment_size);

	// ensure enough room exists to write the log entry meta, the object, and reserve space for
	// the checksum & meta at the end
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

	assert(type != LOG_ENTRY_TYPE_SEGMENT_HEADER || head->getUtilization() == 0);

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
