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

#ifndef _LOG_H_
#define _LOG_H_

#include <stdint.h>
#include <shared/LogTypes.h>
#include <shared/Segment.h>
#include <shared/common.h>
#include <shared/backup_client.h>

namespace RAMCloud {

//it'd be nice if the callback could take a Log *, but the class isn't defined
//yet and we use log_eviction_cb_t in the class definition below. ugh. is there
//a solution?
typedef void (*log_eviction_cb_t)(log_entry_type_t, const void *, const uint64_t, void *);
typedef void (*log_foreach_cb_t)(const Segment *);

class Log {
  public:
	Log(const uint64_t, void *, const uint64_t, BackupClient *);
       ~Log();
	const void *append(log_entry_type_t, const void *, uint64_t);
	void        free(log_entry_type_t, const void *, uint64_t);
	bool        registerType(log_entry_type_t, log_eviction_cb_t, void *);
	void        printStats();
	uint64_t    getMaximumAppend();
	void init();
	void restore();
	void forEachSegment(log_foreach_cb_t, uint64_t limit);

  private:
	void        clean(void);
	bool        newHead();
	void        checksumHead();
	void        retireHead();
	const void *appendAnyType(log_entry_type_t, const void *, uint64_t);
	log_eviction_cb_t getEvictionCallback(log_entry_type_t, void **);
	Segment    *getSegment(const void *, uint64_t);

	//XXX- fixme: should be extensible
	log_eviction_cb_t callback;
	log_entry_type_t  callback_type;
	void		 *callback_cookie;

	uint64_t max_append;		// max bytes append() can ever take
	uint64_t segment_size;		// size of each segment in bytes
	void    *base;			// base of all segments
	Segment **segments;		// array of all segments
	Segment *head;			// head of the log
	Segment *free_list;		// free (utilization == 0) segments
	uint64_t nsegments;		// total number of segments in the system
	uint64_t nfree_list;		// number of segments in free list
	uint64_t bytes_stored;		// bytes stored in the log (non-metadata only)
	bool     cleaning;		// presently cleaning the log
	BackupClient *backup;
	DISALLOW_COPY_AND_ASSIGN(Log);
};

} // namespace

#endif // !_LOG_H_
