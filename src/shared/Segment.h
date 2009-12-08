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

#ifndef _SEGMENT_H_
#define _SEGMENT_H_

#include <stdint.h>

#include <shared/common.h>
#include <shared/backup_client.h>

namespace RAMCloud {

class Segment {
  public:
	Segment(uint64_t, void *, const uint64_t, BackupClient *);
       ~Segment();
	void        reset(uint64_t);
	const void *append(const void *, const uint64_t);
	void        free(uint64_t);
	const void *getBase();
	uint64_t getId();
	uint64_t getFreeTail();
	uint64_t getLength();
	uint64_t getUtilization();
	bool	 checkRange(const void *, uint64_t);
	void	 finalize();
	Segment *link(Segment *n);
	Segment *unlink();
  private:
	void     *base;
	uint64_t  id;			// segment id
	uint64_t  total_bytes;		// capacity of the segment
	uint64_t  free_bytes;		// bytes free in segment (anywhere)
	uint64_t  tail_bytes;		// bytes free in tail of segment (i.e. never written to)
	bool      isMutable;

	BackupClient *backup;

	Segment  *next, *prev;
	DISALLOW_COPY_AND_ASSIGN(Segment);
};

} // namespace

#endif // !_SEGMENT_H_
