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
#include <string.h>
#include <shared/Segment.h>
#include <shared/backup_client.h>

namespace RAMCloud {

Segment::Segment(uint64_t init_id,
                 void *buf,
                 const uint64_t len,
                 BackupClient *backup_client)
    : base(buf),
      id(~(0ull)),
      total_bytes(len),
      free_bytes(total_bytes),
      tail_bytes(total_bytes),
      isMutable(false),
      backup(backup_client),
      next(0),
      prev(0)
{
	assert(buf != NULL);
	assert(len > 0);

	reset(init_id);
}

Segment::~Segment()
{
	assert(0);
}

void
Segment::reset(uint64_t new_id)
{
	assert(!isMutable);

	if (id != ~(0ull))
		backup->Free(id);

	free_bytes  = total_bytes;
	tail_bytes  = total_bytes;
	isMutable   = true;
	id	    = new_id;
	memset(base, 0xcc, total_bytes);
}

const void *
Segment::append(const void *buf, uint64_t len)
{
	assert(isMutable);

	if (tail_bytes < len)
		return NULL;

	assert(free_bytes >= len);

	uint64_t offset = total_bytes - tail_bytes;
	void *loc = (uint8_t *)base + offset;

	memcpy(loc, buf, len);
	backup->Write(id, offset, buf, len);
	free_bytes -= len;
	tail_bytes -= len;

	return loc;
}

void
Segment::free(uint64_t len)
{
	assert((len + free_bytes) <= total_bytes);
	free_bytes += len;
}

const void *
Segment::getBase() const
{
	return base;
}

uint64_t
Segment::getId() const
{
	return id;
}

uint64_t
Segment::getFreeTail() const
{
	return tail_bytes;
}

uint64_t
Segment::getLength() const
{
	return total_bytes;
}

uint64_t
Segment::getUtilization() const
{
	return total_bytes - free_bytes;
}

bool
Segment::checkRange(const void *p, uint64_t len) const
{
	uintptr_t up = (uintptr_t)p;
	uintptr_t ub = (uintptr_t)base;

	return (up >= ub && up < (ub + total_bytes));
}

void
Segment::finalize()
{
	isMutable = false;
	backup->Commit(id);
}

void
Segment::restore(uint64_t restore_seg_id)
{
    //printf("Segment restoring from %llu:\n", restore_seg_id);
    backup->Retrieve(restore_seg_id, base);
    // TODO restore all sorts of state/invariants
    // It seems we want to restore this information by making a single
    // pass which happens in the server to rebuild the hashtable
    id = restore_seg_id;
}

Segment *
Segment::link(Segment *n)
{
	assert(prev == NULL && next == NULL);
	assert(n == NULL || n->prev == NULL);

	if (n != NULL)
		n->prev = this;
	next = n;

	return this;
}

Segment *
Segment::unlink()
{
	if (prev != NULL)
		prev->next = next;
	if (next != NULL)
		next->prev = prev;

	Segment *n = next;
	prev = next = NULL;
	return n;
}

} // namespace
