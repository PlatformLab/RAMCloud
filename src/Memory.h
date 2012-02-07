/* Copyright (c) 2011 Stanford University
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

#ifndef RAMCLOUD_MEMORY_H
#define RAMCLOUD_MEMORY_H

#include "Common.h"

namespace RAMCloud {
namespace Memory {

void* xmalloc(const CodeLocation& where, size_t len);
void* xmemalign(const CodeLocation& where, size_t alignment, size_t len);
char* xstrdup(const CodeLocation& where, const char* str);

/**
 * Simpler type for use in creating unique_ptrs which call specific function for
 * deallocation instead of delete (usually std::free()).
 * Example:
 * unique_ptr_free page(Memory::xmemalign(HERE, getpagesize(), getpagesize()),
 *                      std::free);
 */
typedef std::unique_ptr<void, void (*)(void*)> unique_ptr_free; // NOLINT

} // end Memory
} // end RAMCloud

#endif  // RAMCLOUD_MEMORY_H
