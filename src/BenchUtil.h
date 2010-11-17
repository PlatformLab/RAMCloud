/* Copyright (c) 2010 Stanford University
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

/**
 * \file
 * Header file for utilities useful in benchmarking.
 */

#include "Common.h"

#ifndef RAMCLOUD_BENCHUTIL_H
#define RAMCLOUD_BENCHUTIL_H

namespace RAMCloud {

uint64_t getCyclesPerSecond();
uint64_t cyclesToNanoseconds(uint64_t cycles);
double cyclesToSeconds(uint64_t cycles);
uint64_t nanosecondsToCycles(uint64_t ns);

void fillRandom(void* buf, uint32_t size);
void fillPrintableRandom(void* buf, uint32_t size);

} // end RAMCloud

#endif
