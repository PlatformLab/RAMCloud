/* -*-c++-*- */
/*
 * Copyright (C) 1998 David Mazieres (dm@uun.org)
 * The work in this file is available elsewhere under other licenses.
 * It licensed here with explicit permission as follows:
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

// RAMCloud pragma [CPPLINT=0]

#ifndef RAMCLOUD_MSB_H
#define RAMCLOUD_MSB_H 1

#ifdef __cplusplus
#define EXTERN_C extern "C"
#else /* !__cplusplus */
#define EXTERN_C extern
#endif /* !__cplusplus */

/*
 * Routines for calculating the most significant bit of an integer.
 */

const char bytemsb[0x100] = {
  0, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5,
  5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
  6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7,
  7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
  7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
  7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
  8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
};

/* Find last set (most significant bit) */
static inline unsigned int fls32 (uint32_t) __attribute__ ((const));
static inline unsigned int
fls32 (uint32_t v)
{
  if (v & 0xffff0000) {
    if (v & 0xff000000)
      return 24 + bytemsb[v>>24];
    else
      return 16 + bytemsb[v>>16];
  }
  if (v & 0x0000ff00)
    return 8 + bytemsb[v>>8];
  else
    return bytemsb[v];
}

/* Ceiling of log base 2 */
static inline int log2c32 (uint32_t) __attribute__ ((const));
static inline int
log2c32 (uint32_t v)
{
  return v ? (int) fls32 (v - 1) : -1;
}

static inline unsigned int fls64 (uint64_t) __attribute__ ((const));
static inline unsigned int
fls64 (uint64_t v)
{
  uint32_t h;
  if ((h = v >> 32))
    return 32 + fls32 (h);
  else
    return fls32 ((uint32_t) v);
}

static inline int log2c64 (uint64_t) __attribute__ ((const));
static inline int
log2c64 (uint64_t v)
{
  return v ? (int) fls64 (v - 1) : -1;
}

#define fls(v) (sizeof (v) > 4 ? fls64 (v) : fls32 (v))
#define log2c(v) (sizeof (v) > 4 ? log2c64 (v) : log2c32 (v))

/*
 * For symmetry, a 64-bit find first set, "ffs," that finds the least
 * significant 1 bit in a word.
 */

EXTERN_C const char bytelsb[];

static inline unsigned int
ffs32 (uint32_t v)
{
  if (v & 0xffff) {
    if (int vv = v & 0xff)
      return bytelsb[vv];
    else
      return 8 + bytelsb[v >> 8 & 0xff];
  }
  else if (int vv = v & 0xff0000)
    return 16 + bytelsb[vv >> 16];
  else if (v)
    return 24 + bytelsb[v >> 24 & 0xff];
  else
    return 0;
}

static inline unsigned int
ffs64 (uint64_t v)
{
  uint32_t l;
  if ((l = v & 0xffffffff))
    return fls32 (l);
  else if ((l = v >> 32))
    return 32 + fls32 (l);
  else
    return 0;
}

#define ffs(v) (sizeof (v) > 4 ? ffs64 (v) : ffs32 (v))

#undef EXTERN_C

#endif /* _MSB_H_  */
