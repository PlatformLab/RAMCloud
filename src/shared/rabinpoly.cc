/*
 * Copyright (C) 1999 David Mazieres (dm@uun.org)
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

#include <inttypes.h>

#include <shared/rabinpoly.h>
#include <shared/msb.h>
#ifndef INT64
# define INT64(n) n##LL
#endif
#define MSB64 INT64(0x8000000000000000)
                    
uint64_t
polymod (uint64_t nh, uint64_t nl, uint64_t d)
{
  //  assert (d);
  int k = fls64 (d) - 1;
  d <<= 63 - k;

  if (nh) {
    if (nh & MSB64)
      nh ^= d;
    for (int i = 62; i >= 0; i--)
      if (nh & ((uint64_t) 1) << i) {
	nh ^= d >> (63 - i);
	nl ^= d << (i + 1);
      }
  }
  for (int i = 63; i >= k; i--)
  {  
    if (nl & INT64 (1) << i)
      nl ^= d >> (63 - i);
  }
  
  return nl;
}

uint64_t
polygcd (uint64_t x, uint64_t y)
{
  for (;;) {
    if (!y)
      return x;
    x = polymod (0, x, y);
    if (!x)
      return y;
    y = polymod (0, y, x);
  }
}

void
polymult (uint64_t *php, uint64_t *plp, uint64_t x, uint64_t y)
{
  uint64_t ph = 0, pl = 0;
  if (x & 1)
    pl = y;
  for (int i = 1; i < 64; i++)
    if (x & (INT64 (1) << i)) {
      ph ^= y >> (64 - i);
      pl ^= y << i;
    }
  if (php)
    *php = ph;
  if (plp)
    *plp = pl;
}

uint64_t
polymmult (uint64_t x, uint64_t y, uint64_t d)
{
  uint64_t h, l;
  polymult (&h, &l, x, y);
  return polymod (h, l, d);
}

bool
polyirreducible (uint64_t f)
{
  uint64_t u = 2;
  int m = (fls64 (f) - 1) >> 1;
  for (int i = 0; i < m; i++) {
    u = polymmult (u, u, f);
    if (polygcd (f, u ^ 2) != 1)
      return false;
  }
  return true;
}

void
rabinpoly::calcT ()
{
  //  assert (poly >= 0x100);
  int xshift = fls64 (poly) - 1;
  shift = xshift - 8;
  uint64_t T1 = polymod (0, INT64 (1) << xshift, poly);
  for (int j = 0; j < 256; j++)
  {
    T[j] = polymmult (j, T1, poly) | ((uint64_t) j << xshift);
  }
}

rabinpoly::rabinpoly (uint64_t p)
  : poly (p)
{
  calcT ();
}

window::window (uint64_t poly)
  : rabinpoly (poly), fingerprint (0), bufpos (-1)
{
  uint64_t sizeshift = 1;
  for (int i = 1; i < size; i++)
    sizeshift = append8 (sizeshift, 0);
  for (int i = 0; i < 256; i++)
    U[i] = polymmult (i, sizeshift, poly);
  bzero ((char*) buf, sizeof (buf));
}
