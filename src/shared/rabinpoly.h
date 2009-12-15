/* -*-c++-*- */
/*
 * Copyright (C) 2000 David Mazieres (dm@uun.org)
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
#ifndef _RABINPOLY_H_
#define _RABINPOLY_H_ 1

#include <sys/types.h>
#include <string.h>

u_int64_t polymod (u_int64_t nh, u_int64_t nl, u_int64_t d);
u_int64_t polygcd (u_int64_t x, u_int64_t y);
void polymult (u_int64_t *php, u_int64_t *plp, u_int64_t x, u_int64_t y);
u_int64_t polymmult (u_int64_t x, u_int64_t y, u_int64_t d);
bool polyirreducible (u_int64_t f);
u_int64_t polygen (u_int degree);

class rabinpoly {
  int shift;
  u_int64_t T[256];		// Lookup table for mod
  void calcT ();
public:
  const u_int64_t poly;		// Actual polynomial

  explicit rabinpoly (u_int64_t poly);
  u_int64_t append8 (u_int64_t p, u_char m) const
    { return ((p << 8) | m) ^ T[p >> shift]; }
};

class window : public rabinpoly {
public:
  enum {size = 48};
  //enum {size = 24};
private:
  u_int64_t fingerprint;
  int bufpos;
  u_int64_t U[256];
  u_char buf[size];

public:
  window (u_int64_t poly);
  u_int64_t slide8 (u_char m) {
    if (++bufpos >= size)
      bufpos = 0;
    u_char om = buf[bufpos];
    buf[bufpos] = m;
    return fingerprint = append8 (fingerprint ^ U[om], m);
  }
  void reset () { 
    fingerprint = 0; 
    bzero ((char*) buf, sizeof (buf));
  }
};

#endif /* !_RABINPOLY_H_ */
