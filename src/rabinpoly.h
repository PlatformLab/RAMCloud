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

#ifndef RAMCLOUD_RABINPOLY_H
#define RAMCLOUD_RABINPOLY_H 1

#include <sys/types.h>
#include <string.h>

uint64_t polymod(uint64_t nh, uint64_t nl, uint64_t d);
uint64_t polygcd(uint64_t x, uint64_t y);
void polymult(uint64_t *php, uint64_t *plp, uint64_t x, uint64_t y);
uint64_t polymmult(uint64_t x, uint64_t y, uint64_t d);
bool polyirreducible(uint64_t f);

class rabinpoly {
    int shift;
    uint64_t T[256];                    // Lookup table for mod
    void calcT();
  public:
    const uint64_t poly;                // Actual polynomial

    explicit rabinpoly(uint64_t poly);
    virtual ~rabinpoly() { }
    uint64_t append8(uint64_t p, uint8_t m) const
        { return ((p << 8) | m) ^ T[p >> shift]; }
};

class window : public rabinpoly {
  public:
    enum {size = 48};
    //enum {size = 24};
  private:
    uint64_t fingerprint;
    int bufpos;
    uint64_t U[256];
    uint8_t buf[size];

  public:
    explicit window(uint64_t poly);
    uint64_t slide8(uint8_t m) {
        if (++bufpos >= size)
            bufpos = 0;
        uint8_t om = buf[bufpos];
        buf[bufpos] = m;
        return fingerprint = append8(fingerprint ^ U[om], m);
    }
    void reset() {
        fingerprint = 0;
        bzero(reinterpret_cast<char*>(buf), sizeof(buf));
    }
};

#endif /* !_RABINPOLY_H_ */
