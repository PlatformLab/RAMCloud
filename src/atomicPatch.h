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

/**
 * \file
 * In gcc 4.4.4 the cstdatomic header file omitted the bodies for
 * several functions. This header file supplies them.  Gcc is fixed
 * in later releases.
 */

#ifndef RAMCLOUD_ATOMICPATCH_H
#define RAMCLOUD_ATOMICPATCH_H

#if __GNUC__ == 4 && __GNUC_MINOR__ < 6

namespace std {
    template<typename _Tp>
    void
    store(_Tp* __v, memory_order __m)
    { atomic_address::store(__v, __m); }

    template<typename _Tp>
    _Tp*
    load(memory_order __m)
    { return static_cast<_Tp*>(atomic_address::load(__m)); }

    template<typename _Tp>
    _Tp*
    exchange(_Tp* __v, memory_order __m)
    { return static_cast<_Tp*>(atomic_address::exchange(__v, __m)); }
}

#endif //version check

#endif // RAMCLOUD_ATOMICPATCH_H

