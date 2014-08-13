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

#ifndef RAMCLOUD_NOOP_H
#define RAMCLOUD_NOOP_H

#include "Common.h"

namespace RAMCloud {

/**
 * Like an (atomic) integer that is compiled out (for performance reasons).
 * The value of the integer is always 0, regardless of what operations you
 * might do with it.
 */
template<typename T>
struct NoOp {
    NoOp() {
    }
    explicit NoOp(const T& value) {
    }
    NoOp(const NoOp<T>& other) { // NOLINT
    }
    NoOp<T>& operator=(const NoOp<T>& other) {
        return *this;
    }
    NoOp<T>& operator=(const T&) {
        return *this;
    }
    operator T() const {
        return T(0);
    }
    T load() const {
        return 0;
    }
    // arithmetic
    NoOp<T>& operator+=(const T&) {
        return *this;
    }
    NoOp<T>& operator-=(const T&) {
        return *this;
    }
    NoOp<T>& operator++() {
        return *this;
    }
    NoOp<T>& operator--() {
        return *this;
    }
    NoOp<T> operator++(int _) {
        return *this;
    }
    NoOp<T> operator--(int _) {
        return *this;
    }
    NoOp<T>& operator&=(const T&) {
        return *this;
    }
    NoOp<T>& operator|=(const T&) {
        return *this;
    }
    NoOp<T>& operator^=(const T&) {
        return *this;
    }
};
static_assert(__is_empty(NoOp<uint64_t>),
              "NoOp<...> should be empty");
static_assert(sizeof(NoOp<uint64_t>) == 1,
              "sizeof(NoOp<...>) should be 1");

} // end RAMCloud

#endif  // RAMCLOUD_NOOP_H
