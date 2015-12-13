/* Copyright (c) 2011-2015 Stanford University
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

#ifndef RAMCLOUD_EXCEPTION_H
#define RAMCLOUD_EXCEPTION_H

#include "CodeLocation.h"
#include "Minimal.h"

namespace RAMCloud {

// Imported from Common.h (don't want to include Common.h here, since
// that would expose it to all RAMCloud clients).
string demangle(const char* name);

/**
 * The base class for all RAMCloud exceptions.
 */
struct Exception : public std::exception {
    explicit Exception(const CodeLocation& where)
        : message(""), errNo(0), where(where), whatCache() {}
    Exception(const CodeLocation& where, std::string msg)
        : message(msg), errNo(0), where(where), whatCache() {}
    Exception(const CodeLocation& where, int errNo)
        : message(""), errNo(errNo), where(where), whatCache() {
        message = strerror(errNo);
    }
    Exception(const CodeLocation& where, string msg, int errNo)
        : message(msg + ": " + strerror(errNo)), errNo(errNo), where(where),
          whatCache() {}
    Exception(const Exception& other)
        : message(other.message), errNo(other.errNo), where(other.where),
          whatCache() {}
    virtual ~Exception() throw() {}
    string str() const {
        return (demangle(typeid(*this).name()) + ": " + message +
                ", thrown at " + where.str());
    }
    const char* what() const throw() {
        if (whatCache)
            return whatCache.get();
        string s(str());
        char* cStr = new char[s.length() + 1];
        whatCache.reset(const_cast<const char*>(cStr));
        memcpy(cStr, s.c_str(), s.length() + 1);
        return cStr;
    }
    string message;
    int errNo;
    CodeLocation where;
  private:
    mutable std::unique_ptr<const char[]> whatCache;
};

/**
 * A fatal error that should exit the program.
 */
struct FatalError : public Exception {
    explicit FatalError(const CodeLocation& where)
        : Exception(where) {}
    FatalError(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
    FatalError(const CodeLocation& where, int errNo)
        : Exception(where, errNo) {}
    FatalError(const CodeLocation& where, string msg, int errNo)
        : Exception(where, msg, errNo) {}
};

/**
 * An exception that is thrown when someone tries to wait for an
 * RPC that has been canceled.
 */
struct RpcCanceledException : public Exception {
    explicit RpcCanceledException(const CodeLocation& where)
        : Exception(where) {}
};

/// An exception used only for testing purposes.
struct TestingException : public Exception {
    explicit TestingException(const CodeLocation& where) : Exception(where) {}
};

/**
 * Throws an exception after a given certain number of calls.
 * Used for testing only.
 */
template<typename E = TestingException>
struct DelayedThrower {
#if TESTING
    explicit DelayedThrower(uint64_t tillThrow = ~0UL)
        : tillThrow(tillThrow)
    {
    }
    void operator()() {
        if (tillThrow == 0) {
            tillThrow = ~0UL;
            throw E(HERE);
        } else {
            --tillThrow;
        }
    }
    uint64_t tillThrow;
#else
    explicit DelayedThrower(uint64_t tillThrow = ~0UL) {}
    void operator()() {}
#endif
};

} // end RAMCloud

#endif  // RAMCLOUD__H
