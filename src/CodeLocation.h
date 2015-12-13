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

#ifndef RAMCLOUD_CODELOCATION_H
#define RAMCLOUD_CODELOCATION_H

#include "Minimal.h"

namespace RAMCloud {

/**
 * Describes the location of a line of code.
 * You can get one of these with #HERE.
 */
struct CodeLocation {
    /// Called by #HERE only.
    CodeLocation(const char* file,
                 const uint32_t line,
                 const char* function,
                 const char* prettyFunction)
        : file(file)
        , line(line)
        , function(function)
        , prettyFunction(prettyFunction)
    {}
    string str() const {
        return format("%s at %s:%d",
                      qualifiedFunction().c_str(),
                      relativeFile().c_str(),
                      line);
    }
    const char* baseFileName() const;
    string relativeFile() const;
    string qualifiedFunction() const;

    /// __FILE__
    const char* file;
    /// __LINE__
    uint32_t line;
    /// __func__
    const char* function;
    /// __PRETTY_FUNCTION__
    const char* prettyFunction;
};

/**
 * Constructs a #CodeLocation describing the line from where it is used.
 */
#define HERE \
    RAMCloud::CodeLocation(__FILE__, __LINE__, __func__, __PRETTY_FUNCTION__)


} // end RAMCloud

#endif  // RAMCLOUD_CODELOCATION_H
