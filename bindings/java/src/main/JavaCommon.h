/* Copyright (c) 2014 Stanford University
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
 * A header file that has some useful utilities for the Java bindings.
 */

#ifndef RAMCLOUD_JAVACOMMON_H
#define RAMCLOUD_JAVACOMMON_H

#include "ByteBuffer.h"

namespace RAMCloud {
    
/**
 * This macro is used to catch C++ exceptions and convert them into Java
 * exceptions. Be sure to wrap the individual RamCloud:: calls in try blocks,
 * rather than the entire methods, since doing so with functions that return
 * non-void is a bad idea with undefined(?) behaviour. 
 *
 * _returnValue is the value that should be returned from the JNI function
 * when an exception is caught and generated in Java. As far as I can tell,
 * the exception fires immediately upon returning from the JNI method. I
 * don't think anything else would make sense, but the JNI docs kind of
 * suck.
 */
#define EXCEPTION_CATCHER(statusBuffer)                                 \
    catch (ClientException& e) {                                        \
        statusBuffer.write(static_cast<uint32_t>(e.status));            \
        return;                                                         \
    }                                                                   \
    statusBuffer.write<uint32_t>(0);

// Time C++ blocks
#define TIME_CPP false

#define check_null(var, msg)                                            \
    if (var == NULL) {                                                  \
        throw Exception(HERE, "RAMCloud: NULL returned: " msg "\n");    \
    }

#define bufferSize 2097152
    
}

#endif // RAMCLOUD_JAVACOMMON_H
