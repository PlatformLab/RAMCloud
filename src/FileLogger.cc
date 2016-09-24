/* Copyright (c) 2016 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <string.h>
#include "FileLogger.h"
#include "Logger.h"

namespace RAMCloud {

/**
 * Constructor for FileLogger objects.
 *
 * \param level
 *      The log level to use for all messages coming through this object.
 * \param prefix
 *      An additional string to prepend to each log message, typically
 *      used to identify the source of the message.  Must be static.
 */
FileLogger::FileLogger(LogLevel level, const char* prefix)
    : f(NULL)
    , level(level)
    , prefix(prefix)
    , partialLine()
{
    cookie_io_functions_t functions;
    functions.read = NULL;
    functions.write = write;
    functions.seek = NULL;
    functions.close = NULL;
    f = fopencookie(this, "w", functions);
    if (f == NULL) {
        RAMCLOUD_DIE("Couldn't create FileLogger for %s", prefix);
    }
}

/**
 * Destructor for FileLogger objects. After this method is invoked, the
 * associated FILE must never be used  again: it's up to the caller to
 * ensure that all references to the FILE have been neutralized.
 */
FileLogger::~FileLogger()
{
    if (!partialLine.empty()) {
        RAMCLOUD_LOG(level, "%s%s", prefix, partialLine.c_str());
    }
}

/**
 * Returns the FILE object associated with this object, which is writable
 * but not readable.
 */
FILE*
FileLogger::getFile()
{
    return f;
}

/**
 * This function is invoked by the FILE mechanism to write new bytes to the
 * stream.
 *
 * \param cookie
 *      Pointer to the FileLogger object associated with this FILE.
 * \param buf
 *      Where to store new bytes (not used).
 * \param size
 *      Number of bytes of storage available at buf.
 *
 * \return
 *      The number of bytes written (always the same as size).
 */
ssize_t
FileLogger::write(void *cookie, const char *buf, size_t size)
{
    FileLogger* fileLogger = static_cast<FileLogger*>(cookie);
    // Each iteration through the following loop generates one log
    // message for one line of the output data (assuming there is at
    // least one newline character in the data).
    size_t pos = 0;
    while (pos < size) {
        const void* newline = memchr(buf + pos, '\n', size - pos);
        if (newline == NULL) {
            break;
        }
        size_t newlineIndex = (static_cast<const char*>(newline) - buf);
        fileLogger->partialLine.append(buf + pos, newlineIndex - pos);
        RAMCLOUD_LOG(fileLogger->level, "%s%s", fileLogger->prefix,
                fileLogger->partialLine.c_str());
        fileLogger->partialLine.resize(0);
        pos = newlineIndex + 1;
    }

    if (pos < size) {
        fileLogger->partialLine.append(buf + pos, size - pos);
    }
    return size;
}



} // namespace RAMCloud
