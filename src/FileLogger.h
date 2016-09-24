/* Copyright (c) 2016 Stanford University
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

#ifndef RAMCLOUD_FILELOGGER_H
#define RAMCLOUD_FILELOGGER_H

#include <stdio.h>

#include "Common.h"
#include "Logger.h"

namespace RAMCloud {

/**
 * This class creates writable FILE objects that map to the RAMCloud log
 * (i.e. each line written on the FILE becomes one line in the RAMCloud
 * log). This class depends on the fopencookie mechanism.
 */
class FileLogger {
  public:
    FileLogger(LogLevel level, const char* prefix);
    ~FileLogger();
    FILE* getFile();

  PROTECTED:
    static ssize_t write(void *cookie, const char *buf, size_t size);

    /// The FILE whose writes will be logged via this object.
    FILE* f;

    /// Log level to use  for all log messages.
    LogLevel level;

    /// String to prepend to each log message.
    const char* prefix;

    /// Used temporarily to store incomplete lines received by the
    /// write method. Normally empty.
    string partialLine;

    DISALLOW_COPY_AND_ASSIGN(FileLogger);
};

} // namespace RAMCloud

#endif // RAMCLOUD_FILELOGGER_H

