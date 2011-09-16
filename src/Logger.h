/* Copyright (c) 2010 Stanford University
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

#ifndef RAMCLOUD_LOGGER_H
#define RAMCLOUD_LOGGER_H

#include "Common.h"

namespace RAMCloud {

class CodeLocation;

/**
 * The levels of verbosity for messages logged with #LOG.
 */
enum LogLevel {
    // Keep this in sync with logLevelNames defined inside _LOG.
    SILENT_LOG_LEVEL = 0,
    /**
     * Bad stuff that shouldn't happen. The system broke its contract to users
     * in some way or some major assumption was violated.
     */
    ERROR,
    /**
     * Messages at the WARNING level indicate that, although something went
     * wrong or something unexpected happened, it was transient and
     * recoverable.
     */
    WARNING,
    /**
     * Somewhere in between WARNING and DEBUG...
     */
    NOTICE,
    /**
     * Messages at the DEBUG level don't necessarily indicate that anything
     * went wrong, but they could be useful in diagnosing problems.
     */
    DEBUG,
    NUM_LOG_LEVELS // must be the last element in the enum
};

enum LogModule {
    DEFAULT_LOG_MODULE = 0,
    TRANSPORT_MODULE,
    NUM_LOG_MODULES // must be the last element in the enum
};

/**
 * This class is used to print informational and error messages to stderr or a
 * file. You'll usually want to use the RAMCLOUD_LOG macro to log messages, but
 * you'll need to access this class to configure the verbosity of the logger
 * and where the log messages should go. To get a pointer to the current Logger
 * instance, use Context::get().logger; see the Context class for more info.
 */
class Logger {
  public:
    explicit Logger(LogLevel level = NOTICE);
    ~Logger();

    void setLogFile(const char* path, bool truncate = false);

    void setLogLevel(LogModule, LogLevel level);
    void setLogLevel(LogModule, int level);
    void setLogLevel(string module, string level);
    void changeLogLevel(LogModule, int delta);

    void setLogLevels(LogLevel level);
    void setLogLevels(int level);
    void setLogLevels(string level);
    void changeLogLevels(int delta);

    void saveLogLevels(LogLevel (&currentLogLevels)[NUM_LOG_MODULES]) {
        std::copy(logLevels, logLevels + NUM_LOG_MODULES, currentLogLevels);
    }

    void restoreLogLevels(const LogLevel (&newLogLevels)[NUM_LOG_MODULES]) {
        std::copy(newLogLevels, newLogLevels + NUM_LOG_MODULES, logLevels);
    }

    void logMessage(LogModule module, LogLevel level,
                    const CodeLocation& where,
                    const char* format, ...)
        __attribute__((format(printf, 5, 6)));

    /**
     * Return whether the current logging configuration includes messages of
     * the given level. This is separate from #RAMCLOUD_LOG in case there's some
     * non-trivial work that goes into calculating a log message, and it's not
     * possible or convenient to include that work as an expression in the
     * argument list to #RAMCLOUD_LOG.
     */
    bool isLogging(LogModule module, LogLevel level) {
        return (level <= logLevels[module]);
    }

  PRIVATE:
    /**
     * The stream on which to log messages.  NULL means use stderr.
     * Note: we don't ever set this value to stderr: if the application
     * changes stderr we want to automatically use the new value.
     */
    FILE* stream;

    /**
     * An array indexed by LogModule where each entry means that, for that
     * module, messages at least as important as the entry's value will be
     * logged.
     */
    LogLevel logLevels[NUM_LOG_MODULES];

    friend class LoggingTest;
    DISALLOW_COPY_AND_ASSIGN(Logger);
};

extern Logger fallbackLogger;

} // end RAMCloud

#define RAMCLOUD_CURRENT_LOG_MODULE RAMCloud::DEFAULT_LOG_MODULE

/**
 * Log a message for the system administrator.
 * The #RAMCLOUD_CURRENT_LOG_MODULE macro should be set to the LogModule to
 * which the message pertains.
 * \param[in] level
 *      The level of importance of the message (LogLevel).
 * \param[in] format
 *      A printf-style format string for the message. It should not have a line
 *      break at the end, as RAMCLOUD_LOG will add one.
 * \param[in] ...
 *      The arguments to the format string.
 */
#define RAMCLOUD_LOG(level, format, ...) do { \
    RAMCloud::Logger& _logger = RAMCloud::Context::isSet() \
            ? *RAMCloud::Context::get().logger \
            : RAMCloud::fallbackLogger; \
    if (_logger.isLogging(RAMCLOUD_CURRENT_LOG_MODULE, level)) { \
        _logger.logMessage(RAMCLOUD_CURRENT_LOG_MODULE, level, HERE, \
                           format "\n", ##__VA_ARGS__); \
    } \
    RAMCLOUD_TEST_LOG(format, ##__VA_ARGS__); \
} while (0)

/**
 * Log an ERROR message and throw a #RAMCloud::FatalError.
 * The #RAMCLOUD_CURRENT_LOG_MODULE macro should be set to the LogModule to
 * which the message pertains.
 * \param[in] format_
 *      See #RAMCLOUD_LOG().
 * \param[in] ...
 *      See #RAMCLOUD_LOG().
 * \throw FatalError
 *      Always thrown.
 */
#define RAMCLOUD_DIE(format_, ...) do { \
    RAMCLOUD_LOG(RAMCloud::ERROR, format_, ##__VA_ARGS__); \
    throw RAMCloud::FatalError(HERE, \
                               RAMCloud::format(format_, ##__VA_ARGS__)); \
} while (0)

#include "TestLog.h"

#endif  // RAMCLOUD_LOGGER_H
