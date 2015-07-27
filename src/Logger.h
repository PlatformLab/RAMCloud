/* Copyright (c) 2010-2015 Stanford University
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

#include <time.h>
#include <mutex>
#include <unordered_map>

#include "CodeLocation.h"
#include "SpinLock.h"


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
    EXTERNAL_STORAGE_MODULE,
    NUM_LOG_MODULES // must be the last element in the enum
};

/**
 * This class is used to print informational and error messages to stderr or a
 * file. You'll usually want to use the RAMCLOUD_LOG macro to log messages, but
 * you'll need to access this class to configure the verbosity of the logger
 * and where the log messages should go.
 *
 * Note: this class is thread-safe.
 */
class Logger {
  public:
    explicit Logger(LogLevel level = WARNING);
    ~Logger();
    static Logger& get();

    void setLogFile(const char* path, bool truncate = false);

    void setLogLevel(LogModule, LogLevel level);
    void setLogLevel(LogModule, int level);
    void setLogLevel(string module, string level);
    void changeLogLevel(LogModule, int delta);
    void reset();

    void setLogLevels(LogLevel level);
    void setLogLevels(int level);
    void setLogLevels(string level);
    void changeLogLevels(int delta);
    void assertionError(const char *assertion, const char *file,
                        unsigned int line, const char *function);

    void saveLogLevels(LogLevel (&currentLogLevels)[NUM_LOG_MODULES]) {
        std::copy(logLevels, logLevels + NUM_LOG_MODULES, currentLogLevels);
    }

    void restoreLogLevels(const LogLevel (&newLogLevels)[NUM_LOG_MODULES]) {
        std::copy(newLogLevels, newLogLevels + NUM_LOG_MODULES, logLevels);
    }

    class pairHash {
    public:
        size_t operator()(const std::pair<const char*, int > &key) const{
            return reinterpret_cast<size_t>(key.first) ^
                static_cast<size_t>(key.second);
        }
    };

    void logBacktrace(LogModule module, LogLevel level,
                      const CodeLocation& where);
    void logMessage(bool collapse, LogModule module, LogLevel level,
                    const CodeLocation& where,
                    const char* format, ...)
        __attribute__((format(printf, 6, 7)));

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

    static void installCrashBacktraceHandlers();

  PRIVATE:
    void cleanCollapseMap(struct timespec now);
    FILE* getStream();
    void printMessage(struct timespec t, const char* message, int skipCount);

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

    /**
     * Used for monitor-style locking so the Logger is thread-safe.
     * Note: this used to be a std::recursive_mutex, but this resulted
     * in bad behavior under contention (if one thread is logging
     * continuously, others can get locked out for 100ms or more, which
     * caused server pings to timeout).
     */
    SpinLock mutex;
    typedef std::unique_lock<SpinLock> Lock;

    /**
     * Singleton global logger that will be returned by Logger::get.
     */
    static Logger* sharedLogger;

    /**
     * Objects of the following type are used in collapseMap to keep
     * track of recently logged entries so that duplicates can be
     * collapsed.
     */
    struct SkipInfo {
        /**
         * Don't print this log message again until the time given below.
         */
        struct timespec nextPrintTime;

        /**
         * Number of times we have skipped printing this message
         * because nextPrintTime hadn't yet been reached.
         */
        int skipCount;
        std::string message;

        SkipInfo(struct timespec nextPrintTime, int skipCount,
                std::string message)
            : nextPrintTime(nextPrintTime), skipCount(skipCount),
              message(message) {}
        SkipInfo()
            : nextPrintTime({0, 0}), skipCount(0), message() {}
    };

    /**
     * This object is used for log message collapsing: if the same message
     * is output repeatedly in a short time window, the first message is
     * printed immediately, but duplicates are not printed. Information about
     * that is recorded here, and eventually we print a single message for
     * all of the duplicates. Keys are log messages (everything except the
     * time part).
     */
    typedef std::unordered_map<std::pair<const char*, int>, SkipInfo, pairHash>
        CollapseMap;
    CollapseMap collapseMap;

    /**
     * This variable determines the interval over which log message collapsing
     * is done: once a given message has been printed, the same message will
     * not be printed again for this many milliseconds (this value is normally
     * constant, but can be modified for testing).
     */
    uint32_t collapseIntervalMs;

    /**
     * This is the value of collapseIntervalMs except during testing.
     */
    static const uint32_t DEFAULT_COLLAPSE_INTERVAL = 5000;

    /**
     * Don't retain more than this many entries in collapseMap at a time:
     * this is to limit memory usage if an application should generate a large
     * number of unique log entries in a short time interval.
     */
    uint32_t maxCollapseMapSize;

    /**
     * This is the value of maxCollapseMapSize except during testing.
     */
    static const uint32_t DEFAULT_COLLAPSE_MAP_LIMIT = 1000;

    /**
     * Call cleanCollapseMap when this time is reached (this is the smallest
     * nextPrintTime for any entry in the table, or a large time if the
     * table is empty).
     */
    struct timespec nextCleanTime;

    /**
     * If non-zero, overrides default value for buffer size in logMessage
     * (used for testing).
     */
    uint32_t testingBufferSize;

    DISALLOW_COPY_AND_ASSIGN(Logger);
};


} // end RAMCloud

#define RAMCLOUD_CURRENT_LOG_MODULE RAMCloud::DEFAULT_LOG_MODULE

/**
 * Log a backtrace for the system administrator.
 * The #RAMCLOUD_CURRENT_LOG_MODULE macro should be set to the LogModule to
 * which the message pertains.
 * \param[in] level
 *      The level of importance of the message (LogLevel).
 */
#define RAMCLOUD_BACKTRACE(level) do { \
    RAMCloud::Logger& _logger = Logger::get(); \
    if (_logger.isLogging(RAMCLOUD_CURRENT_LOG_MODULE, level)) { \
        _logger.logBacktrace(RAMCLOUD_CURRENT_LOG_MODULE, level, HERE); \
    } \
} while (0)

/**
 * Log a message for the system administrator with (CLOG) or without (LOG)
 * collapsing frequent messages.
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
    RAMCloud::Logger& _logger = Logger::get(); \
    if (_logger.isLogging(RAMCLOUD_CURRENT_LOG_MODULE, level)) { \
        _logger.logMessage(false, RAMCLOUD_CURRENT_LOG_MODULE, level, HERE, \
                           format "\n", ##__VA_ARGS__); \
    } \
    RAMCLOUD_TEST_LOG(format, ##__VA_ARGS__); \
} while (0)

#define RAMCLOUD_CLOG(level, format, ...) do { \
    RAMCloud::Logger& _logger = Logger::get(); \
    if (_logger.isLogging(RAMCLOUD_CURRENT_LOG_MODULE, level)) { \
        _logger.logMessage(true, RAMCLOUD_CURRENT_LOG_MODULE, level, HERE, \
                           format "\n", ##__VA_ARGS__); \
    } \
    RAMCLOUD_TEST_LOG(format, ##__VA_ARGS__); \
} while (0)

/**
 * Log an ERROR message, dump a backtrace, and throw a #RAMCloud::FatalError.
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
    RAMCLOUD_BACKTRACE(RAMCloud::ERROR); \
    throw RAMCloud::FatalError(HERE, \
                               RAMCloud::format(format_, ##__VA_ARGS__)); \
} while (0)

#include "TestLog.h"

#endif  // RAMCLOUD_LOGGER_H
