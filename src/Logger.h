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

#include <condition_variable>
#include <thread>
#include <time.h>
#include <unordered_map>

#include "CodeLocation.h"
#include "SpinLock.h"
#include "Tub.h"

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
    void setLogFile(int fd);

    void setLogLevel(LogModule, LogLevel level);
    void setLogLevel(LogModule, int level);
    void setLogLevel(string module, string level);
    void changeLogLevel(LogModule, int delta);
    void reset();
    void sync();

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
    bool addToBuffer(const char* src, int length);
    void cleanCollapseMap(struct timespec now);
    static void printThreadMain(Logger* logger);

    /**
     * Log output gets written to this file descriptor (default is 3, for
     * stderr).
     */
    int fd;

    /**
     * True means that fd came from a file that we opened, so we must
     * eventually close fd when the Logger is destroyed. False means that
     * someone else provided this descriptor, so it's their responsibility
     * to close it.
     */
    bool mustCloseFd;

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
     * all of the duplicates. Keys are pairs consisting of file name and
     * line number where the log message is generated.
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

    // The following variables implement a circular buffer used to store
    // log entries temporarily. A background thread prints out entries in
    // the buffer. This is done because I/O is expensive and potentially
    // unpredictable in speed. It's important not to delay the code that
    // generates log entries, since it could be the dispatcher and delays
    // could cause the server to be considered dead.

    /**
     * Used to synchronize access to buffer metadata.
     */
    SpinLock bufferMutex;

    /**
     * The print thread uses this to sleep when it runs out of log data
     * to print.
     */
    std::condition_variable_any logDataAvailable;

    /**
     * Total number of bytes available in messageBuffer.
     */
    int bufferSize;

    /**
     * Buffer space (dynamically allocated, must be freed).
     */
    char* messageBuffer;

    /**
     * Offset in messageBuffer of the location where the next log message
     * will be stored
     */
    volatile int nextToInsert;

    /**
     * Offset in messageBuffer of the first byte of data that has not yet been
     * printed. Modified only by the printer thread (and this is the only
     * shared variable modified by the printer thread).
     */
    volatile int nextToPrint;

    /**
     * Nonzero means that the most recently generated log entries had to be
     * discarded because we ran out of buffer space (the print thread got
     * behind). The value indicates how many entries were lost.
     */
    int discardedEntries;

    /**
     * This thread is responsible for invoking the (potentially blocking)
     * kernel calls to write out the log.
     */
    Tub<std::thread> printThread;

    /**
     * Set to true to cause the print thread to exit during the Logger
     * destructor.
     */
    bool printThreadExit;

    /**
     * If non-zero, overrides default value for buffer size in logMessage
     * (used for testing).
     */
    int testingBufferSize;

    /**
     * If true (typically only during unit tests) don't wakeup the
     * print thread.
     */
    bool testingNoNotify;

    /**
     * If non-NULL,  points to time information that will be used in all
     * log messages, instead of using the current time. Intended primarily
     * for testing.
     */
    struct timespec* testingLogTime;

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
    Logger::get().sync(); \
    RAMCLOUD_BACKTRACE(RAMCloud::ERROR); \
    throw RAMCloud::FatalError(HERE, \
                               RAMCloud::format(format_, ##__VA_ARGS__)); \
} while (0)

#include "TestLog.h"

#endif  // RAMCLOUD_LOGGER_H
