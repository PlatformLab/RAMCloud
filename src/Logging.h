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

#ifndef RAMCLOUD_LOGGING_H
#define RAMCLOUD_LOGGING_H

#include "Common.h"

namespace RAMCloud {

class CodeLocation;

/**
 * A module for capturing "test log entries" to facilitate unit testing.
 *
 * TEST_LOG calls can be removed by disabling TESTING.  Further, test logging
 * can be run time toggled using enable() and disable() to prevent unit tests
 * which aren't interested in the test log from accumulating the log in RAM.
 *
 * The easiest interface is to simply instantiate Enable at the beginning of a
 * test method.
 *
 * Example:
 * \code
 * void
 * FooClass::methodToTest()
 * {
 *     TEST_LOG("log message");
 * }
 *
 * void
 * test()
 * {
 *     TestLog::Enable _;
 *
 *     foo->methodName();
 *
 *     CPPUNIT_ASSERT_EQUAL(
 *         "void RAMCloud::FooClass:methodToTest(): log message",
 *         TestLog::get());
 * }
 * \endcode
 *
 * TEST_LOG calls in deeper calls may be irrelevant to a specific unit test
 * so a predicate can be specified using setPredicate or as an argument to
 * the constructor of Enable to select only interesting test log entries.
 * See setPredicate() for more detail.
 */
namespace TestLog {
    void reset();
    void disable();
    void enable();
    string get();
    void log(const CodeLocation& where, const char* format, ...)
#ifdef __INTEL_COMPILER
        __attribute__((format(printf, 2, 3)));
#else
        __attribute__((format(gnu_printf, 2, 3)));
#endif
    void setPredicate(bool (*pred)(string));

    /**
     * Reset and enable the test log on construction, reset and disable it
     * on destruction.
     *
     * Allows one to instrument a function in an exception safe way with
     * test logging just by sticking one of these on the stack.
     */
    struct Enable {
        /// Reset and enable/disable the test log on construction/destruction.
        Enable()
        {
            enable();
        }

        /**
         * Reset and enable/disable the test log on construction/destruction
         * using a particular predicate to filter test log entries.
         *
         * \param[in] pred
         *      See setPredicate().
         */
        Enable(bool (*pred)(string))
        {
            setPredicate(pred);
            enable();
        }

        /// Reset and disable test logging automatically.
        ~Enable()
        {
            disable();
        }
    };
} // namespace RAMCloud::TestLog

} // namespace RAMCloud
#if TESTING
/**
 * Log an entry in the test log for use in unit tests.
 *
 * See RAMCloud::TestLog for examples on how to use this for testing.
 *
 * \param[in] format
 *      A printf-style format string for the message. It should not have a line
 *      break at the end.
 * \param[in] ...
 *      The arguments to the format string.
 */
#define TEST_LOG(format, ...) \
    RAMCloud::TestLog::log(HERE, format, ##__VA_ARGS__)
#else
#define TEST_LOG(format, ...)
#endif

namespace RAMCloud {

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

class Logger {
  public:
    explicit Logger(LogLevel level);

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
     * the given level. This is separate from #LOG in case there's some
     * non-trivial work that goes into calculating a log message, and it's not
     * possible or convenient to include that work as an expression in the
     * argument list to #LOG.
     */
    bool isLogging(LogModule module, LogLevel level) {
        return (level <= logLevels[module]);
    }

  private:
    /**
     * The stream on which to log messages.
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

extern Logger logger;

} // end RAMCloud

#define CURRENT_LOG_MODULE RAMCloud::DEFAULT_LOG_MODULE

/**
 * Log a message for the system administrator.
 * The #CURRENT_LOG_MODULE macro should be set to the LogModule to which the
 * message pertains.
 * \param[in] level
 *      The level of importance of the message (LogLevel).
 * \param[in] format
 *      A printf-style format string for the message. It should not have a line
 *      break at the end, as LOG will add one.
 * \param[in] ...
 *      The arguments to the format string.
 */
#define LOG(level, format, ...) do { \
    if (RAMCloud::logger.isLogging(CURRENT_LOG_MODULE, level)) \
        RAMCloud::logger.logMessage(CURRENT_LOG_MODULE, level, HERE, \
                                    format "\n", ##__VA_ARGS__); \
    TEST_LOG(format, ##__VA_ARGS__); \
} while (0)

/**
 * Log an ERROR message and throw a #RAMCloud::FatalError.
 * The #CURRENT_LOG_MODULE macro should be set to the LogModule to which the
 * message pertains.
 * \param[in] format_
 *      See #LOG().
 * \param[in] ...
 *      See #LOG().
 * \throw FatalError
 *      Always thrown.
 */
#define DIE(format_, ...) do { \
    LOG(RAMCloud::ERROR, format_, ##__VA_ARGS__); \
    throw RAMCloud::FatalError(HERE, \
                               RAMCloud::format(format_, ##__VA_ARGS__)); \
} while (0)

#endif  // RAMCLOUD_LOGGING_H
