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

#ifndef RAMCLOUD_TESTLOG_H
#define RAMCLOUD_TESTLOG_H

#include "Logger.h"

namespace RAMCloud {

class CodeLocation;

/**
 * A module for capturing "test log entries" to facilitate unit testing.
 *
 * RAMCLOUD_TEST_LOG calls can be removed by disabling TESTING.  Further, test
 * logging can be run time toggled using enable() and disable() to prevent unit
 * tests which aren't interested in the test log from accumulating the log in
 * RAM.
 *
 * The easiest interface is to simply instantiate Enable at the beginning of a
 * test method.
 *
 * Example:
 * \code
 * void
 * FooClass::methodToTest()
 * {
 *     RAMCLOUD_TEST_LOG("log message");
 * }
 *
 * void
 * test()
 * {
 *     TestLog::Enable _;
 *
 *     foo->methodName();
 *
 *     EXPECT_EQ(
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
        __attribute__((format(gnu_printf, 2, 3)));
    void setPredicate(bool (*pred)(string));
    void setPredicate(string pred);
    string getUntil(const string searchPattern,
                    const size_t fromPos, size_t* nextPos);
    /**
     * Reset and enable the test log on construction, reset and disable it
     * on destruction.
     *
     * Allows one to instrument a function in an exception safe way with
     * test logging just by sticking one of these on the stack.
     */
    class Enable {
      public:
        Enable();
        explicit Enable(bool (*pred)(string));
        explicit Enable(string pred);
        Enable(const char* pred, const char* pred2, ...);
        ~Enable();
      private:
        LogLevel savedLogLevels[NUM_LOG_MODULES];
    };
} // namespace RAMCloud::TestLog

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
#define RAMCLOUD_TEST_LOG(format, ...) \
    RAMCloud::TestLog::log(HERE, format, ##__VA_ARGS__)
#else
#define RAMCLOUD_TEST_LOG(format, ...)
#endif

} // namespace RAMCloud

#endif // RAMCLOUD_TESTLOG_H
