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

#include <stdarg.h>

#include <thread>

#include "Common.h"
#include "TestLog.h"

namespace RAMCloud {
namespace TestLog {
    namespace {
        typedef std::unique_lock<std::mutex> Lock;
        /**
         * Used to synchronize access to the TestLog for line level
         * atomicity. This symbol is not exported.  It's priority ensures it is
         * initialized before #transportManager.
         */
        /// @cond
        std::mutex mutex __attribute__((init_priority(300)));
        /// @endcond

        /**
         * The current predicate function used to select test log entries.
         * This symbol is not exported.
         */
        bool (*predicate)(string) = 0;

        /**
         * One or more predicate strings. If non-empty, one of the strings
         * must match the function name in a log entry in order for the
         * log entry to be recorded.  This symbol is not exported.
         */
        std::vector<string> predStrings;

        /**
         * Whether test log entries should be recorded.
         * This symbol is not exported.
         */
        bool enabled = false;

        /**
         * The current test log.
         * This symbol is not exported.  It's priority ensures it is initialized
         * before #transportManager.
         */
        /// @cond
        string  __attribute__((init_priority(300))) message;
        /// @endcond
    }

    /// Reset the contents of the test log.
    void
    reset()
    {
        Lock _(mutex);
        message = "";
    }

    /**
     * Reset the test log and quit recording test log entries and
     * remove any predicates that were installed.
     */
    void
    disable()
    {
        Lock _(mutex);
        message = "";
        enabled = false;
        predicate = NULL;
        predStrings.clear();
    }

    /// Reset the test log and begin recording test log entries.
    void
    enable()
    {
        Lock _(mutex);
        message = "";
        enabled = true;
    }

    /**
     * Returns the current test log.
     *
     * \return
     *      The current test log.
     */
    string
    get()
    {
        Lock _(mutex);
        return message;
    }

    /**
     * Returns until the matched position of current test log.
     * If nothing is matched, string with error message is returned.
     * \param[in]  searchPattern
     *      Returning string is
     *      starting at fromPos and ending before the searchPattern.
     *      If this is "" (null string),
     *      all the remaining message is returned.
     * \param[in]  fromPos
     *      Starting position for pattern matching.
     * \param[out] nextPos
     *      Position of 1st character of the matched searchPattern.
     *      This can be used as next 'fromPos'
     **/
    string
    getUntil(const string searchPattern,
               const size_t fromPos,
               size_t* nextPos) {
        Lock _(mutex);
        if (searchPattern.empty()) {
            *nextPos = message.length();
            return message.substr(fromPos, *nextPos - fromPos);
        }
        // skip the first charactor to avoid matching first word again.
        *nextPos = message.find(searchPattern, fromPos + 1);

        if (*nextPos == string::npos) {
            return "Log::getMatched(): Pattern '"
                    + searchPattern
                    + "' not found in "
                    + message.substr(fromPos);
        };
        return message.substr(fromPos, *nextPos - fromPos);
    }

    /**
     * Don't call this directly, see RAMCLOUD_TEST_LOG instead.
     *
     * Log a message to the test log for unit testing.
     *
     * \param[in] where
     *      The result of #HERE.
     * \param[in] format
     *      See #RAMCLOUD_LOG except the string should end with a newline
     *      character.
     * \param[in] ...
     *      See #RAMCLOUD_LOG.
     */
    void
    log(const CodeLocation& where,
        const char* format, ...)
    {
        Lock _(mutex);

        if (!enabled || (predicate && !predicate(where.function)))
            return;

        if (!predStrings.empty()) {
            bool found = false;
            foreach (string& s, predStrings) {
                if (s == where.function) {
                    found = true;
                    break;
                }
            }
            if (!found)
                return;
        }

        if (message.length())
            message += " | ";

        message += RAMCloud::format("%s: ", where.function);

        va_list ap;
        va_start(ap, format);
        message += vformat(format, ap);
        va_end(ap);
    }

    /**
     * Install a predicate to select only the relevant test log entries.
     *
     * \param[in] pred
     *      A predicate which is passed the value of __PRETTY_FUNCTION__
     *      from the RAMCLOUD_TEST_LOG call site.  The predicate should
     *      return true precisely when the test log entry for the
     *      corresponding RAMCLOUD_TEST_LOG invocation should be included
     *      in the test log.
     */
    void
    setPredicate(bool (*pred)(string))
    {
        Lock _(mutex);
        predicate = pred;
    }

    /**
     * Install a single predicate string, replacing any existing
     * predicate strings.
     *
     * \param[in] pred
     *      Log entries will be recorded only if the value of
     *      __PRETTY_FUNCTION__ from blog entry matches this.
     */
    void
    setPredicate(string pred)
    {
        Lock _(mutex);
        predStrings.clear();
        predStrings.push_back(pred);
    }

    /// Reset and enable/disable the test log on construction/destruction.
    Enable::Enable()
    {
        Logger::get().saveLogLevels(savedLogLevels);
        Logger::get().setLogLevels(SILENT_LOG_LEVEL);
        enable();
    }

    /**
     * Reset and enable/disable the test log on construction/destruction
     * using a particular predicate to filter test log entries.
     *
     * \param[in] pred
     *      See setPredicate().
     */
    Enable::Enable(bool (*pred)(string))
    {
        Logger::get().saveLogLevels(savedLogLevels);
        Logger::get().setLogLevels(SILENT_LOG_LEVEL);
        setPredicate(pred);
        enable();
    }

    /**
     * Reset and enable/disable the test log on construction/destruction
     * using a particular predicate string to filter test log entries.
     *
     * \param[in] pred
     *      See setPredicate().
     */
    Enable::Enable(string pred)
    {
        Logger::get().saveLogLevels(savedLogLevels);
        Logger::get().setLogLevels(SILENT_LOG_LEVEL);
        setPredicate(pred);
        enable();
    }

    /**
     * Reset and enable/disable the test log on construction/destruction
     * using a collection of predicate strings to filter log entries.
     * A log entry will be recorded if and only if the value of its
     * __PRETTY_FUNCTION__ is equal to one of the arguments to this
     * constructor.
     *
     * \param[in] pred
     *      First acceptable function name.
     * \param[in] pred2
     *      Next acceptable function name. Additional names may follow
     *      this argument; the argument list must be terminated
     *      by a NULL pointer.
     */
    Enable::Enable(const char* pred, const char* pred2, ...)
    {
        Logger::get().saveLogLevels(savedLogLevels);
        Logger::get().setLogLevels(SILENT_LOG_LEVEL);
        predStrings.clear();
        predStrings.push_back(pred);
        predStrings.push_back(pred2);
        va_list ap;
        va_start(ap, pred2);
        while (1) {
            const char* s = va_arg(ap, const char*);
            if (s == NULL) {
                break;
            }
            predStrings.push_back(s);
        }
        va_end(ap);
        enable();
    }

    /// Reset and disable test logging automatically.
    Enable::~Enable()
    {
        disable();
        Logger::get().restoreLogLevels(savedLogLevels);
    }

} // end RAMCloud::TestLog
} // end RAMCloud
