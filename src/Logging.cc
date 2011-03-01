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

#include <stdarg.h>
#include <time.h>

#include <boost/lexical_cast.hpp>
#include <boost/thread/thread.hpp>
#include <boost/unordered_map.hpp>

#include "Logging.h"
#include "Tub.h"

namespace RAMCloud {

namespace TestLog {
    namespace {
        typedef boost::unique_lock<boost::mutex> Lock;
        /**
         * Used to synchronize access to the TestLog for line level
         * atomicity. This symbol is not exported.  It's priority ensures it is
         * initialized before #transportManager.
         */
        /// @cond
        boost::mutex mutex __attribute__((init_priority(300)));
        /// @endcond

        /**
         * The current predicate which is used to select test log entries.
         * This symbol is not exported.
         */
        bool (*predicate)(string) = 0;

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
     * remove any predicate that was installed.
     */
    void
    disable()
    {
        Lock _(mutex);
        message = "";
        enabled = false;
        predicate = NULL;
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
     * Don't call this directly, see TEST_LOG instead.
     *
     * Log a message to the test log for unit testing.
     *
     * \param[in] where
     *      The result of #HERE.
     * \param[in] format
     *      See #LOG except the string should end with a newline character.
     * \param[in] ...
     *      See #LOG.
     */
    void
    log(const CodeLocation& where,
        const char* format, ...)
    {
        Lock _(mutex);
        va_list ap;
        char line[512];

        if (!enabled || (predicate && !predicate(where.function)))
            return;

        if (message.length())
            message += " | ";

        snprintf(line, sizeof(line), "%s: ", where.function);
        message += line;

        va_start(ap, format);
        vsnprintf(line, sizeof(line), format, ap);
        va_end(ap);
        message += line;
    }

    /**
     * Install a predicate to select only the relevant test log entries.
     *
     * \param[in] pred
     *      A predicate which is passed the value of __PRETTY_FUNCTION__
     *      from the TEST_LOG call site.  The predicate should return true
     *      precisely when the test log entry for the corresponding TEST_LOG
     *      invocation should be included in the test log.
     */
    void
    setPredicate(bool (*pred)(string))
    {
        Lock _(mutex);
        predicate = pred;
    }
} // end RAMCloud::TestLog

Logger logger(NOTICE);

/**
 * Friendly names for each #LogLevel value.
 * Keep this in sync with the LogLevel enum.
 */
static const char* logLevelNames[] = {"(none)", "ERROR", "WARNING",
                                      "NOTICE", "DEBUG"};

static_assert(unsafeArrayLength(logLevelNames) == NUM_LOG_LEVELS,
              "logLevelNames size does not match NUM_LOG_LEVELS");
/**
 * Friendly names for each #LogModule value.
 * Keep this in sync with the LogModule enum.
 */
static const char* logModuleNames[] = {"default", "transport"};

static_assert(unsafeArrayLength(logModuleNames) == NUM_LOG_MODULES,
              "logModuleNames size does not match NUM_LOG_MODULES");

namespace {
/// RAII-style POSIX stdio file lock
class FileLocker {
  public:
    explicit FileLocker(FILE* handle)
        : handle(handle) {
        flockfile(handle);
    }
    ~FileLocker() {
        funlockfile(handle);
    }
  private:
    FILE* const handle;
    DISALLOW_COPY_AND_ASSIGN(FileLocker);
};
} // anonymous namespace

/**
 * Create a new debug logger.
 * \param[in] level
 *      Messages for all modules at least as important as \a level will be
 *      logged.
 */
Logger::Logger(LogLevel level) : stream(stderr)
{
    setLogLevels(level);
}

/**
 * Set the log level for a particular module.
 * \param[in] module
 *      The module whose level to set.
 * \param[in] level
 *      Messages for \a module at least as important as \a level will be
 *      logged.
 */
void
Logger::setLogLevel(LogModule module, LogLevel level)
{
    logLevels[module] = level;
}

/**
 * Set the log level for a particular module.
 * \param[in] module
 *      The module whose level to set.
 * \param[in] level
 *      Messages for \a module at least as important as \a level will be
 *      logged. This will be clamped to a valid LogLevel if it is out of range.
 */
void
Logger::setLogLevel(LogModule module, int level)
{
    if (level < 0)
        level = 0;
    else if (level >= NUM_LOG_LEVELS)
        level = NUM_LOG_LEVELS - 1;
    setLogLevel(module, static_cast<LogLevel>(level));
}

/**
 * Set the log level for a particular module.
 * \param[in] module
 *      The module whose level to set.  Given as a string from #logModuleNames.
 * \param[in] level
 *      Messages for \a module at least as important as \a level will be
 *      logged. This will be clamped to a valid LogLevel if it is out of range.
 *      Given as a string from #logLevelNames or as a decimal number indicating
 *      that level's index in the array.
 */
void
Logger::setLogLevel(string module, string level)
{
    int moduleIndex = 0;
    for (; moduleIndex < NUM_LOG_MODULES; ++moduleIndex) {
        if (module == logModuleNames[moduleIndex])
            break;
    }
    if (moduleIndex == NUM_LOG_MODULES) {
        LOG(WARNING, "Ignoring bad log module name: %s", module.c_str());
        return;
    }
    int moduleLevel;
    try {
        moduleLevel = boost::lexical_cast<int>(level);
    } catch (boost::bad_lexical_cast& e) {
        for (moduleLevel = static_cast<int>(ERROR);
             moduleLevel < NUM_LOG_LEVELS;
             ++moduleLevel)
        {
            if (level == logLevelNames[moduleLevel])
                break;
        }
        if (moduleLevel == NUM_LOG_LEVELS) {
            LOG(WARNING, "Ignoring bad log module level: %s", level.c_str());
            return;
        }
    }
    setLogLevel(static_cast<LogModule>(moduleIndex), moduleLevel);
}

/**
 * Change the log level by a relative amount for a particular module.
 * \param[in] module
 *      The module whose level to change.
 * \param[in] delta
 *      The amount (positive or negative) by which to change the log level
 *      of \a module. The resulting level will be clamped to a valid LogLevel
 *      if it is out of range.
 */
void
Logger::changeLogLevel(LogModule module, int delta)
{
    int level = static_cast<int>(logLevels[module]);
    setLogLevel(module, level + delta);
}

/**
 * Set the log level for all modules.
 * \param[in] level
 *      Messages for the modules at least as important as \a level will be
 *      logged.
 */
void
Logger::setLogLevels(LogLevel level)
{
    for (int i = 0; i < NUM_LOG_MODULES; i++) {
        LogModule module = static_cast<LogModule>(i);
        setLogLevel(module, level);
    }
}
/**
 * Set the log level for all modules.
 * \param[in] level
 *      Messages for all modules at least as important as \a level will be
 *      logged. This will be clamped to a valid LogLevel if it is out of range.
 */
void
Logger::setLogLevels(int level)
{
    if (level < 0)
        level = 0;
    else if (level >= NUM_LOG_LEVELS)
        level = NUM_LOG_LEVELS - 1;
    setLogLevels(static_cast<LogLevel>(level));
}
/**
 * Set the log level for all modules.
 * \param[in] level
 *      Messages for all modules at least as important as \a level will be
 *      logged. This will be clamped to a valid LogLevel if it is out of range.
 */
void
Logger::setLogLevels(string level)
{
    int moduleLevel;
    try {
        moduleLevel = boost::lexical_cast<int>(level);
    } catch (boost::bad_lexical_cast& e) {
        for (moduleLevel = static_cast<int>(ERROR);
             moduleLevel < NUM_LOG_LEVELS;
             ++moduleLevel)
        {
            if (level == logLevelNames[moduleLevel])
                break;
        }
        if (moduleLevel == NUM_LOG_LEVELS) {
            LOG(WARNING, "Ignoring bad log module level: %s", level.c_str());
            return;
        }
    }
    setLogLevels(moduleLevel);
}

/**
 * Change the log level by a relative amount for all modules.
 * \param[in] delta
 *      The amount (positive or negative) by which to change the log level
 *      of the modules. The resulting level will be clamped to a valid LogLevel
 *      if it is out of range.
 */
void
Logger::changeLogLevels(int delta)
{
    for (int i = 0; i < NUM_LOG_MODULES; i++) {
        LogModule module = static_cast<LogModule>(i);
        changeLogLevel(module, delta);
    }
}

/**
 * Log a message for the system administrator.
 * \param[in] sourceFile 
 *      The file this logMessage call is associated with.
 *      This can be used to look up the appropriate module.
 * \param[in] level
 *      See #LOG.
 * \param[in] where
 *      The result of #HERE.
 * \param[in] format
 *      See #LOG except the string should end with a newline character.
 * \param[in] ...
 *      See #LOG.
 */
void
Logger::logMessage(string sourceFile, LogLevel level,
                   const CodeLocation& where,
                   const char* format, ...)
{
    static int pid = getpid();
    va_list ap;
    struct timespec now;

    clock_gettime(CLOCK_REALTIME, &now);
    FileLocker _(stream);

    fprintf(stream, "%010lu.%09lu %s:%d in %s %s %s[%d]: ",
            now.tv_sec, now.tv_nsec,
            where.relativeFile().c_str(), where.line,
            where.qualifiedFunction().c_str(),
            logModuleNames[fileToModule(sourceFile)],
            logLevelNames[level],
            pid);

    va_start(ap, format);
    vfprintf(stream, format, ap);
    va_end(ap);

    fflush(stream);
}

/**
 * Return the Module associated with the given filename. The trouble with
 * #define'ing the module name and catching it in the LOG macro is that
 * we must often define in the header file (for logging in inline functions).
 * Unfortunately, doing so in headers can propagate easily throughout the
 * system and before you know it, lots of important messages are considered
 * part of a module you're not expecting and may well be squelching.
 */
LogModule
Logger::fileToModule(string& file)
{
    static Tub<boost::unordered_map<string, LogModule>> lookupTub;

    if (!lookupTub) {
        lookupTub.construct();
        boost::unordered_map<string, LogModule>& lookup = *lookupTub;

        string transportFiles[] = {
            "Driver.cc", "Driver.h", "InfUdDriver.cc", "InfUdDriver.h",
            "MockDriver.cc", "MockDriver.h", "UdpDriver.cc", "UdpDriver.h",
            "BindTransport.cc", "BindTransport.h", "FastTransport.cc",
            "FastTransport.h", "InfRcTransport.cc", "InfRcTransport.h",
            "MockFastTransport.h", "MockTransport.cc", "MockTransport.h",
            "TcpTransport.cc", "TcpTransport.h", "Transport.h",
            "TransportFactory.h", "TransportManager.cc",
            "TransportManager.h"
        };
        for (uint32_t i = 0; i < unsafeArrayLength(transportFiles); i++)
            lookup[transportFiles[i]] = TRANSPORT_MODULE;
    }

    boost::unordered_map<string, LogModule>& lookup = *lookupTub;
    if (lookup.find(file) != lookup.end())
        return lookup[file];

    return DEFAULT_LOG_MODULE;
}

} // end RAMCloud

