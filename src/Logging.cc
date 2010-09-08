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

/**
 * \file
 * Implementation for debug logs.
 */

#include <stdarg.h>
#include <sys/time.h>
#include <Logging.h>

namespace RAMCloud {

namespace TestLog {
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
     * This symbol is not exported.
     */
    string message;

    /// Clear the contents of the test log.
    void
    clear()
    {
        message = "";
    }

    /**
     * Clear the test log and quit recording test log entries and
     * remove any predicate that was installed.
     */
    void
    disable()
    {
        clear();
        enabled = false;
        predicate = NULL;
    }

    /// Clear the test log and begin recording test log entries.
    void
    enable()
    {
        clear();
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
        return message;
    }

    /**
     * Don't call this directly, see TEST_LOG instead.
     *
     * Log a message to the test log for unit testing.
     *
     * \param[in] func
     *      The result of __PRETTY_FUNCTION__.
     * \param[in] format
     *      See #LOG except the string should end with a newline character.
     * \param[in] ...
     *      See #LOG.
     */
    void
    log(const char* func,
        const char* format, ...)
    {
        va_list ap;
        char line[512];

        if (predicate && !predicate(func))
            return;

        if (message.length())
            message += " | ";

        snprintf(line, sizeof(line), "%s: ", func);
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

/**
 * Friendly names for each #LogModule value.
 * Keep this in sync with the LogModule enum.
 */
static const char* logModuleNames[] = {"default"};

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
 * Return the number of characters of __FILE__ that make up the path prefix.
 * That is, __FILE__ plus this value will be the relative path from the top
 * directory of the RAMCloud repo.
 */
static int
length__FILE__Prefix()
{
    const char* start = __FILE__;
    const char* match = strstr(__FILE__, "src/Logging.cc");
    assert(match != NULL);
    return (match - start);
}

/**
 * Log a message for the system administrator.
 * \param[in] module
 *      The module to which the message pertains.
 * \param[in] level
 *      See #LOG.
 * \param[in] file
 *      The result of __FILE__.
 * \param[in] line
 *      The result of __LINE__.
 * \param[in] format
 *      See #LOG except the string should end with a newline character.
 * \param[in] ...
 *      See #LOG.
 */
void
Logger::logMessage(LogModule module, LogLevel level,
                   const char* file, uint32_t line,
                   const char* format, ...)
{
    static int fileCharsToSkip = length__FILE__Prefix();

    va_list ap;
    struct timeval now;

    gettimeofday(&now, NULL);
    fprintf(stream, "%010u.%06u %s:%d %s %s: ",
            now.tv_sec, now.tv_usec,
            file + fileCharsToSkip, line,
            logModuleNames[module],
            logLevelNames[level]);

    va_start(ap, format);
    vfprintf(stream, format, ap);
    va_end(ap);

    fflush(stream);
}

/**
 * Format a message for use in #DIE().
 * \param[in] module
 *      See #logMessage().
 * \param[in] level
 *      See #logMessage().
 * \param[in] file
 *      See #logMessage().
 * \param[in] line
 *      See #logMessage().
 * \param[in] format
 *      See #logMessage().
 * \param[in] ...
 *      See #logMessage().
 * \return
 *      A string containing the formatted message.
 */
std::string
Logger::getMessage(LogModule module, LogLevel level,
                   const char* file, uint32_t line,
                   const char* format, ...)
{
    static int fileCharsToSkip = length__FILE__Prefix();
    std::string message;
    char buf[1024];
    va_list ap;

    snprintf(buf, sizeof(buf), "%s:%d %s %s: ",
             file + fileCharsToSkip, line,
             logModuleNames[module],
             logLevelNames[level]);
    message.append(buf);

    va_start(ap, format);
    vsnprintf(buf, sizeof(buf), format, ap);
    va_end(ap);
    message.append(buf);

    return message;
}

} // end RAMCloud

