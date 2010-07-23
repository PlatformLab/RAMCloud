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

Logger logger(NOTICE);

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
 * Log a message for the system administrator.
 * \param[in] module
 *      The module to which the message pertains.
 * \param[in] level
 *      See #LOG.
 * \param[in] format
 *      See #LOG.
 * \param[in] ...
 *      See #LOG.
 */
void
Logger::logMessage(LogModule module, LogLevel level, const char* format, ...)
{
    // Keep this in sync with the LogLevel enum.
    static const char* logLevelNames[] = {"(none)", "ERROR", "WARNING",
                                          "NOTICE", "DEBUG"};
    static const char* logModuleNames[] = {"default"};
    va_list ap;
    struct timeval now;

    gettimeofday(&now, NULL);
    fprintf(stream, "%010u.%06u %s %s: ",
            now.tv_sec, now.tv_usec,
            logModuleNames[module],
            logLevelNames[level]);

    va_start(ap, format);
    vfprintf(stream, format, ap);
    va_end(ap);

    fflush(stream);
}

} // end RAMCloud

