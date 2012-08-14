/* Copyright (c) 2010-2012 Stanford University
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
#include <execinfo.h>

#include <boost/lexical_cast.hpp>

#include "Logger.h"
#include "ShortMacros.h"
#include "ThreadId.h"
#include "Util.h"

namespace RAMCloud {

Logger* Logger::sharedLogger = NULL;

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

/**
 * Create a new debug logger; messages will go to stderr by default.
 * \param[in] level
 *      Messages for all modules at least as important as \a level will be
 *      logged.
 */
Logger::Logger(LogLevel level)
    : stream(NULL)
    , mutex()
    , collapseMap()
    , collapseIntervalMs(DEFAULT_COLLAPSE_INTERVAL)
    , maxCollapseMapSize(DEFAULT_COLLAPSE_MAP_LIMIT)
    , nextCleanTime({0, 0})
    , collapsingDisableCount(0)
    , testingBufferSize(0)
{
    setLogLevels(level);
}

/**
 * Destructor for debug logs.
 */
Logger::~Logger()
{
    Lock lock(mutex);
    if (stream != NULL)
        fclose(stream);
}

/**
 * Return the singleton shared instance that is normally used for logging.
 */
Logger&
Logger::get()
{
    if (sharedLogger == NULL) {
        sharedLogger = new Logger();
    }
    return *sharedLogger;
}

/**
 * Arrange for future log messages to go to a particular file.
 * \param path
 *      The pathname for the log file, which may or may not exist already.
 * \param truncate
 *      True means the log file should be truncated if it already exists;
 *      false means an existing log file is retained, and new messages are
 *      appended.
 *      
 */
void
Logger::setLogFile(const char* path, bool truncate)
{
    Lock lock(mutex);
    FILE* f = fopen(path, truncate ? "w" : "a");
    if (f == NULL) {
        throw Exception(HERE,
                        format("couldn't open log file '%s'", path),
                        errno);
    }
    if (stream != NULL)
        fclose(stream);
    stream = f;
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
    Lock lock(mutex);
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
    // No lock needed: doesn't access Logger object.
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
    // No lock needed: doesn't access Logger object.
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
    Lock lock(mutex);
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
    // No lock needed: doesn't access Logger object.
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
    // No lock needed: doesn't access Logger object.
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
    // No lock needed: doesn't access Logger object.
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
    // No lock needed: doesn't access Logger object.
    for (int i = 0; i < NUM_LOG_MODULES; i++) {
        LogModule module = static_cast<LogModule>(i);
        changeLogLevel(module, delta);
    }
}

/**
 * Turn off the mechanism that normally suppresses duplicate log messages.
 * Calls to this method nest: for each call here, there must be a
 * corresponding call to \c enableCollapsing before collapsing will be
 * enabled again.
 */
void
Logger::disableCollapsing()
{
    Lock lock(mutex);
    collapsingDisableCount++;
}

/**
 * This method cancels the impact of a previous call to \c disableCollapsing.
 * Once there has been one call here for every call to \c disableCollapsing,
 * collapsing will be re-enabled.
 */
void
Logger::enableCollapsing()
{
    Lock lock(mutex);
    collapsingDisableCount--;
}

/**
 * Log a backtrace for the system administrator.
 * This version doesn't provide C++ name demangling so it can be helpful
 * to run the output of the backtrace through c++filt.
 * \param[in] module
 *      The module to which the message pertains.
 * \param[in] level
 *      See #LOG.
 * \param[in] where
 *      The result of #HERE.
 */
void
Logger::logBacktrace(LogModule module, LogLevel level,
                     const CodeLocation& where)
{
    // No lock needed: doesn't access Logger object.
    const int maxFrames = 128;
    void* retAddrs[maxFrames];
    int frames = backtrace(retAddrs, maxFrames);
    char** symbols = backtrace_symbols(retAddrs, frames);
    if (symbols == NULL) {
        // If the malloc failed we might be able to get the backtrace out
        // to stderr still.
        backtrace_symbols_fd(retAddrs, frames, 2);
        return;
    }

    logMessage(module, level, where, "Backtrace:\n");
    for (int i = 0; i < frames; ++i)
        logMessage(module, level, where, "%s\n", symbols[i]);

    free(symbols);
}

/**
 * Log a message for the system administrator.
 * \param[in] module
 *      The module to which the message pertains.
 * \param[in] level
 *      See #LOG.
 * \param[in] where
 *      The result of #HERE.
 * \param[in] fmt
 *      See #LOG except the string should end with a newline character.
 * \param[in] ...
 *      See #LOG.
 */
void
Logger::logMessage(LogModule module, LogLevel level,
                   const CodeLocation& where,
                   const char* fmt, ...)
{
    Lock lock(mutex);
    static int pid = getpid();
    va_list ap;
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);

    // Compute the body of the log message except for the initial timestamp
    // (i.e. process the original format string, and add location/process/thread
    // info).
    string message = format("%s:%d in %s %s %s[%d:%lu]: ",
            where.relativeFile().c_str(), where.line,
            where.qualifiedFunction().c_str(), logModuleNames[module],
            logLevelNames[level], pid, ThreadId::get());
    char buffer[2000];
    va_start(ap, fmt);
    uint32_t bufferSize = testingBufferSize ? testingBufferSize
            : downCast<uint32_t>(sizeof(buffer));
    uint32_t needed = vsnprintf(buffer, bufferSize, fmt, ap);
    va_end(ap);
    message += buffer;
    if (needed >= bufferSize) {
        // Couldn't quite fit the whole message in our fixed-size buffer.
        // Just truncate the message.
        snprintf(buffer, bufferSize, "... (%d chars truncated)\n",
                (needed + 1 - bufferSize));
        message += buffer;
    }

    if (collapsingDisableCount > 0) {
        printMessage(now, message.c_str(), 0);
        return;
    }

    // Suppress messages that we have printed recently.
    CollapseMap::iterator iter = collapseMap.find(message);
    SkipInfo* skip = NULL;
    if (iter != collapseMap.end()) {
        // We have printed this message before; if it was recently,
        // don't print the current message, but keep track of the fact
        // that we skipped it.
        skip = &iter->second;
        if (Util::timespecLess(now, skip->nextPrintTime)) {
            skip->skipCount++;
            return;
        }

        // We've printed this message before, but it was a while ago,
        // so print the message again.
    } else {
        // Make a new collapseMap entry so we won't print this message
        // again for a while.
        skip = &collapseMap[message];
    }
    skip->nextPrintTime = Util::timespecAdd(now,
            {collapseIntervalMs/1000,
            (collapseIntervalMs%1000)*1000000});

    // Print previously-deferred messages, if needed.
    if (Util::timespecLessEqual(nextCleanTime, now)) {
        cleanCollapseMap(now);
    }
    if (Util::timespecLess(skip->nextPrintTime, nextCleanTime)) {
        nextCleanTime = skip->nextPrintTime;
    }

    // Print the current message.
    printMessage(now, message.c_str(), skip->skipCount);
    skip->skipCount = 0;

    // If collapseMap gets too large, just delete an entry at random.
    if (collapseMap.size() > maxCollapseMapSize) {
        collapseMap.erase(collapseMap.begin());
    }
}

/**
 * This method is called to print delayed log messages. If a bunch of
 * duplicates for the message were suppressed, but no new copies of that
 * message are logged, we eventually want to print out information about
 * the suppressed duplicates. This method does that (and it also removes
 * old entries from \c collapseMap). The caller must hold the Logger lock.
 *
 * \param now
 *      The current time: information will be printed about any suppressed
 *      duplicates with \c nextPrintTime less than or equal to this, or
 *      such entries will be removed from collapseMap if there are no
 *      suppressed duplicates.
 */
void
Logger::cleanCollapseMap(struct timespec now)
{
    nextCleanTime = Util::timespecAdd(now, {1000, 0});
    CollapseMap::iterator iter = collapseMap.begin();
    while (iter != collapseMap.end()) {
        const string* message = &iter->first;
        SkipInfo* skip = &iter->second;
        if (Util::timespecLessEqual(skip->nextPrintTime, now)) {
            // This entry has been around for a while: if there were
            // suppress messages then print one; otherwise delete the
            // entry.
            if (skip->skipCount == 0) {
                // This entry is old and there haven't been any suppressed
                // log messages for it; just delete the entry.
                iter++;
                collapseMap.erase(*message);
                continue;
            }

            // This entry contains suppressed log messages, but it's been
            // a long time since we printed the last one; print another one.

            printMessage(skip->nextPrintTime, message->c_str(),
                    skip->skipCount-1);
            skip->skipCount = 0;
            skip->nextPrintTime = Util::timespecAdd(now,
                    {collapseIntervalMs/1000,
                    (collapseIntervalMs%1000)*1000000});
        }
        if (Util::timespecLess(skip->nextPrintTime, nextCleanTime)) {
            nextCleanTime = skip->nextPrintTime;
        }
        iter++;
    }
}

/**
 * Return the I/O stream to use for log output.
 */
FILE*
Logger::getStream()
{
    if (stream != NULL)
        return stream;
    return stderr;
}

/**
 * Utility method that actually prints messages; eliminates duplicated code.
 * The caller must hold the Logger lock.
 *
 * \param t
 *      Time to be printed in the message.
 * \param message
 *      Main body of the message (everything but the timestamp).
 * \param skipCount
 *      If nonzero, additional message is printed before the main one,
 *      indicating that duplicate messages were suppressed.
 */
void
Logger::printMessage(struct timespec t, const char* message, int skipCount)
{
    FILE* f = getStream();
    if (skipCount > 0) {
        fprintf(f, "%010lu.%09lu (%d duplicates of the following message "
                "were suppressed)\n", t.tv_sec, t.tv_nsec, skipCount);
    }
    fprintf(f, "%010lu.%09lu %s", t.tv_sec, t.tv_nsec, message);
    fflush(f);
}

/**
 * Restore a logger to its default initialized state. Used primarily by tests.
 */
void
Logger::reset()
{
    if (stream != NULL)
        fclose(stream);
    stream = NULL;
    collapseMap.clear();
    collapseIntervalMs = DEFAULT_COLLAPSE_INTERVAL;
    maxCollapseMapSize = DEFAULT_COLLAPSE_MAP_LIMIT;
    collapsingDisableCount = 0;
    testingBufferSize = 0;
}

/**
 * This replaces the default __assert_fail function, which is invoked by the
 * assert macro when there are errors. This function differs from the default
 * __assert_fail function in that it dumps its output to the RAMCloud log file.
 *
 * \param assertion
 *      Textual description of the assertion that failed.
 * \param file
 *      Name of the source file containing the assertion.
 * \param line
 *      Line number of the line containing the assertion, within \a file.
 * \param function
 *      Textual description of the function containing the assertion.
 */
void
Logger::assertionError(const char *assertion, const char *file,
                       unsigned int line, const char *function)
{
    Lock lock(mutex);
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);

    // Compute the body of the log message except for the initial timestamp
    char buffer[strlen(assertion) + 500];
    snprintf(buffer, sizeof(buffer),
            "%s:%d in %s %s %s[%d:%lu]: Assertion `%s' failed.\n",
            file, line, function,
            logModuleNames[RAMCLOUD_CURRENT_LOG_MODULE],
            logLevelNames[ERROR], getpid(), ThreadId::get(), assertion);
    printMessage(now, buffer, 0);
}

} // end RAMCloud

/**
 * This replaces the default __assert_fail function, which is invoked by the
 * assert macro when there are errors. This function differs from the default
 * __assert_fail function in that it dumps its output to the RAMCloud log file.
 *
 * \param assertion
 *      Textual description of the assertion that failed.
 * \param file
 *      Name of the source file containing the assertion.
 * \param line
 *      Line number of the line containing the assertion, within \a file.
 * \param function
 *      Textual description of the function containing the assertion.
 */
void
__assert_fail(const char *assertion, const char *file, unsigned int line,
        const char *function)
{
    RAMCloud::Logger::get().assertionError(assertion, file, line, function);
    abort();
}
