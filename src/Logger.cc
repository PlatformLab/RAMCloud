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
#include <execinfo.h>
#include <signal.h>
#include <stdexcept>

#include <boost/lexical_cast.hpp>

#include "Cycles.h"
#include "LogCabinLogger.h"
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
static const char* logModuleNames[] = {
    "default",
    "transport",
    "externalStorage"
};

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
    // Collapsing log messages is usually more trouble than it is worth.
    // It causes log messages to show up out of order and has created more
    // confusion than it has save.
    // The default below is now to run the system without log collapsing
    // unless enableCollapsing() is called first.
    , testingBufferSize(0)
{
    setLogLevels(level);
#if ENABLE_LOGCABIN
    LogCabinLogger::setup(level);
#endif
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
    // No lock needed: doesn't access Logger object.
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

    logMessage(false, module, level, where, "Backtrace:\n");
    for (int i = 0; i < frames; ++i)
        logMessage(false, module, level, where, "%s\n", symbols[i]);

    free(symbols);
}

/**
 * Log a message for the system administrator.
 * \param[in] collapse
 *      Collapse log messages when set.
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
Logger::logMessage(bool collapse, LogModule module, LogLevel level,
                   const CodeLocation& where,
                   const char* fmt, ...)
{
    uint64_t start = Cycles::rdtsc();
    Lock lock(mutex);

    static int pid = getpid();
    va_list ap;
    uint32_t bufferSize;
    uint32_t needed;
    string message;
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);

    // Check for possibility of collapsing before the message is generated at
    // all.
    SkipInfo* skip = NULL;
    if (collapse) {
        auto messageId = std::make_pair(where.file, where.line);
        CollapseMap::iterator iter = collapseMap.find(messageId);
        if (iter != collapseMap.end()) {
            // We have printed this message before; if it was recently,
            // don't print the current message, but keep track of the fact
            // that we skipped it.
            skip = &iter->second;
            if (Util::timespecLess(now, skip->nextPrintTime)) {
                skip->skipCount++;
                goto done;
            }

            // We've printed this message before, but it was a while ago,
            // so print the message again.
        } else {
            skip = &collapseMap[messageId];
        }
    }

    // Compute the body of the log message except for the initial timestamp
    // (i.e. process the original format string, and add location/process/thread
    // info).
    message = format("%s:%d in %s %s %s[%d:%d]: ",
            where.relativeFile().c_str(), where.line,
            where.qualifiedFunction().c_str(), logModuleNames[module],
            logLevelNames[level], pid, ThreadId::get());
    char buffer[2000];
    va_start(ap, fmt);
    bufferSize = testingBufferSize ? testingBufferSize
            : downCast<uint32_t>(sizeof(buffer));
    needed = vsnprintf(buffer, bufferSize, fmt, ap);
    va_end(ap);
    message += buffer;
    if (needed >= bufferSize) {
        // Couldn't quite fit the whole message in our fixed-size buffer.
        // Just truncate the message.
        snprintf(buffer, bufferSize, "... (%d chars truncated)\n",
                (needed + 1 - bufferSize));
        message += buffer;
    }

    if (!collapse) {
        printMessage(now, message.c_str(), 0);
        goto done;
    }

    // If we reach this point of control, then we are collapsing but this is
    // the first time we have seen this CodeLocation, or this entry was
    // previously purged.
    //
    // Make a new collapseMap entry so we won't print this message again for a
    // while.
    skip->message = message;
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

  done:
    // Make sure this method did not take very long to execute.  If the
    // logging gets backed up it is really bad news, because it can lock
    // up the server so that it appears dead and crash recovery happens
    // (e.g., the Dispatch thread might be blocked waiting to log a message).
    // Thus, generate a log message to help people trying to debug the "crash".
    //
    // One way this problem can happen is if logs are being sent back
    // over ssh and there are a lot of log messages. Solution: don't
    // do that! Always log to a local file.
    double elapsedMs = Cycles::toSeconds(Cycles::rdtsc() - start)*1e3;
    if (elapsedMs > 10) {
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        printMessage(now, format("%s:%d in %s default ERROR[%d:%d]: "
                "Logger got stuck for %.1f ms, which could hang server\n",
                HERE.relativeFile().c_str(), HERE.line,
                HERE.qualifiedFunction().c_str(), getpid(), ThreadId::get(),
                elapsedMs).c_str(), 0);
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
        auto messageId = &iter->first;
        SkipInfo* skip = &iter->second;
        if (Util::timespecLessEqual(skip->nextPrintTime, now)) {
            // This entry has been around for a while: if there were
            // suppress messages then print one; otherwise delete the
            // entry.
            if (skip->skipCount == 0) {
                // This entry is old and there haven't been any suppressed
                // log messages for it; just delete the entry.
                iter++;
                collapseMap.erase(*messageId);
                continue;
            }

            // This entry contains suppressed log messages, but it's been
            // a long time since we printed the last one; print another one.

            printMessage(now, skip->message.c_str(),
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
            "%s:%d in %s %s %s[%d:%d]: Assertion `%s' failed.\n",
            file, line, function,
            logModuleNames[RAMCLOUD_CURRENT_LOG_MODULE],
            logLevelNames[ERROR], getpid(), ThreadId::get(), assertion);
    printMessage(now, buffer, 0);
}

/**
 * Signal handler which logs a backtrace and exits.
 *
 * \param signal
 *      Signal number which caused the handler to be invoked.
 * \param info
 *      Details about the cause of the signal. Used to find the
 *      faulting address for segfaults.
 * \param ucontext
 *      CPU context at the time the signal occurred.
 */
static void
criticalErrorHandler(int signal, siginfo_t* info, void* ucontext)
{
    ucontext_t* uc = static_cast<ucontext_t*>(ucontext);
    void* callerAddress =
        reinterpret_cast<void*>(uc->uc_mcontext.gregs[REG_RIP]);

    LOG(ERROR, "Signal %d (%s) at address %p from %p",
        signal, strsignal(signal), info->si_addr,
        callerAddress);

    const int maxFrames = 128;
    void* retAddrs[maxFrames];
    int frames = backtrace(retAddrs, maxFrames);

    // Overwrite sigaction with caller's address.
    retAddrs[1] = callerAddress;

    char** symbols = backtrace_symbols(retAddrs, frames);
    if (symbols == NULL) {
        // If the malloc failed we might be able to get the backtrace out
        // to stderr still.
        backtrace_symbols_fd(retAddrs, frames, 2);
        return;
    }

    LOG(ERROR, "Backtrace:");
    for (int i = 1; i < frames; ++i)
        LOG(ERROR, "%s\n", symbols[i]);

    free(symbols);

    // use abort, rather than exit, to dump core/trap in gdb
    abort();
}

/**
 * Logs a backtrace and exits. For use with std::set_terminate().
 */
static void
terminateHandler()
{
    BACKTRACE(ERROR);

    // use abort, rather than exit, to dump core/trap in gdb
    abort();
}

/**
 * Install handlers for SIGSEGV and std::terminate which log backtraces to
 * the process log file before exiting. This function currently ignores all
 * the thread-safety issues that signals introduce. Since the handlers are
 * only invoked in fatal cases its probably not a big deal.
 */
void
Logger::installCrashBacktraceHandlers()
{
    struct sigaction signalAction;
    signalAction.sa_sigaction = criticalErrorHandler;
    signalAction.sa_flags = SA_RESTART | SA_SIGINFO;

    if (sigaction(SIGSEGV, &signalAction, NULL) != 0)
        LOG(ERROR, "Couldn't set signal handler for SIGSEGV, oh well");

    std::set_terminate(terminateHandler);
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
