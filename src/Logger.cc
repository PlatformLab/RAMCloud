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

#include <fcntl.h>
#include <stdarg.h>
#include <execinfo.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdexcept>

#include <boost/lexical_cast.hpp>

#include "Cycles.h"
#include "LogCabinLogger.h"
#include "Logger.h"
#include "ShortMacros.h"
#include "ThreadId.h"
#include "TimeTrace.h"
#include "Unlock.h"
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
    : fd(2)
    , mustCloseFd(false)
    , mutex()
    , collapseMap()
    , collapseIntervalMs(DEFAULT_COLLAPSE_INTERVAL)
    , maxCollapseMapSize(DEFAULT_COLLAPSE_MAP_LIMIT)
    , nextCleanTime({0, 0})
    , bufferMutex()
    , logDataAvailable()
    , bufferSize(1000000)
    , messageBuffer()
    , nextToInsert(0)
    , nextToPrint(0)
    , discardedEntries(0)
    , printThread()
    , printThreadExit(false)
    , testingBufferSize(0)
    , testingNoNotify(false)
    , testingLogTime(NULL)
{
    setLogLevels(level);
    messageBuffer = new char[bufferSize];

    // Touch every page in the buffer to make sure that memory has been
    // allocated (don't want to take page faults during log operations).
    for (int i = 0; i < bufferSize; i += 1000) {
        messageBuffer[i] = 'x';
    }

    // Start the print thread.
    printThread.construct(printThreadMain, this);

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

    // Exit the print thread.
    {
        Lock bufferLock(bufferMutex);
        printThreadExit = true;
        logDataAvailable.notify_one();
    }
    printThread->join();

    if (mustCloseFd)
        close(fd);
    delete messageBuffer;
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
    int newFd = open(path, O_CREAT | O_WRONLY | (truncate ? O_TRUNC : 0),
            0666);
    if (newFd < 0) {
        throw Exception(HERE,
                        format("couldn't open log file '%s'", path),
                        errno);
    }
    lseek(newFd, 0, SEEK_END);
    if (mustCloseFd)
        close(fd);
    fd = newFd;
    mustCloseFd = true;
}

/**
 * Arrange for future log messages to go to a particular file descriptor.
 * \param newFd
 *      The file descriptor for the log file, which must already have
 *      been opened by the caller. This file descriptor belongs to the caller,
 *      meaning that we will never close it.
 */
void
Logger::setLogFile(int newFd)
{
    if (mustCloseFd)
        close(fd);
    fd = newFd;
    mustCloseFd = false;
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
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
#ifdef TESTING
    if (testingLogTime != NULL) {
        now = *testingLogTime;
    }
#endif
    Lock lock(mutex);

    // See if this log message should be collapsed away entirely.
    SkipInfo* skip = NULL;
    int skipCount = 0;
    if (collapse) {
        auto messageId = std::make_pair(where.file, where.line);
        CollapseMap::iterator iter = collapseMap.find(messageId);
        if (iter != collapseMap.end()) {
            // We have printed this message before; if it was recent,
            // don't print the current message, but keep track of the fact
            // that we skipped it.
            skip = &iter->second;
            if (Util::timespecLess(now, skip->nextPrintTime)) {
                skip->skipCount++;
                return;
            }

            // We've printed this message before, but it was a while ago,
            // so print the message again.
            skipCount = skip->skipCount;
        } else {
            // Initialize a new entry in the map.
            if (collapseMap.size() >= maxCollapseMapSize) {
                // The map has gotten too large; just delete an entry
                // at random.
                collapseMap.erase(collapseMap.begin());
            }
            skip = &collapseMap[messageId];
        }
    }

#define MAX_MESSAGE_CHARS 2000
    // Extra space for a message about truncated characters, if needed.
#define TRUNC_MESSAGE_SPACE 50
    char buffer[MAX_MESSAGE_CHARS + TRUNC_MESSAGE_SPACE];
    int spaceLeft = MAX_MESSAGE_CHARS;
    if (testingBufferSize != 0) {
        spaceLeft = testingBufferSize;
    }
    int charsLost = 0;
    int charsWritten = 0;
    int actual;

    // Generate a message about discarded entries, if relevant.
    if (discardedEntries > 0) {
        CodeLocation here = HERE;
        actual = snprintf(buffer, spaceLeft,
                "%010lu.%09lu %s:%d in %s %s[%d]: %d log messages "
                "lost because of buffer overflow\n",
                now.tv_sec, now.tv_nsec, here.baseFileName(),
                here.line, here.function, logLevelNames[WARNING],
                ThreadId::get(), discardedEntries);
        if (actual >= spaceLeft) {
            // We ran out of space in the buffer (should never happen here).
            charsLost += 1 + actual - spaceLeft;
            actual = spaceLeft - 1;
        }
        charsWritten += actual;
        spaceLeft -= actual;
    }

    // Create the new log message in a local buffer. First write a standard
    // prefix containing timestamp, information about source file, etc.
    actual = snprintf(buffer+charsWritten, spaceLeft,
            "%010lu.%09lu %s:%d in %s %s[%d]: ",
            now.tv_sec, now.tv_nsec, where.baseFileName(), where.line,
            where.function, logLevelNames[level],
            ThreadId::get());
    if (actual >= spaceLeft) {
        // We ran out of space in the buffer (should never happen here).
        charsLost += 1 + actual - spaceLeft;
        actual = spaceLeft - 1;
    }
    charsWritten += actual;
    spaceLeft -= actual;

    // Next, add info about skipped entries, if relevant.
    if (skipCount > 0) {
        actual = snprintf(buffer + charsWritten, spaceLeft,
                "(%d duplicates of this message were skipped) ", skipCount);
        if (actual >= spaceLeft) {
            // We ran out of space in the buffer (should never happen here).
            charsLost += 1 + actual - spaceLeft;
            actual = spaceLeft - 1;
        }
        charsWritten += actual;
        spaceLeft -= actual;
    }

    // Last, add the caller's log message.
    va_list ap;
    va_start(ap, fmt);
    actual = vsnprintf(buffer + charsWritten, spaceLeft, fmt, ap);
    va_end(ap);
    if (actual >= spaceLeft) {
        // We ran out of space in the buffer.
        charsLost += 1 + actual - spaceLeft;
        actual = spaceLeft - 1;
    }
    charsWritten += actual;
    spaceLeft -= actual;

    if (charsLost > 0) {
        // Ran out of space: add a note about the lost info.
        charsWritten += snprintf(buffer + charsWritten, TRUNC_MESSAGE_SPACE,
                "... (%d chars truncated)\n", charsLost);
    }

    if (addToBuffer(buffer, charsWritten)) {
        discardedEntries = 0;
    } else {
        discardedEntries++;
    }

    if (collapse) {
        // If we reach this point of control, then we have just printed a
        // collapsible message (either it's new or  we haven't printed it in
        // a while). Save information so we don't print this message again
        // for a while.
        skip->message.assign(buffer, charsWritten);
        skip->nextPrintTime = Util::timespecAdd(now,
                {collapseIntervalMs/1000,
                (collapseIntervalMs%1000)*1000000});
        if (Util::timespecLess(skip->nextPrintTime, nextCleanTime)) {
            nextCleanTime = skip->nextPrintTime;
        }
    }

    // Print previously-deferred messages, if needed.
    if (Util::timespecLessEqual(nextCleanTime, now)) {
        cleanCollapseMap(now);
    }

    // Make sure this method did not take very long to execute.  If the
    // logging gets backed up it is really bad news, because it can lock
    // up the server so that it appears dead and crash recovery happens
    // (e.g., the Dispatch thread might be blocked waiting to log a message).
    // Thus, generate a log message to help people trying to debug the "crash".
    double elapsedMs = Cycles::toSeconds(Cycles::rdtsc() - start)*1e3;
    if (elapsedMs > 10) {
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        CodeLocation here = HERE;
        snprintf(buffer, sizeof(buffer), "%010lu.%09lu %s:%d in %s "
                "ERROR[%d]: Logger got stuck for %.1f ms, which could "
                "hang server\n",
                now.tv_sec, now.tv_nsec, here.baseFileName(),
                here.line, here.function, ThreadId::get(),
                elapsedMs);
        addToBuffer(buffer, downCast<int>(strlen(buffer)));
    }
}

/**
 * This method is called to print delayed log messages. If a bunch of
 * duplicates for a message were suppressed, but no new copies of that
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
    // We won't check again for a very long time, unless there are
    // entries in the collapse map (see below).
    nextCleanTime = Util::timespecAdd(now, {1000, 0});
    CollapseMap::iterator iter = collapseMap.begin();
    while (iter != collapseMap.end()) {
        auto messageId = &iter->first;
        SkipInfo* skip = &iter->second;
        if (Util::timespecLessEqual(skip->nextPrintTime, now)) {
            // This entry has been around for a while: if there were
            // suppressed messages then print one; otherwise delete the
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
            // (splice a message about skipped messages into the log message).
            size_t i = skip->message.find("]:");
            assert(i > 20);
            string newMessage = format("%010lu.%09lu", now.tv_sec, now.tv_nsec)
                    + skip->message.substr(20, i+2-20)
                    + format(" (%d duplicates of this message were skipped)",
                    skip->skipCount-1)
                    + skip->message.substr(i+2);
            if (addToBuffer(newMessage.c_str(),
                    downCast<int>(newMessage.size()))) {
                skip->skipCount = 0;
            }
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
 * This method copies a block of data into the printBuffer and wakes up
 * the print thread to output it.
 * 
 * \param src
 *      First byte in block of data to add to the buffer.
 * \param length
 *      Total number of bytes of data to add.  Must be > 0.
 * 
 * \return
 *      The return value is true if the data was successfully added, and false
 *      if there wasn't enough space for all of it (in which case none is
 *      added).
 */
bool
Logger::addToBuffer(const char* src, int length)
{
    Lock lock(bufferMutex);

    // First, write data at the end of the buffer, if there's space there.
    if (nextToInsert >= nextToPrint) {
        int count = std::min(bufferSize - nextToInsert, length);
        memcpy(messageBuffer + nextToInsert, src, count);
        src += count;
        nextToInsert += count;
        if (nextToInsert == bufferSize) {
            nextToInsert = 0;
        }
        length -= count;
    }

    if (length > 0) {
        // We get here if the space at the end of the buffer was all
        // occupied, or if that wasn't enough to hold all of the data.
        // See if there's space at the beginning of the buffer. Note: we
        // can't use the last byte before nextToPrint; if we do, we'll
        // wrap around and lose all the buffered data.
        if (length > (nextToPrint - 1 - nextToInsert)) {
            return false;
        }
        memcpy(messageBuffer + nextToInsert, src, length);
        nextToInsert += length;
    }

    if (!testingNoNotify) {
        logDataAvailable.notify_one();
    }
    return true;
}

/**
 * This method is the main program for a separate thread that runs in the
 * background to print log messages to secondary storage (this way, the
 * main RAMCloud threads aren't delayed for I/O).
 * 
 * \param owner
 *      The owning Logger. This thread accesses only information related
 *      to the buffer and the output file.
 */
void
Logger::printThreadMain(Logger* logger)
{
    Lock lock(logger->bufferMutex);
    int bytesToPrint;
    ssize_t bytesPrinted;
    while (true) {
        if (logger-> printThreadExit) {
            TEST_LOG("print thread exiting");
            return;
        }

        // Handle buffer wraparound.
        if (logger->nextToPrint >= logger->bufferSize) {
            logger->nextToPrint = 0;
        }

        // See if there is new log data to print.
        bytesToPrint = logger->nextToInsert - logger->nextToPrint;
        if (bytesToPrint < 0) {
            // If we get here, it means that nextToInsert has wrapped back
            // to the start of the buffer.
            bytesToPrint = logger->bufferSize - logger->nextToPrint;
        }
        if (bytesToPrint == 0) {
            // The line below is not needed for correctness, but it
            // results in better cache performance: as long as the printer
            // keeps up, only the first part of the buffer will be used,
            // and the later parts will never be touched.
            logger->nextToPrint = logger->nextToInsert = 0;
            logger->logDataAvailable.wait(lock);
            continue;
        }

        // Print whatever data is available (be careful not to hold the
        // buffer lock during expensive kernel calls, since this will
        // block calls to logMessage).
        {
            Unlock<SpinLock> unlock(logger->bufferMutex);
            bytesPrinted = write(logger->fd,
                    logger->messageBuffer + logger->nextToPrint,
                    bytesToPrint);
        }
        if (bytesPrinted < 0) {
            fprintf(stderr, "Error writing log: %s\n", strerror(errno));

            // Skip these bytes; otherwise we'll loop infinitely.
            logger->nextToPrint += downCast<int>(bytesToPrint);
        } else {
            logger->nextToPrint += downCast<int>(bytesPrinted);
        }
    }
}

/**
 * Restore a logger to its default initialized state. Used primarily by tests.
 */
void
Logger::reset()
{
    if (mustCloseFd) {
        close(fd);
    }
    fd = 2;
    mustCloseFd = 0;
    collapseMap.clear();
    collapseIntervalMs = DEFAULT_COLLAPSE_INTERVAL;
    maxCollapseMapSize = DEFAULT_COLLAPSE_MAP_LIMIT;
    nextCleanTime = {0, 0};
    nextToInsert = 0;
    nextToPrint = 0;
    discardedEntries = 0;
    testingBufferSize = 0;
    testingLogTime = NULL;
    testingNoNotify = false;
}

/**
 * Wait for all buffered log messages to be printed. This method is intended
 * only for tests and a few special situations such as application exit. It
 * should *not* be used in the normal course of logging messages, since it
 * can result in long delays that could potentially cause the server to be
 * considered crashed.
 */
void
Logger::sync()
{
    while (nextToInsert != nextToPrint) {
        usleep(100);
    }
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
    int count = snprintf(buffer, sizeof(buffer),
            "%010lu.%09lu %s:%d in %s %s[%d]: Assertion `%s' failed.\n",
            now.tv_sec, now.tv_nsec, file, line, function,
            logLevelNames[ERROR], ThreadId::get(), assertion);
    addToBuffer(buffer, count);
    sync();
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
