/* Copyright (c) 2015 Diego Ongaro
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
#if ENABLE_LOGCABIN

#include <LogCabin/Client.h>
#include <LogCabin/Debug.h>

#include "LogCabinLogger.h"

namespace RAMCloud {
namespace LogCabinLogger {

namespace {

namespace LCDebug = LogCabin::Client::Debug;
typedef LCDebug::LogLevel LCLogLevel;

/**
 * Invoked by LogCabin to print log messages. Routes them to the RAMCloud
 * logger.
 */
void
logHandler(LogCabin::Client::Debug::DebugMessage message)
{
    LogLevel level;
    if (message.logLevel >= int(LCLogLevel::VERBOSE))
        level = LogLevel::DEBUG;
    else if (message.logLevel >= int(LCLogLevel::NOTICE))
        level = LogLevel::NOTICE;
    else if (message.logLevel >= int(LCLogLevel::WARNING))
        level = LogLevel::WARNING;
    else
        level = LogLevel::ERROR;

    std::string filename = format("logcabin/%s", message.filename);
    CodeLocation where(filename.c_str(),
                       message.linenum,
                       message.function,
                       message.function /* don't have pretty function */);
    Logger& logger = Logger::get();
    if (logger.isLogging(LogModule::EXTERNAL_STORAGE_MODULE, level)) {
        logger.logMessage(false,
                          LogModule::EXTERNAL_STORAGE_MODULE,
                          level,
                          where,
                          "%s\n",
                          message.message.c_str());
    }
}

} // anonymous namespace

///////// LogCabinStorage public //////////

/**
 * Set up debug logging in the LogCabin library to log to the RAMCloud
 * logger.
 * \param logLevel
 *      Configure LogCabin to log messages that are at least as important as
 *      the given RAMCloud log level (messages that are less important will be
 *      not be logged).
 */
void
setup(LogLevel logLevel)
{
    const char* policy;
    switch (logLevel) {
        case LogLevel::SILENT_LOG_LEVEL:
            policy = "SILENT";
            break;
        case LogLevel::ERROR:
            policy = "ERROR";
            break;
        case LogLevel::WARNING:
            policy = "WARNING";
            break;
        case LogLevel::NOTICE:
            policy = "NOTICE";
            break;
        case LogLevel::DEBUG:
            policy = "VERBOSE";
            break;
        default:
            policy = "NOTICE";
    }
    LCDebug::setLogHandler(logHandler);
    LCDebug::setLogPolicy(LCDebug::logPolicyFromString(policy));
}

} // namespace RAMCloud::LogCabinLogger
} // namespace RAMCloud

#endif // ENABLE_LOGCABIN
