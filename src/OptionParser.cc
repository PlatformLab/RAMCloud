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

#include <boost/program_options.hpp>
#include <iostream>

#include "Common.h"
#include "Transport.h"
#include "OptionParser.h"

namespace RAMCloud {

/**
 * Parse the common RAMCloud command line options and store
 * them in #options.
 *
 * \param argc
 *      Count of the number of elements in argv.
 * \param argv
 *      A Unix style word tokenized array of command arguments including
 *      the executable name as argv[0].
 */
OptionParser::OptionParser(int argc,
                           char* argv[])
    : options()
    , allOptions("Usage")
    , appOptions()
{
    setup(argc, argv);
}

/**
 * Parse the common RAMCloud command line options and store
 * them in #options as well as additional application-specific options.
 *
 * \param appOptions
 *      The OptionsDescription to be parsed.  Each of the options that are
 *      part of it should include a pointer to where the value should be
 *      stored after parsing.  See boost::program_options::options_description
 *      for details.
 * \param argc
 *      Count of the number of elements in argv.
 * \param argv
 *      A Unix style word tokenized array of command arguments including
 *      the executable name as argv[0].
 */
OptionParser::OptionParser(
        const OptionsDescription& appOptions,
        int argc,
        char* argv[])
    : options()
    , allOptions("Usage")
    , appOptions(appOptions)
{
    setup(argc, argv);
}

/// Print program option documentation to stderr.
void
OptionParser::usage() const
{
    std::cerr << allOptions << std::endl;
}

/**
 * Print program option documentation to stderr and exit the program with
 * failure.
 */
void
OptionParser::usageAndExit() const
{
    usage();
    exit(EXIT_FAILURE);
}

/**
 * Internal method to do the heavy lifting of parsing the command line.
 *
 * Parses both application-specific and common options aborting and
 * printing usage if the --help flag is encountered.
 *
 * \param argc
 *      Count of the number of elements in argv.
 * \param argv
 *      A Unix style word tokenized array of command arguments including
 *      the executable name as argv[0].
 */
void
OptionParser::setup(int argc, char* argv[])
{
    namespace po = ProgramOptions;

    int defaultLogLevel;

    // Basic options supported on the command line of all apps
    OptionsDescription commonOptions("Common");
    commonOptions.add_options()
        ("help", "produce help message");

    // Options allowed on command line and in config file for all apps
    OptionsDescription configOptions("RAMCloud");
    configOptions.add_options()
        ("logLevel,l",
         po::value<int>(&defaultLogLevel)->
            default_value(NOTICE),
         "Default log level for all modules, see LogLevel")
        ("local,L",
         po::value<string>(&options.localLocator)->
           default_value("fast+udp:host=0.0.0.0,port=12242"),
         "Service locator to listen on")
        ("coordinator,C",
         po::value<string>(&options.coordinatorLocator)->
           default_value("fast+udp:host=0.0.0.0,port=12246"),
         "Service locator where the coordinator can be contacted");

    allOptions.add(commonOptions).add(configOptions).add(appOptions);

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, allOptions), vm);
    po::notify(vm);

    if (vm.count("help"))
        usageAndExit();

    logger.setLogLevels(defaultLogLevel);
}

} // end RAMCloud
