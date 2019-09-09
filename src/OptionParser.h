/* Copyright (c) 2010-2013 Stanford University
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

#ifndef RAMCLOUD_OPTIONPARSER_H
#define RAMCLOUD_OPTIONPARSER_H

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#pragma GCC diagnostic ignored "-Weffc++"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#include <boost/program_options.hpp>
#pragma GCC diagnostic pop

#include "Transport.h"

namespace RAMCloud {

/// See boost::program_options, just a synonym for that namespace.
namespace ProgramOptions {
    using namespace boost::program_options; // NOLINT
}
/// See boost::program_options::options_description, just a type synonym.
typedef ProgramOptions::options_description OptionsDescription;

/// See boost::program_options::positional_options_description, just a
/// type synonym.
typedef ProgramOptions::positional_options_description
        PositionalOptionsDescription;

/// Holds values for generic RAMCloud options.
class CommandLineOptions {
  public:
    CommandLineOptions()
        : coordinatorLocator()
        , localLocator()
        , externalStorageLocator()
        , pcapFilePath()
        , sessionTimeout(0)
        , portTimeout(0)
        , clusterName()
        , configDir()
        , dpdkPort(0)
    {
    }

    /// Returns the local locator the application should listen on, if any.
    const string& getLocalLocator() const
    {
        return localLocator;
    }

    /**
     * Returns the locator the application should contact the coordinator
     * at, if any.
     */
    const string& getCoordinatorLocator() const
    {
        return coordinatorLocator;
    }

    /**
     * Returns information about how to connect to an external storage
     * server that holds coordinator configuration information.
     */
    const string& getExternalStorageLocator() const
    {
        return externalStorageLocator;
    }

    /**
     * Returns a name identifying the RAMCloud cluster to connect with.
     * Allows multiple clusters to coexist without interference.
     */
    const string& getClusterName() const
    {
        return clusterName;
    }

    /**
     * Returns the path of the directory containing RAMCloud configuration
     * files.
     */
    const string& getConfigDir() const
    {
        return configDir;
    }

    /**
     * Returns the locator the application should contact the coordinator
     * at, if any.
     */
    const string& getPcapFilePath() const
    {
        return pcapFilePath;
    }

    /**
     * Returns the time (in ms) after which transports should assume that
     * a connection has failed.  0 means use a transport-specific default.
     */
    uint32_t getSessionTimeout() const
    {
        return sessionTimeout;
    }

    /**
     * Returns the time (in ms) after which transports should assume that
     * the client for the lisning port is dead.
     */
    int32_t getPortTimeout() const
    {
        return portTimeout;
    }

    /**
     * Returns the integer id of the device port to use with the
     * DPDK network driver.
     */
    int getDpdkPort() const
    {
        return dpdkPort;
    }

    string coordinatorLocator;      ///< See getCoordinatorLocator().
    string localLocator;            ///< See getLocalLocator().
    string externalStorageLocator;  ///< See getExternalStorageLocator().
    string pcapFilePath;            ///< Packet log file, "" to disable.
    uint32_t sessionTimeout;        ///< See getSessionTimeout().
    int32_t  portTimeout;           ///< See getSessionTimeout().
    string clusterName;             ///< See getClusterName().
    string configDir;               ///< See getConfigDir().
    int dpdkPort;                   ///< See getDpdkPort().
};

/**
 * Parses command line options for RAMCloud applications.  It also allows
 * one to specify additional options that are program specific.
 *
 * Example use:
 * \code
 *  OptionsDescription telnetOptions("Telnet");
 *  telnetOptions.add_options()
 *      ("generate,g",
 *       ProgramOptions::bool_switch(&generate),
 *       "Continuously send random data")
 *      ("server,s",
 *       ProgramOptions::value<vector<string> >(&serverLocators),
 *       "Server locator of server, can be repeated to send to all");
 *
 *  OptionParser optionParser(telnetOptions, argc, argv);
 *
 *  if (!serverLocators.size()) {
 *      optionParser.usage();
 *      DIE("Error: No servers specified to telnet to.");
 *  }
 *  \endcode
 */
class OptionParser {
  public:
    OptionParser(int argc, char* argv[]);
    OptionParser(const OptionsDescription& appOptions,
                 int argc, char* argv[]);
    OptionParser(const OptionsDescription& appOptions,
                 const PositionalOptionsDescription& positionalOptions,
                 int argc, char* argv[]);
    void usage() const;
    void usageAndExit() const;

    /// Values for options common to all RAMCloud applications.
    CommandLineOptions options;

  private:
    void setup(int argc, char* argv[]);

    /// The composition of appOptions and the RAMCloud common options.
    OptionsDescription allOptions;

    /// Additional application-specific options that should be parsed.
    const OptionsDescription appOptions;

    /// Command line options that are allowed to be identified by their
    /// positions without option names.
    const PositionalOptionsDescription positionalOptions;
};

} // end RAMCloud

#endif  // RAMCLOUD_OPTIONPARSER_H
