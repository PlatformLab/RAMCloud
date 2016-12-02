/* Copyright (c) 2016 University of Utah
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

#include <iostream>
#include <boost/program_options.hpp>
#include <boost/version.hpp>
namespace po = boost::program_options;

#include <string>
#include <vector>

#include "CycleCounter.h"
#include "Cycles.h"
#include "PerfStats.h"
#include "RamCloud.h"

using namespace RAMCloud;

class StatDumper {
  public:
    explicit StatDumper(CommandLineOptions* options)
        : ramcloud{options}
        , dispatchDelay{}
        , readRate{}
        , stats()
        , currentStats{&stats[1]}
        , previousStats{&stats[0]}
    {
    }

    void
    run()
    {
        int i = 0;
        while (true) {
            printf("== %d ==\n", i++);
            collectStats();
            fflush(stdout);
            sleep(1);
        }
    }

  PRIVATE:

    void collectStats()
    {
        std::swap(currentStats, previousStats);

        ramcloud.serverControlAll(
            WireFormat::ControlOp::GET_PERF_STATS, NULL, 0, currentStats);

        printf("%s\n",
           PerfStats::printClusterStats(previousStats, currentStats).c_str());

        merge();
    }

    void merge()
    {

    }


    RamCloud ramcloud;
    double dispatchDelay;
    double readRate;

    Buffer stats[2];
    Buffer* currentStats;
    Buffer* previousStats;

    DISALLOW_COPY_AND_ASSIGN(StatDumper);
};


int
main(int argc, char *argv[])
try
{
    std::string logFile{};
    std::string logLevel{"NOTICE"};
    CommandLineOptions options{};

    po::options_description desc{
        "Usage: StatDumper [options]\n\n"
        "Collects statistics from across a RAMCloud cluster for simple\n"
        "performance monitoring.\n\n"
        "Allowed options:"};

    desc.add_options()
        ("coordinator,C", po::value<string>(&options.coordinatorLocator),
                "Service locator for the cluster coordinator (required)")
        ("logFile", po::value<string>(&logFile),
                "Redirect all output to this file")
        ("logLevel,l", po::value<string>(&logLevel)->default_value("NOTICE"),
                "Print log messages only at this severity level or higher "
                "(ERROR, WARNING, NOTICE, DEBUG)")
        ("help,h", "Print this help message");

    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).
            options(desc).run(), vm);
    po::notify(vm);

    if (logFile.size() != 0) {
        // Redirect both stdout and stderr to the log file.  Don't
        // call logger.setLogFile, since that will not affect printf
        // calls.
        FILE* f = fopen(logFile.c_str(), "w");
        if (f == NULL) {
            RAMCLOUD_LOG(ERROR, "couldn't open log file '%s': %s",
                    logFile.c_str(), strerror(errno));
            exit(1);
        }
        stdout = stderr = f;
    }

    Logger::get().setLogLevels(logLevel);

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        exit(0);
    }

    if (options.coordinatorLocator.empty()) {
        RAMCLOUD_LOG(ERROR, "missing required option --coordinator");
        exit(1);
    }

    StatDumper dumper{&options};
    dumper.run();

    return 0;
} catch (std::exception& e) {
    RAMCLOUD_LOG(ERROR, "%s", e.what());
    exit(1);
}
