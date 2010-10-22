/* Copyright (c) 2009 Stanford University
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

#include <stdlib.h>
#include <getopt.h>
#include <errno.h>

#include "config.h"
#include "BackupClient.h"
#include "OptionParser.h"
#include "Master.h"
#include "TransportManager.h"

static int cpu;

int
main(int argc, char *argv[])
try
{
    using namespace RAMCloud;

    ServerConfig config;
    vector<string> backupLocators;

    OptionsDescription serverOptions("Master");
    serverOptions.add_options()
        ("cpu,c",
         ProgramOptions::value<int>(&cpu)->
            default_value(-1),
         "CPU mask to pin to")
        ("backup,b",
         ProgramOptions::value<vector<string> >(&backupLocators),
         "Backup locators to backup to, can specify more than one");

    OptionParser optionParser(serverOptions, argc, argv);

    LOG(NOTICE, "Using %lu backups", backupLocators.size());

    config.coordinatorLocator = optionParser.options.getCoordinatorLocator();
    config.localLocator = optionParser.options.getLocalLocator();

    LOG(NOTICE, "server: Listening on %s", config.localLocator.c_str());

    if (cpu != -1) {
        if (!pinToCpu(cpu))
            DIE("server: Couldn't pin to core %d", cpu);
        LOG(DEBUG, "server: Pinned to core %d", cpu);
    }

    transportManager.initialize(config.localLocator.c_str());

    BackupManager backup;
    foreach (string& locator, backupLocators)
        backup.addHost(transportManager.getSession(locator.c_str()));

    Master server(&config, &backup);
    server.run();

    return 0;
} catch (RAMCloud::Exception& e) {
    using namespace RAMCloud;
    LOG(ERROR, "server: %s", e.message.c_str());
}
