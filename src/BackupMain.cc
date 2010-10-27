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

/**
 * \file
 * Provides a way to launch a standalone backup server.
 */

#include "config.h"

#include "BackupServer.h"
#include "BackupStorage.h"
#include "OptionParser.h"
#include "TransportManager.h"

/**
 * Instantiates a backup server using the configuration information in
 * config.h.  The backup server runs forever awaiting requests from
 * master servers.
 */
int
main(int argc, char* argv[])
{
    using namespace RAMCloud;

    BackupServer::Config config;
    // CPU mask for binding the backup to a specific set of cores.
    int cpu;

    OptionsDescription extraOptions("Backup");
    extraOptions.add_options()
        ("cpu,c",
         ProgramOptions::value<int>(&cpu)->
            default_value(-1),
         "CPU mask to pin to");

    OptionParser optionParser(extraOptions, argc, argv);
    config.coordinatorLocator = optionParser.options.getCoordinatorLocator();
    config.localLocator = optionParser.options.getLocalLocator();

    LOG(NOTICE, "backup: Listening on %s", config.localLocator.c_str());

    if (cpu != -1) {
        if (!pinToCpu(cpu))
            DIE("backup: Couldn't pin to core %d", cpu);
        LOG(DEBUG, "backup: Pinned to core %d", cpu);
    }

    // Set the address for the backup to listen on.
    transportManager.initialize(config.localLocator.c_str());

    SingleFileStorage storage(SEGMENT_SIZE, 16, "backup.log", 0);
    BackupServer server(config, storage);
    server.run();

    return 0;
}
