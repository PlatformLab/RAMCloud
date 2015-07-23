/* Copyright (c) 2012-2015 Stanford University
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

#include <cstdio>

#include "RamCloud.h"

#include "ClusterMetrics.h"
#include "CycleCounter.h"
#include "Cycles.h"
#include "Logger.h"
#include "ObjectFinder.h"
#include "OptionParser.h"
#include "PingClient.h"
#include "ShortMacros.h"

using namespace RAMCloud;

int
main(int argc, char *argv[])
try
{
    int clientIndex;
    int numClients;
    int count;
    uint32_t objectDataSize;

    Context context(true);

    OptionsDescription clientOptions("Client");
    clientOptions.add_options()
        ("clientIndex",
         ProgramOptions::value<int>(&clientIndex)->
            default_value(0),
         "Index of this client (first client is 0)")
        ("numClients",
         ProgramOptions::value<int>(&numClients)->
            default_value(1),
         "Total number of clients running")
        ("number,n",
         ProgramOptions::value<int>(&count)->
            default_value(1024),
         "The number of values to insert.")
        ("size,s",
         ProgramOptions::value<uint32_t>(&objectDataSize)->
            default_value(1024*1024),
         "Number of bytes to insert per object during insert phase.");

    OptionParser optionParser(clientOptions, argc, argv);
    context.transportManager->setSessionTimeout(
        optionParser.options.getSessionTimeout());

    string coordinatorLocator =
            optionParser.options.getExternalStorageLocator();
    if (coordinatorLocator.size() == 0) {
        coordinatorLocator = optionParser.options.getCoordinatorLocator();
    }
    LOG(NOTICE, "client: Connecting to %s", coordinatorLocator.c_str());
    RamCloud client(&context, coordinatorLocator.c_str(),
            optionParser.options.getClusterName().c_str());

    client.createTable("mainTable");
    uint64_t table = client.getTableId("mainTable");
    Transport::SessionRef mainTableSession =
        context.objectFinder->lookup(table, "0", 1);

    // Create a single value on all masters so that they each have a valid
    // log digest created.  When we choose to crash them we don't want the
    // normal master recovery thinking we've lost data.  This is due to
    // RAM-356.
    char value[objectDataSize];
    memset(value, 'a', objectDataSize);
    int i = 0;
    uint64_t lastBackupTable = 0;
    while (true) {
        char name[10];
        snprintf(name, sizeof(name), "backup%d", ++i);
        client.createTable(name);
        uint64_t table = client.getTableId(name);
        Transport::SessionRef session =
                context.objectFinder->lookup(table, "0", 1);
        // Round-robin table allocation: create tables until we find we've
        // created an extra table on the server with mainTable.
        if (session->getServiceLocator() ==
            mainTableSession->getServiceLocator())
            break;
        client.write(table, "0", 1, value, objectDataSize);
        // Keep track of the table id of one of the backups that doesn't have
        // mainTable on it.  We'll crash it below.
        lastBackupTable = table;
    }

    // Write enough test data to ensure crash will affect the master
    // with mainTable.
    for (int i = 0; i < count; ++i) {
        string key = format("%d", i);
        client.write(table, key.c_str(), downCast<uint16_t>(key.length()),
                value, objectDataSize);
    }

    Transport::SessionRef session =
        context.objectFinder->lookup(lastBackupTable, "0", 1);
    LOG(NOTICE, "Killing %s", session->getServiceLocator().c_str());

    CycleCounter<> backupRecoveryCycles;
    KillRpc killOp(&client, lastBackupTable, "0", 1);

    // Ensure recovery of the master portion completed.
    context.objectFinder->waitForTabletDown(lastBackupTable);
    context.objectFinder->waitForAllTabletsNormal(lastBackupTable);

    // Wait for backup recovery to finish.
    // The way this is detected is a bit weird since it isn't usually
    // an property visible to RAMCloud clients.
    // Consider it finished when replicationTasks == 0 and
    // replicaRecoveries > 0 for all servers except the coordinator.
    // Poll for this condition once per second.
    bool finished = true;
    while (true) {
        finished = true;
        ClusterMetrics metrics(&client);
        for (auto server = metrics.begin();
                server != metrics.end(); ++server++) {
            const string& serverName = server->first;
            // Don't care about metrics on the coordinator, it doesn't
            // have replicas to recover.
            if (serverName == coordinatorLocator)
                continue;
            for (auto metric = server->second.begin();
                    metric != server->second.end(); ++metric) {
                const string& name = metric->first;
                uint64_t value = metric->second;
                if (name == "replicaRecoveries" && value == 0) {
                    LOG(DEBUG, "%s hasn't started backup recovery yet",
                        serverName.c_str());
                    finished = false;
                }
                if (name == "replicationTasks" && value != 0) {
                    LOG(DEBUG, "%s hasn't quiesced replication operations yet",
                        serverName.c_str());
                    finished = false;
                }
            }
        }
        if (finished)
            break;
        sleep(1);
    }

    LOG(NOTICE, "Master/backup recovery done *extremely* rough time: %.2f s",
        Cycles::toSeconds(backupRecoveryCycles.stop()));
} catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}

