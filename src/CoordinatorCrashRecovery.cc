/* Copyright (c) 2013-2014 Stanford University
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

// This program runs a RAMCloud client to test coordinator recovery and in
// process, the correct functioning of many aspects of the system. This
// file works in conjunction with coordinatorrecovery.py that in turn
// uses cluster.py

#include <thread>
#include <boost/program_options.hpp>
#include <boost/version.hpp>
#include <unordered_map>
#include <iostream>
namespace po = boost::program_options;

#include "RamCloud.h"
#include "CycleCounter.h"
#include "Cycles.h"
#include "ServerId.h"
#include "AbstractServerList.h"
#include "Util.h"

using namespace RAMCloud;


enum OPERATIONS {
    START_SERVER = 0,                   // Start a new server
    KILL_SERVER = 1,                    // Kill an existing server
    CREATE_TABLE = 2,                   // Create a new table
    SPLIT_TABLET = 3,                   // Split a tablet
    DROP_TABLE = 4,                     // Drop an existing table
    WRITE_DATA = 5,                     // Write a new object
    NUM_OPS = 6,                        // Number of different operations
};

static char coordinatorServiceLocator[50];

// Total number of clients that will be participating in this test.
static int numClients;

// Index of this client among all of the participating clients (between
// 0 and numClients-1).  Client 0 acts as master to control the overall
// flow of the test; the other clients are slaves that respond to
// commands from the master. Currently, only one client is used.
static int clientIndex;

// The maximum number of tables that can be created during the duration
// of the coordinator crash recovery test
#define MAX_TABLES 200000

// Value of the "--size" command-line option: used by some tests to
// determine the number of bytes in each object.  -1 means the option
// wasn't specified, so each test should pick an appropriate default.
static int objectSize;

// Number of client threads that invoke RAMCloud operations. One such
// thread in addition to the thread that sends RPCs to the coordinator
// and sets crash points is sufficient to introduce asynchrony.
#define NUM_CLIENTS 1

// Number of servers that are initially started when the test is started.
#define NUM_SERVERS_INITIAL 8 // set in clusterperf.py

// Total duration of the coordiator recovery test
#define TOTAL_SECS_COORD_RECOVERY 100

// Maximum number of servers that can be killed during the test.
// It is better to be conservative here because we don't want to run out
// of backups for recovery.
#define MAX_KILL_COUNT 30

// This threshold is used in deciding whether or not a server can be
// killed in the current state of the cluster. This is essentially a
// buffer from the actual number of servers running.
#define KILL_THRESHOLD 15

// Number of different operations the client thread performs.
#define NUM_OPS 6

#define MAX_METRICS 8

// The following struct is used to maintain a mapping between objects
// and tables. It also defines the object parameters as required by the
// fillBuffer and checkBuffer APIs. Key length is also required but it
// can be obtained from the key because the key is NULL terminated.
typedef struct
{
    uint64_t tableId;           // Table ID this object corresponds to
    char key[21];               // Key this object corresponds to
    int size;                   // Size of the object
} ObjectIdentifier;

// The following struct keeps track of the objects, tables, servers, tablets
// that were added/modified/removed during the test. This data is used when
// doing consistency checks to match the coordinator's state.
struct localState
{

    localState()
        : servers(),
          killedServers(),
          tables(),
          droppedTables(),
          splitPoints(),
          objects(),
          currentNumServers(NUM_SERVERS_INITIAL),
          tableCount(1)
    {
    }

    /**
     * List of servers that were started during the test
     * and currently should be in the coordinator server list
     */
    vector<string> servers;

    /**
     * List of servers that were killed during the test
     * and currently should not be in the coordinator server list
     */
    vector<string> killedServers;

    /**
     * The list of tables that were created during the coordinator
     * recovery test
     */
    vector<string> tables;

    /**
     * The list of tables that were dropped during the coordinator recovery
     * test
     */
    vector<string> droppedTables;

    /**
     * A map between table ID and the split key hashes.
     */
    std::unordered_multimap<uint64_t, uint64_t> splitPoints;

    /**
     * The list of objects that were created during the coordinator recovery
     * test
     */
    vector<ObjectIdentifier> objects;

    /**
     * The number of servers currently running in the cluster
     */
    uint64_t currentNumServers;

    /**
     * The number of tables created by the test
     */
    uint64_t tableCount;
};

/**
 * Fill a buffer with an ASCII value that can be checked later to ensure
 * that no data has been lost or corrupted.  A particular tableId, key and
 * keyLength are incorporated into the value (under the assumption that
 * the value will be stored in that object), so that values stored in
 * different objects will be detectably different.
 *
 * \param buffer
 *      Buffer to fill; any existing contents will be discarded.
 * \param size
 *      Number of bytes of data to place in the buffer.
 * \param tableId
 *      This table identifier will be reflected in the value placed in the
 *      buffer.
 * \param key
 *      This key will be reflected in the value placed in the buffer.
 * \param keyLength
 *      This key Length will be reflected in the value placed in the buffer.
 */
void
fillBuffer(Buffer& buffer, uint32_t size, uint64_t tableId,
           const void* key, uint16_t keyLength)
{
    char chunk[51];
    buffer.reset();
    uint32_t bytesLeft = size;
    int position = 0;
    while (bytesLeft > 0) {
        // Write enough data to completely fill the chunk buffer, then
        // ignore the terminating NULL character that snprintf puts at
        // the end.
        snprintf(chunk, sizeof(chunk),
            "| %d: tableId 0x%lx, key %.*s, keyLength 0x%x %s",
            position, tableId, keyLength, reinterpret_cast<const char*>(key),
            keyLength, "0123456789");
        uint32_t chunkLength = static_cast<uint32_t>(sizeof(chunk) - 1);
        if (chunkLength > bytesLeft) {
            chunkLength = bytesLeft;
        }
        buffer.appendCopy(chunk, chunkLength);
        bytesLeft -= chunkLength;
        position += chunkLength;
    }
}

/**
 * Check the contents of a buffer to ensure that it contains the same data
 * generated previously by fillBuffer.  Generate a log message if a
 * problem is found.
 *
 * \param buffer
 *      Buffer whose contents are to be checked.
 * \param expectedLength
 *      The buffer should contain this many bytes.
 * \param tableId
 *      This table identifier should be reflected in the buffer's data.
 * \param key
 *      This key should be reflected in the buffer's data.
 * \param keyLength
 *      This key length should be reflected in the buffer's data.
 *
 * \return
 *      True means the buffer has the "expected" contents; false means
 *      there was an error.
 */
bool
checkBuffer(Buffer* buffer, uint32_t expectedLength, uint64_t tableId,
            const void* key, uint16_t keyLength)
{
    uint32_t length = buffer->size();
    if (length != expectedLength) {
        RAMCLOUD_LOG(ERROR, "corrupted data: expected %u bytes, "
                "found %u bytes", expectedLength, length);
        return false;
    }
    Buffer comparison;
    fillBuffer(comparison, expectedLength, tableId, key, keyLength);
    for (uint32_t i = 0; i < expectedLength; i++) {
        char c1 = *buffer->getOffset<char>(i);
        char c2 = *comparison.getOffset<char>(i);
        if (c1 != c2) {
            int start = i - 10;
            const char* prefix = "...";
            const char* suffix = "...";
            if (start <= 0) {
                start = 0;
                prefix = "";
            }
            uint32_t length = 20;
            if (start+length >= expectedLength) {
                length = expectedLength - start;
                suffix = "";
            }
            RAMCLOUD_LOG(ERROR, "corrupted data: expected '%c', got '%c' "
                    "(\"%s%.*s%s\" vs \"%s%.*s%s\")", c2, c1, prefix, length,
                    static_cast<const char*>(comparison.getRange(start,
                    length)), suffix, prefix, length,
                    static_cast<const char*>(buffer->getRange(start,
                    length)), suffix);
            return false;
        }
    }
    return true;
}



/**
 * Thread that sends RPCs to coordinator instructing it to crash at specific
 * crash points.
 *
 * \param id
 *      Id of this thread that causes coordinator crashes. This is not used
 *      right now because there is only one such thread.
 */
void
crashCoord(int id)
{
    Context context2(false);
    RamCloud r2(&context2, coordinatorServiceLocator);
    RamCloud *cluster = &r2;

    const char *crashPoints[] = {"create_1", "create_2", "create_3",
                                  "create_4", "drop_1", "drop_2", "drop_3",
                                  "drop_4", "split_1", "split_2", "enlist_1",
                                  "enlist_2"};
    const uint64_t numCrashPoints = 12;

    uint64_t startCycles = Cycles::rdtsc();
    while (true) {
        uint64_t rand = generateRandom() % numCrashPoints;
        cluster->setRuntimeOption("crashCoordinator",
                                         crashPoints[rand]);
        usleep(100);
        double secsWaiting =
                        Cycles::toSeconds(Cycles::rdtsc() - startCycles);
        if (secsWaiting >= TOTAL_SECS_COORD_RECOVERY)
            break;
    }
}

/**
 * Check if a specific server is alive in the cluster.
 *
 * \param cluster
 *      RAMCloud object use to invoke RAMCloud operations.
 * \param locator
 *      The service locator of the server
 * \param serverList
 *      The server list obtained from the coordinator
 *
 * \return
 *      True, if the server is alive.
 */
static bool
checkServerAlive(RamCloud *cluster, const char *locator,
                    ProtoBuf::ServerList serverList)
{
    bool serverPresent = false;

    for (int i = 0; i < serverList.server_size(); i++) {
        const ProtoBuf::ServerList_Entry& server = serverList.server(i);
        const string& serviceLocator = server.service_locator();

        /* check if the newly enlisted master is in the server list and is up */
        if (strcmp(serviceLocator.c_str(), locator) == 0 &&
                   server.status() == (uint32_t)ServerStatus::UP) {
            serverPresent = true;
            RAMCLOUD_LOG(NOTICE, "Found new master: %s",
                         serviceLocator.c_str());
        }
    }
    if (!serverPresent)
        RAMCLOUD_LOG(ERROR, "FAILURE: Did not find new master at %s or "
                            "it is not up but is still in the server list\n",
                             locator);
    return serverPresent;
}

/**
 * Check if a specific server is NOT in the cluster.
 *
 * \param cluster
 *      RAMCloud object use to invoke RAMCloud operations.
 * \param locator
 *      The service locator of the server
 * \param serverList
 *      The server list obtained from the coordinator
 *
 * \return
 *      True, if the server is not in the cluster.
 */
static bool
checkServerKilled(RamCloud *cluster, const char *locator,
                    ProtoBuf::ServerList serverList)
{
    bool serverAbsent = true;

    for (int i = 0; i < serverList.server_size(); i++) {
        const ProtoBuf::ServerList_Entry& server = serverList.server(i);
        const string& serviceLocator = server.service_locator();

        if (strcmp(serviceLocator.c_str(), locator) == 0 &&
                server.status() == (uint32_t)ServerStatus::UP) {
            RAMCLOUD_LOG(ERROR, "FAILURE: Server at %s is in the list"
                         "and is up", serviceLocator.c_str());
            serverAbsent = false;
        } else if (strcmp(serviceLocator.c_str(), locator) == 0 &&
                    server.status() == (uint32_t)ServerStatus::CRASHED) {
            RAMCLOUD_LOG(ERROR, "FAILURE: Server at %s is dead but is still in"
                         "the list being recovered", serviceLocator.c_str());
            serverAbsent = false;
        }
    }
    if (!serverAbsent)
        RAMCLOUD_LOG(ERROR, "FAILURE: Killed master at %s in server list",
                     locator);
    return serverAbsent;
}

/**
 * Checks for inconsistencies in the coordinator server list between what
 * the coordinator believes and some local information.
 *
 * \param cluster
 *      RAMCloud object used to invoke RAMCloud operations
 * \param servers
 *      List of servers that were started during the test
 *      and currently should be in the coordinator server list
 * \param killedServers
 *      List of servers that were killed during the test
 *      and currently should not be in the coordinator server list
 *
 * \return
 *      True, if the coordinator server list has no abormalities.
 */
bool
checkServerList(RamCloud *cluster, vector<string> servers,
                vector<string> killedServers)
{
    bool aliveStatus = true;
    bool deadStatus = true;
    ProtoBuf::ServerList serverList;
    CoordinatorClient::getServerList(cluster->clientContext, &serverList);
    for (vector<string>::iterator it = servers.begin();
            it != servers.end(); ++it)
        aliveStatus = aliveStatus && checkServerAlive(cluster, (*it).c_str(),
                                                        serverList);

    for (vector<string>::iterator it = killedServers.begin();
            it != killedServers.end(); ++it)
        deadStatus = deadStatus && checkServerKilled(cluster, (*it).c_str(),
                                                        serverList);

    return aliveStatus && deadStatus;
}

/**
 * Start a new server in the cluster
 *
 * \param[out] state
 *      Local state data maintained by this test.
 */
void
startServer(struct localState *state)
{
    /**
     * parse output from script to find out where the servers were started
     * so that the corresponding locators can be appended to a list to be
     * used while doing consistency checks
     */
    char locator[50];

    FILE *fp = popen((const char *) "python ../scripts/startserver.py"
                " -d ./logs/shm &", "r");
    if (!fp) {
        RAMCLOUD_LOG(ERROR,
                     "Couldn't open ../scripts/startserver.py: %s",
                     strerror(errno));
        return;
    }
    if (fscanf(fp, "%s", locator) < 0) {
        RAMCLOUD_LOG(ERROR,
            "Couldn't find service locator string in script startserver.py");
        return;
    }
    // Try to find the last locator value in startserver.py.
    while (!feof(fp)) {
        if (fscanf(fp, "%s", locator) < 0)
            break;
    }
    pclose(fp);
    //if (strlen(locator) > 5) {
    if (strcmp(locator, "NOSERVER")) {
        // a new server was available
        state->currentNumServers++;
        state->servers.push_back(string(locator));
        RAMCLOUD_LOG(NOTICE, "Starting server at %s\n", locator);
        /**
         * if this server locator is being reused, then remove it from the
         * killed servers list if it exists.
         */
        vector<string>::iterator position = state->killedServers.begin();
        for (; position != state->killedServers.end(); position++) {
            if (strcmp(locator, (*position).c_str()) == 0) {
                RAMCLOUD_LOG(NOTICE, "Removing server at %s from killed"
                                     "servers list. Now being reused"
                                     "for a new server\n", locator);
                state->killedServers.erase(position);
                break;
            }
        }
    } else {
        RAMCLOUD_LOG(NOTICE, "No more servers available now\n");
    }
}

/**
 * Kill a server in the cluster
 *
 * \param[out] state
 *      Local state data maintained by this test.
 * \param serverLocator
 *      The service locator of the server that is going to be killed
 */
void
killServer(struct localState *state, const char* serverLocator)
{
    char st[100];
    RAMCLOUD_LOG(ERROR, "KILLING SERVER AT %s", serverLocator);
    snprintf(st, sizeof(st), "python crashserver.py -l %s &", serverLocator);
    if (system(reinterpret_cast<char *>(st))) {
        RAMCLOUD_LOG(ERROR, "Error invoking crashserver.py");
        return;
    }
    state->killedServers.push_back(string(serverLocator));

    vector<string>::iterator position = state->servers.begin();
    for (; position != state->servers.end(); position++) {
        if (strcmp(serverLocator, (*position).c_str()) == 0) {
            state->servers.erase(position);
            break;
        }
    }
}

/**
 * Create a table.
 *
 * \param cluster
 *      RAMCloud object used to invoke RAMCLoud operations.
 * \param[out] state
 *      Local state data maintained by this test.
 *
 * \return
 *      True, if a table was created.
 */
bool
createTable(RamCloud *cluster, struct localState *state)
{
    char tableName[20];

    if (state->tableCount <= MAX_TABLES) {
        snprintf(tableName, sizeof(tableName), "table_%lu", state->tableCount);
        state->tableCount++;
        cluster->createTable(tableName);
        string newTable(tableName);
        state->tables.push_back(newTable);
        return true;
    } else {
        return false;
    }
}

/**
 * Write an object into a randomly selected table
 *
 * \param cluster
 *      RAMCloud object used to invoke RAMCloud operations.
 * \param[out] state
 *      Local state data maintained by this test.
 *
 * \return
 *      True, if the write operation was a success.
 */
bool
writeData(RamCloud *cluster, struct localState *state)
{
    Buffer input;
    uint64_t randIndex = generateRandom() %
                            (static_cast<int>((state->tables).size()));
    string& tableString = state->tables.at(randIndex);
    const char *tableName = tableString.c_str();
    string msg("no exception");
    uint64_t tableId;
    try {
        tableId = cluster->getTableId(tableName);
    }
    catch (ClientException& e) {
        msg = e.toSymbol();
        RAMCLOUD_LOG(ERROR, "FAILURE: getting Table id of %s. %s"
                            " exception received", tableName, msg.c_str());
        return false;
    }

    char key[21];
    key[20] = '\0';
    uint16_t keyLength = 20;
    // Get a random key of length 20
    Util::genRandomString(key, keyLength);

    fillBuffer(input, objectSize, tableId, key, keyLength);

    string message("no exception");
    try {
        cluster->write(tableId, key, keyLength, input.getRange(0, objectSize),
                       objectSize);
    }
    catch (ClientException& e) {
        message = e.toSymbol();
        RAMCLOUD_LOG(ERROR, "FAILURE: %s exception received for"
                            "table_id %lu", message.c_str(), tableId);
        return false;
    }

    ObjectIdentifier object;
    object.tableId = tableId;
    object.size = objectSize;
    strncpy(object.key, key, sizeof(object.key));
    state->objects.push_back(object);
    return true;
}

/**
 * Read back data corresponding to the objecs that were written by writeData
 * and verify that it is correct.
 *
 * \param cluster
 *      RAMCloud object used to invoke RAMCloud operations.
 * \param objects
 *      The list of objects that were created during the coordinator recovery
 *      test
 *
 * \return
 *      True, if the write operation was a success.
 */
bool
verifyObjects(RamCloud *cluster, vector<ObjectIdentifier> objects)
{
    uint64_t tableId;
    const char *key;
    uint16_t keyLength;
    int size;
    vector<ObjectIdentifier>::iterator it;
    RAMCLOUD_LOG(NOTICE, "Verifying objects");
    for (it = objects.begin(); it != objects.end(); ++it) {
        Buffer output;
        tableId = (*it).tableId;
        key = (const char *)(*it).key;
        keyLength = downCast<uint16_t>(strlen(key));
        size = (*it).size;
        string message("no exception");
        try {
            cluster->read(tableId , key, keyLength, &output);
        }
        catch (ClientException& e) {
            message = e.toSymbol();
            RAMCLOUD_LOG(ERROR, "FAILURE: %s exception received",
                         message.c_str());
            return false;
        }

        if (!(checkBuffer(&output, size, tableId, key, keyLength))) {
            RAMCLOUD_LOG(ERROR, "FAILURE: Check buffer failed");
            return false;
        }
    }
    return true;
}

/**
 * Split a tablet at a random hash value
 *
 * \param cluster
 *      RAMCloud object used to invoke RAMCloud operations.
 * \param[out] state
 *      Local state data maintained by this test.
 *
 * \return
 *      True if the split was successful
 */
bool
splitTablet(RamCloud *cluster, struct localState *state)
{
    bool ret = true;

    // pick a random table
    uint64_t randIndex = generateRandom() %
                            static_cast<int>(state->tables.size());
    std::string tableString(state->tables[randIndex]);
    const char *tableName = tableString.c_str();

    uint64_t splitPoint = generateRandom();

    string message("no exception");
    try {
        cluster->splitTablet(tableName, splitPoint);
    }
    catch (ClientException& e) {
        message = e.toSymbol();
        RAMCLOUD_LOG(ERROR, "FAILURE: %s exception received", message.c_str());
        return false;
    }

    // splitPoints can have duplicates for same tableID.
    // Not a correctness issue though since splitTablet is idempotent
    state->splitPoints.insert(std::make_pair(cluster->getTableId(tableName),
                                             splitPoint));
    RAMCLOUD_LOG(NOTICE, "split point for %lu at %lu",
                 cluster->getTableId(tableName), splitPoint);
    return ret;
}

/**
 * Drop a table
 *
 * \param cluster
 *      RAMCloud object used to invoke RAMCloud operations.
 * \param[out] state
 *      Local state data maintained by this test.
 *
 * \return
 *      True, if the able was dropped successfully
 */
bool
dropTable(RamCloud *cluster, struct localState *state)
{
    // pick a random tableId to drop. Then remove the corresponding objects
    // from the objects vector and corresponding split points from splitPoints

    bool ret = true;

    uint64_t randIndex = generateRandom() % (static_cast<int>
                                ((state->tables).size()));
    std::string tableString(state->tables[randIndex]);

    const char *tableName = tableString.c_str();
    string msg("no exception");
    uint64_t tableId = 0; // initial value only

    try {
        tableId = cluster->getTableId(tableName);
    }
    catch (ClientException& e) {
        msg = e.toSymbol();
        RAMCLOUD_LOG(ERROR, "FAILURE: Trying to drop table %s that does not"
                            "exist. %s exception received", tableName,
                            msg.c_str());
        return false;
    }

    RAMCLOUD_LOG(NOTICE, "Dropping table %s", tableName);
    string message("no exception");
    try {
        cluster->dropTable(tableName);
    }
    catch (ClientException& e) {
        message = e.toSymbol();
        RAMCLOUD_LOG(ERROR, "FAILURE: Dropping table %s, %s exception"
                            " received", tableName, message.c_str());
        return false;
    }
    (state->droppedTables).push_back(string(tableName));

    /* remove this table from the list of tables */
    state->tables.erase(state->tables.begin() + randIndex);

    /* remove all objects in this table Id */
    vector<ObjectIdentifier>::iterator iter;
    for (iter = state->objects.begin(); iter != state->objects.end(); ) {
        if (iter->tableId == tableId)
            iter = state->objects.erase(iter);
        else
            ++iter;
    }

    /* remove all split points from the map that belonged to this table Id */
    state->splitPoints.erase(tableId);
    return ret;
}

/**
 * Checks if all the split points are reflected in the tablet map
 *
 * \param cluster
 *      RAMCloud object used to invoke RAMCloud operations.
 * \param splitPoints
 *      A map between table ID and the split key hashes.
 *
 * \return
 *      True, if all the spit points are reflected in the tablet map
 */
bool
checkSplitTablets(RamCloud *cluster,
                  std::unordered_multimap<uint64_t, uint64_t> splitPoints)
{
    RAMCLOUD_LOG(NOTICE, "Checking for Split tablets");
    bool ret = true;
    int count = 0;
    std::unordered_multimap<uint64_t, uint64_t>::iterator it;
    for (it = splitPoints.begin(); it != splitPoints.end(); ++it) {
        ProtoBuf::TableConfig tableConfig;
        CoordinatorClient::getTableConfig(cluster->clientContext,
                                                  it->first, &tableConfig);
        count = 0;
        RAMCLOUD_LOG(NOTICE, "Printing TabletMap\n");
        foreach (const ProtoBuf::TableConfig::Tablet& tablet,
                                                tableConfig.tablet()) {

            if (it->second == tablet.end_key_hash() + 1)
                count++;
            else if (it->second == tablet.start_key_hash())
                count++;

            RAMCLOUD_LOG(NOTICE, "Table ID %lu, start key hash %lu,"
                                "end key hash %lu\n",
                                (uint64_t)tablet.table_id(),
                                (uint64_t)tablet.start_key_hash(),
                                tablet.end_key_hash());
        }
        if (count == 2) { // this split exists
            RAMCLOUD_LOG(NOTICE, "Split exists");
        } else {
            RAMCLOUD_LOG(ERROR, "FAILURE: Table ID at %lu should be split at"
                                " key hash %lu but tabletMap does not"
                                "reflect it", it->first, it->second);
            ret = false;
        }
    }
    return ret;
}

/**
 * Check if the tables created during the coordinator recovery test exist
 *
 * \param cluster
 *      RAMCloud object used to invoke RAMCloud operations.
 * \param tables
 *      The list of tables that were created during the coordinator recovery
 *      test
 *
 * \return
 *      True, if all the tables created still exist
 */
bool
checkCreateTables(RamCloud *cluster, vector<string> tables)
{
    RAMCLOUD_LOG(NOTICE, "Checking for created tables\n");
    bool ret = true;
    for (vector<string>::iterator it = tables.begin();
            it != tables.end(); ++it) {

        string message("no exception");
        try {
            cluster->getTableId((*it).c_str());
        }
        catch (ClientException& e) {
            message = e.toSymbol();
            RAMCLOUD_LOG(ERROR, "FAILURE: Table %s not present",
                                 (*it).c_str());
            ret = false;
        }
       RAMCLOUD_LOG(NOTICE, "Table %s is present", (*it).c_str());
    }
    return ret;
}

/**
 * Check if the tables dropped during the coordinator recovery test exist or
 * not
 *
 * \param cluster
 *      RAMCloud object used to invoke RAMCloud operations.
 * \param droppedTables
 *      The list of tables that were dropped during the coordinator recovery
 *      test
 *
 * \return
 *      True, if all the tables dropped don't exist
 */
bool
checkDropTables(RamCloud *cluster, vector<string> droppedTables)
{
    bool ret = true;
    RAMCLOUD_LOG(NOTICE, "Checking for dropped tables\n");
    for (vector<string>::iterator it = droppedTables.begin();
            it != droppedTables.end(); ++it) {

        string message("no exception");
        try {
            cluster->getTableId((*it).c_str());
        }
        catch (ClientException& e) {
            message = e.toSymbol();
            RAMCLOUD_LOG(NOTICE, "Table %s dropped", (*it).c_str());
            continue;
        }
        RAMCLOUD_LOG(ERROR, "FAILURE: Dropped table %s is present",
                    (*it).c_str());
        ret = false;
    }
    return ret;
}

/**
 * Check overall consistency for the coordinator recovery test
 *
 * \param cluster
 *      RAMCloud object used to invoke RAMCloud operations.
 * \param[out] state
 *      Local state data maintained by this test.
 *
 * \return
 *      True, if there are no errors in any of the associated operations.
 */
bool
checkConsistency(RamCloud *cluster, struct localState *state)
{
    bool status = true;
    RAMCLOUD_LOG(NOTICE, "Doing consistency check");
    if (!checkServerList(cluster, state->servers, state->killedServers))
        status = false;
    if (!checkSplitTablets(cluster, state->splitPoints))
        status = false;

    // verifyObjects will check all objects written so far.
    // This will take care of checking that the objects
    // in the killed master are still accessible after
    // recovery
    if (!verifyObjects(cluster, state->objects))
        status = false;
    if (!checkCreateTables(cluster, state->tables))
        status = false;
    if (!checkDropTables(cluster, state->droppedTables))
        status = false;

    return status;
}

/**
 * Thread that performs various RAMCloud operations that include creating/
 * dropping tables, writing/reading objects, splitting tablets and starting/
 * crashing servers. It also spawns a thread that is responsible for crashing
 * the coordinator at specific points.
 *
 * \param id
 *      Thread id. This is not used now because there is only one thread that
 *      coordinatorRecovery() creates.
 */
void
coordTestFunction(int id)
{
    Context context1(false);
    RamCloud r1(&context1, coordinatorServiceLocator);
    RamCloud *cluster = &r1;

    std::thread crashThread(crashCoord, 1);

    uint64_t randIndex;

    /* state required for consistency checks */
    struct localState state;

    double totalTimeSeconds = TOTAL_SECS_COORD_RECOVERY;
    uint64_t totalTime = Cycles::fromSeconds(totalTimeSeconds);
    uint64_t time = 0;
    int killCount = 0;
    int startCount = 0;
    int tableCount = 0;
    int splitCount = 0;
    int objCount = 0;
    int dropCount = 0;
    bool status = true;
    while (time < totalTime) {
        uint64_t start = Cycles::rdtsc();
        randIndex = generateRandom() % NUM_OPS;

        if (randIndex == START_SERVER) {
            startCount++;
            startServer(&state);
        } else if (randIndex == KILL_SERVER &&
                   state.currentNumServers > KILL_THRESHOLD &&
                   state.killedServers.size() < state.currentNumServers -
                                                    KILL_THRESHOLD) {
            // The above condition for killedServers is to make sure that there
            // are at least 10 servers remaining before an attempt to kill an
            // existing server is made. It is better to be conservative in
            // this estimate because otherwise recovery of these servers will
            // not be possible with insufficient backups. It also eases
            // debugging. This value may require some fine tuning depending on
            // the number of machines in the cluster, number of servers used for
            // the test etc.
            // The second condition in the 'if' is required because size()
            // returns unsigned int.
            if (state.servers.size() > 0) {
                ProtoBuf::ServerList serverList;
                CoordinatorClient::getServerList(cluster->clientContext,
                                                 &serverList);
                uint64_t serverIndex = generateRandom() % static_cast<int>
                                       (serverList.server_size());
                const ProtoBuf::ServerList_Entry& server =
                                    serverList.server(static_cast<int>
                                                     (serverIndex));
                const char *serverLocator = server.service_locator().c_str();
                // extra cautious here by limiting the max number of servers
                // that are killed.
                if (server.status() == (uint32_t) ServerStatus::UP &&
                        killCount < MAX_KILL_COUNT) {
                    killCount++;
                    killServer(&state, serverLocator);
                    state.currentNumServers--;
                    sleep(10);
                }
            }
        } else if (randIndex == CREATE_TABLE) {
            if (createTable(cluster, &state))
              tableCount++;
        } else if (randIndex == SPLIT_TABLET) {
            if (state.tables.size() != 0) {
                splitCount++;
                status = splitTablet(cluster, &state);
                RAMCLOUD_LOG(NOTICE, "Split count %d", splitCount);
            }
        } else if (randIndex == DROP_TABLE) {
            if (state.tables.size() != 0) {
                dropCount++;
                status = dropTable(cluster, &state);
            }
        } else if (randIndex == WRITE_DATA) {
            if (state.tables.size() != 0) {
                objCount++;
                status = writeData(cluster, &state);
            }
        }

        if (!status)
            break;

        uint64_t toCheckConsistency = generateRandom() % 100;
        // with 1/100 probability, after each iteration, call checkConsistency
        // This is arbitraty but we don't want to keep doing the check after
        // each iteration.
        if (toCheckConsistency == 99)
            checkConsistency(cluster, &state);
        uint64_t thisLoop = Cycles::rdtsc() - start;
        time+= thisLoop;
    }
    crashThread.join();
    checkConsistency(cluster, &state);
    RAMCLOUD_LOG(NOTICE, "Servers started %d times, killed %d times",
                         startCount, killCount);
    RAMCLOUD_LOG(NOTICE, "Created %d tables, dropped %d tables, wrote %d"
                        " objects, split %d tablets", tableCount, dropCount,
                        objCount, splitCount);
}

/**
 * Entry point for the coordinator recovery test.
 */
void
coordinatorRecovery()
{
    if (clientIndex != 0)
        return;
    int i;
    std::thread ramcloudOpsThread(coordTestFunction, i);
    ramcloudOpsThread.join();
    RAMCLOUD_LOG(NOTICE, "All children done, Main thread exiting\n");
}


// The following struct and table define the coordinator crash recovery test
// in terms of a string name and a function that implements the test.
struct TestInfo {
    const char* name;             // Name of the test; this is
                                  // what gets typed on the command line to
                                  // run the test.
    void (*func)();               // Function that implements the test.
};

// The structure is generic to allow for more similar tests in the future.

TestInfo tests[] = {
    {"coordinatorRecovery", coordinatorRecovery},
};

int
main(int argc, char *argv[])
try
{
    // Parse command-line options.
    vector<string> testNames;
    string coordinatorLocator, logFile;
    string logLevel("NOTICE");
    po::options_description desc(
            "Usage: CoordinatorCrashRecovery [options]\n\n"
            "This program is not normally invoked\n"
            "directly; it is invoked by the coordinatorrecovery script.\n\n"
            "Allowed options:");
    desc.add_options()
        ("clientIndex", po::value<int>(&clientIndex)->default_value(0),
                "Index of this client (first client is 0)")
        ("coordinator,C", po::value<string>(&coordinatorLocator),
                "Service locator for the cluster coordinator (required)")
        ("logFile", po::value<string>(&logFile),
                "Redirect all output to this file")
        ("logLevel,l", po::value<string>(&logLevel)->default_value("NOTICE"),
                "Print log messages only at this severity level or higher "
                "(ERROR, WARNING, NOTICE, DEBUG)")
        ("help,h", "Print this help message")
        ("numClients", po::value<int>(&numClients)->default_value(1),
                "Total number of clients running")
        ("size,s", po::value<int>(&objectSize)->default_value(100),
                "Size of objects (in bytes) to use for test")
        ("testName", po::value<vector<string>>(&testNames),
                "Name(s) of test(s) to run");
    po::positional_options_description desc2;
    desc2.add("testName", -1);
    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).
            options(desc).positional(desc2).run(), vm);
    po::notify(vm);
    if (logFile.size() != 0) {
        // Redirect both stdout and stderr to the log file.
        FILE* f = fopen(logFile.c_str(), "w");
        if (f == NULL) {
            RAMCLOUD_LOG(ERROR, "couldn't open log file '%s': %s",
                    logFile.c_str(), strerror(errno));
            exit(1);
        }
        stdout = stderr = f;
        Logger::get().setLogFile(fileno(f));
    }
    Logger::get().setLogLevels(logLevel);
    if (vm.count("help")) {
        std::cout << desc << '\n';
        exit(0);
    }
    if (coordinatorLocator.empty()) {
        RAMCLOUD_LOG(ERROR, "missing required option --coordinator");
        exit(1);
    }

    snprintf(coordinatorServiceLocator, sizeof(coordinatorServiceLocator),
             "%s", coordinatorLocator.c_str());

    foreach (TestInfo& info, tests) {
        info.func();
    }
}
catch (std::exception& e) {
    RAMCLOUD_LOG(ERROR, "%s", e.what());
    exit(1);
}
