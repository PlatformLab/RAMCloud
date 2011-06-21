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

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <getopt.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>

#include <string>
#include <sstream>
#include <map>

#include "Metrics.h"
#include "TestUtil.h"
#include "RamCloud.h"
#include "MasterServer.h"
#include "BenchUtil.h"
#include "OptionParser.h"

#include "MasterClient.h"
#include "Tub.h"

namespace RC = RAMCloud;
using std::map;
using std::string;
using std::stringstream;



// Map string keys to numerical ones for RAMCloud
// Used by queen/worker
class BenchMapper {
  private:
    map<std::string, uint64_t> keymap;
  public:
    enum WORKER_STATE { IDLE = 0, READ, WRITE, EXIT };
    BenchMapper(): keymap() {
// control table
//        - command flag - IDLE, READ, WRITE, count of objects/time
//        - status flags per worker - IDLE, READ, WRITE
//            - worker operation count
//            - worker timetaken
//
        uint64_t i = 1;
        // 0 is special and is used to indicate that the key does not
        // exist

        keymap["command"] = i++;
        for (int j = 0; j <= 300; j++) {
            stringstream s;
            s << ("worker");
            s << j;
            keymap[s.str() + "status"] = i++;
            keymap[s.str() + "r_commandcount"] = i++;
            keymap[s.str() + "r_timetaken"] = i++;
            keymap[s.str() + "w_commandcount"] = i++;
            keymap[s.str() + "w_timetaken"] = i++;
            keymap[s.str() + "valuecount0"] = i++;
            keymap[s.str() + "valuecount1"] = i++;
        }
    }
    uint64_t getKey(std::string s) const {
        map<string, uint64_t>::const_iterator it = keymap.find(s);
        return (it == keymap.end()) ? 0 : it->second;
    }
};

// TODO(nandu) - global variables could be gotten rid of.
std::string coordinatorLocator;
std::string tableName("test");
std::string controlTableName("test");
uint32_t numTables;
uint32_t multiread;
bool multirow;
bool randomReads;
uint64_t count;
uint64_t size;
int cpu;
bool readOnly = false;
std::string executionMode("standalone");
int numWorkers = -1;
int workerId = -1;
int numCalls = 0;

std::vector<uint32_t> tables;

RC::RamCloud *client;
uint32_t table;
uint32_t controlTable;
const BenchMapper benchMapper;
uint64_t value_counter[2] = {0UL, 0UL}; // Work with 2 values for now - 0 and 1

void
cleanup()
{
    // do no cleaning in workers - tables etc.. are owned by queen
    if (executionMode.compare("worker") != 0) {
        client->dropTable(tableName.c_str());
        client->dropTable(controlTableName.c_str());
    }
    delete client;
    client = NULL;
}

void
setup()
{
    if (cpu != -1) {
        if (!RC::pinToCpu(cpu))
            DIE("bench: Couldn't pin to core %d", cpu);
        LOG(RC::DEBUG, "bench: Pinned to core %d", cpu);
    }

    client = new RC::RamCloud(coordinatorLocator.c_str());

    assert(!atexit(cleanup));

    if (numTables == 1) {
        client->createTable(tableName.c_str());
        table = client->openTable(tableName.c_str());
    } else {
        for (uint32_t i = 0; i < numTables; i++) {
            client->createTable(RC::format("%u", i).c_str());
            tables.push_back(client->openTable(RC::format("%u", i).c_str()));
        }
    }

    std::stringstream ss;
    ss << "control." << tableName;
    controlTableName = ss.str();
    client->createTable(controlTableName.c_str());
    controlTable = client->openTable(controlTableName.c_str());


    // std::stringstream sss;
    // sss << workerId << tableName;
    // tableName = sss.str();

    client->createTable(tableName.c_str());
    table = client->openTable(tableName.c_str());
}

uint64_t
bench(const char *name, void (f)(void))
{
    uint64_t start, end, cycles;

    start = rdtsc();
    f();
    end = rdtsc();

    cycles = end - start;
    printf("%s ns     %12lu\n", name,
           RC::cyclesToNanoseconds(cycles));
    printf("%s avgns  %12lu\n", name,
           RC::cyclesToNanoseconds(cycles) / count);
    fprintf(stderr, "Avg. Latency %s avgns  %12lu\n", name,
            RC::cyclesToNanoseconds(cycles) / count);
    // client->ping();
    return RC::cyclesToNanoseconds(cycles);
}

#define BENCH(fname) bench(#fname, fname)

void
writeInt(uint32_t table, uint64_t key, uint64_t val)
{
    client->write(table, key, &val, sizeof(val));
}

template <class T>
void
readInt(uint32_t table, uint64_t key, T& val)
{
    RC::Buffer value;
    client->read(table, key, &value);
    val = *value.getStart<T>();
}

void
writeOne(uint64_t val, uint64_t key = 0)
{
    char buf[size];
    memset(&buf[0], downCast<uint32_t>(val), size);
    buf[size-1] = 0;
    client->write(table, key, &buf[0], downCast<uint32_t>(size));
}

void
writeOne()
{
    writeOne(0xFF);
}


void
writeMany(void)
{
    if (numTables == 1) {
        numCalls++;
        for (uint64_t i = 0; i < count; i++) {
            uint64_t key = randomReads ? generateRandom() %
                                            (1000*1000*10*numCalls +
                                             1000*1000*workerId + i)
                                       : i;
            writeOne(0xFF, key);
        }
    } else {
        // Write one value in each table
        // TODO(ankitak): Allow writing multiple objects in each table
        for (uint32_t i = 0; i < numTables; i++) {
            uint64_t val = 0xFF;
            char buf[size];
            memset(&buf[0], downCast<uint32_t>(val), size);
            buf[size-1] = 0;
            client->write(tables[i], 0, &buf[0], downCast<uint32_t>(size));
        }
    }
}

void
readMany()
{
    uint64_t key;
    RC::Buffer value;
    for (uint64_t i = 0; i < count; i++) {
        key = randomReads ? generateRandom() % count : i;
        client->read(table, multirow ? key : 0, &value);
        uint64_t val = *value.getStart<uint64_t>();
        if (size != value.getTotalLength()) {
            fprintf(stderr, "readMany failed - size of object does not"
                    " match expected size. Size was %ud",
                    value.getTotalLength());
            throw;
        }
        if (val) {
            value_counter[1]++;
        } else {
            value_counter[0]++;
        }
    }
}

void
multiRead_oneMaster()
{
    uint64_t iterations = count / multiread;

    RC::MasterClient::ReadObject requestObjects[multiread];
    RC::MasterClient::ReadObject* requests[multiread];

    for (uint64_t i = 0; i < iterations; i++) {
        RC::Tub<RC::Buffer> values[multiread];

        for (uint32_t j = 0; j < multiread; j++) {
            uint64_t key = randomReads ? generateRandom() % count : j;
            requestObjects[j] = RC::MasterClient::ReadObject(
                                table, (randomReads || multirow) ? key : 0,
                                &values[j]);
            requests[j] = &requestObjects[j];
        }

        client->multiRead(requests, multiread);

        for (uint32_t k = 0; k < multiread; k++) {
            RC::Buffer *valueBuffer = values[k].get();
            uint64_t val = *valueBuffer->getStart<uint64_t>();
            if (val) {
                value_counter[1]++;
            } else {
                value_counter[0]++;
            }
        }
    }
}

void
multiRead_multiMaster()
{
    // Current implementation assumes that only one object is to be read
    // from each table. i.e., when numTables > 1, multiread = 1
    // TODO(ankitak): Allow for multiple objects from each table

    uint64_t iterations = count / numTables;

    RC::MasterClient::ReadObject requestObjects[numTables];
    RC::MasterClient::ReadObject* requests[numTables];

    for (uint64_t i = 0; i < iterations; i++) {
        RC::Tub<RC::Buffer> values[numTables];
        for (uint32_t j = 0; j < numTables; j++) {
            requestObjects[j] = RC::MasterClient::ReadObject(tables[j], 0,
                                                             &values[j]);
            requests[j] = &requestObjects[j];
        }

        client->multiRead(requests, numTables);

        for (uint32_t j = 0; j < numTables; j++) {
            RC::Buffer *valueBuffer = values[j].get();
            uint64_t val = *valueBuffer->getStart<uint64_t>();
            if (val) {
                value_counter[1]++;
            } else {
                value_counter[0]++;
            }
        }
    }
}

bool
checkAllWorkersInSameState(BenchMapper::WORKER_STATE state)
{
    bool allReady = true;
    for (int worker = 0; worker < numWorkers; worker++) {
        stringstream k;
        k << "worker" << worker << "status";
        uint64_t v = -1;
        try {
            readInt(controlTable, benchMapper.getKey(k.str()), v);
        } catch (RC::ObjectDoesntExistException& e) {
            // Not fatal here - Keep waiting
        } catch (RC::ClientException& e) {
            fprintf(stderr, "readInt failed for %s - RAMCloud Client "
                    "exception: %s\n", k.str().c_str(), e.str().c_str());
            throw;
        }
        if (static_cast<int>(v) != state) {
            allReady = false;
            // fprintf(stderr, "state for workerId %d was %lu, expecting %lu\n",
            //        worker, v, state);
        }
    }
    return allReady;
}


int
waitForAllWorkersToHitState(BenchMapper::WORKER_STATE state,
                            uint64_t timeout = 10)
{
        bool allReady = false;
        uint64_t timeoutmicros = 0;
        while (!allReady) {
            if (timeoutmicros > 1000 * 1000 * timeout) {
                return -1; // timeout before state change.
            }
            allReady = checkAllWorkersInSameState(state);
            const uint64_t sleepmicros = 1000;
            timeoutmicros += sleepmicros;
            usleep(sleepmicros);
        }
        return 0;
}

int
main(int argc, char *argv[])
try
{
    RC::OptionsDescription benchOptions("Bench");
    benchOptions.add_options()
        ("cpu,p",
         RC::ProgramOptions::value<int>(&cpu)->
           default_value(-1),
         "CPU mask to pin to")
         ("multiread,d",
          RC::ProgramOptions::value<uint32_t>(&multiread)->
            default_value(0),
          "Number of objects to be read in each multiread from each table.")
        ("tables,b",
         RC::ProgramOptions::value<uint32_t>(&numTables)->
           default_value(1),
         "Number of tables to be used.")
        ("multirow,m",
         RC::ProgramOptions::bool_switch(&multirow),
         "Write number of objects equal to number parameter.")
        ("number,n",
         RC::ProgramOptions::value<uint64_t>(&count)->
           default_value(10000),
         "Number of iterations to write/read.")
        ("random,R",
         RC::ProgramOptions::bool_switch(&randomReads),
         "Randomize key order instead of incremental.")
        ("tablename,t",
         RC::ProgramOptions::value<std::string>(&tableName),
         "Name of test table used. Default 'test'.")
        ("readonly,o",
         RC::ProgramOptions::bool_switch(&readOnly),
         "Only perform read operations. Do not perform first"
         " bootstrapping write. Default off.")
        ("executionmode,e",
         RC::ProgramOptions::value<std::string>(&executionMode),
         "Mode to execute in - standalone(default)|worker|queen.")
        ("numworkers,l",
         RC::ProgramOptions::value<int>(&numWorkers),
         "Number of workers when executing in queen mode.")
        ("workerid,i",
         RC::ProgramOptions::value<int>(&workerId),
         "Id of this executing worker (worker mode only)")
        ("size,S",
         RC::ProgramOptions::value<uint64_t>(&size)->
           default_value(100),
         "Size in bytes of objects to write/read.");

    RC::OptionParser optionParser(benchOptions, argc, argv);

    coordinatorLocator = optionParser.options.getCoordinatorLocator();
    printf("client: Connecting to %s\n", coordinatorLocator.c_str());

    fprintf(stderr, "Reads: %lu, Size: %lu, Multirow: %d, RandomReads: %d\n",
           count, size, multirow, randomReads);
    fprintf(stderr, "ReadOnly: %d, Multiread: %u\n",
            readOnly, multiread);

    setup();
    fprintf(stderr, "Started Benching!\n");

    if (executionMode.compare("standalone") == 0) {
        if ((!readOnly && (multirow || randomReads))
            || (readOnly && randomReads)) {
            BENCH(writeMany);
        } else {
            BENCH(writeOne);
        }

        if (multiread == 0) {
            BENCH(readMany);
        } else {
            if (numTables == 1) {
                BENCH(multiRead_oneMaster);
            } else {
                BENCH(multiRead_multiMaster);
            }
        }

    } else if (executionMode.compare("worker") == 0) {
        fprintf(stderr, "worker mode - workerId %d\n", workerId);
        stringstream k;
        k << ("worker");
        k << workerId;
        string keyprefix = k.str();

        // Set up initial objects for read loads.
        writeOne(0xFF);

        uint64_t r_overallcount = 0;
        uint64_t r_overalltimetaken = 0;
        uint64_t w_overallcount = 0;
        uint64_t w_overalltimetaken = 0;

        while (1) {
            uint64_t v = -1;
            try {
                readInt(controlTable, benchMapper.getKey("command"),
                        v);
            } catch (RC::ObjectDoesntExistException& e) {
                v = BenchMapper::IDLE;
                fprintf(stderr, "no command object - going IDLE\n");
            }
            fprintf(stderr, "Command was %lu\n", v);
            // Accepting command - change status to indicate this.
            writeInt(controlTable,
                     benchMapper.getKey(keyprefix + "status"),
                     v);
            switch (v) {
            case BenchMapper::READ:
                r_overalltimetaken += (BENCH(readMany));
                r_overallcount += count;
                break;
            case BenchMapper::WRITE:
                w_overalltimetaken += (BENCH(writeMany));
                w_overallcount += count;
                break;
            case BenchMapper::IDLE:
                usleep(10);
                break;
            case BenchMapper::EXIT:
                fprintf(stderr, "worker exiting\n");
                writeInt(controlTable,
                         benchMapper.getKey(keyprefix + "r_commandcount"),
                         r_overallcount);
                writeInt(controlTable,
                         benchMapper.getKey(keyprefix + "r_timetaken"),
                         r_overalltimetaken);
                writeInt(controlTable,
                         benchMapper.getKey(keyprefix + "valuecount0"),
                         value_counter[0]);
                writeInt(controlTable,
                         benchMapper.getKey(keyprefix + "valuecount1"),
                         value_counter[1]);
                writeInt(controlTable,
                         benchMapper.getKey(keyprefix + "w_commandcount"),
                         w_overallcount);
                writeInt(controlTable,
                         benchMapper.getKey(keyprefix + "w_timetaken"),
                         w_overalltimetaken);
                return 0;
            }
        }

    } else if (executionMode.compare("queen") == 0) {
        fprintf(stderr, "queen mode\n");
        if (numWorkers == -1) {
            fprintf(stderr, "Please specify -l number of workers in"
                    " this mode.\n");
            return 1;
        }
        fprintf(stderr, "Expecting %d workers.\n", numWorkers);

        RC::ServerStats wstats;
        uint64_t w_nanos = 0;

        /////// WRITE

        writeInt(controlTable,
                 benchMapper.getKey("command"),
                 BenchMapper::WRITE);
        fprintf(stderr, "command WRITE - %lu\n",
                static_cast<uint64_t>(BenchMapper::WRITE));

        if (waitForAllWorkersToHitState(BenchMapper::WRITE) != 0) {
            fprintf(stderr, "Timed out waiting for workers to move to "
                    "WRITE.\n");
            return 1;
        }
        fprintf(stderr, "All workers in WRITE status\n");

        writeOne(0x00); // Write 0 to signal to workers for latency
                        // counts - this is a signal to readMany()


        // this read resets the serverStats objects so that we can
        // grab counts for totals on the server side like throughput.
        readInt(controlTable,
                RC::MasterServer::TOTAL_READ_REQUESTS_OBJID,
                wstats);
        w_nanos = BENCH(writeMany);
        readInt(controlTable,
                RC::MasterServer::TOTAL_READ_REQUESTS_OBJID,
                wstats);
        writeOne(0xFF); // Write non-zero to signal workers

        bool stillWriting =
            checkAllWorkersInSameState(BenchMapper::WRITE);
        if (!stillWriting) {
            fprintf(stderr, "Workers finished sooner than queen"
                    " expected. Error!\n");
            return 1;
        }


        fprintf(stderr, "Finished running write benchmark\n");


        /////// READ

        writeInt(controlTable,
                 benchMapper.getKey("command"),
                 BenchMapper::READ);
        fprintf(stderr, "command READ - %lu\n",
                static_cast<uint64_t>(BenchMapper::READ));

        if (waitForAllWorkersToHitState(BenchMapper::READ) != 0) {
            fprintf(stderr, "Timed out waiting for workers to move to "
                    "READ.\n");
            return 1;
        }
        fprintf(stderr, "All workers in READ status\n");

        writeOne(0x00); // Write 0 to signal to workers for latency
                        // counts - this is a signal to readMany()
        RC::ServerStats s;

        // this read resets the serverStats objects so that we can
        // grab counts for totals on the server side like throughput.
        readInt(controlTable,
                RC::MasterServer::TOTAL_READ_REQUESTS_OBJID,
                s);
        uint64_t nanos = BENCH(readMany);
        readInt(controlTable,
                RC::MasterServer::TOTAL_READ_REQUESTS_OBJID,
                s);
        writeOne(0xFF); // Write non-zero to signal workers

        bool stillReading =
            checkAllWorkersInSameState(BenchMapper::READ);
        if (!stillReading) {
            fprintf(stderr, "Workers finished sooner than queen"
                    " expected. Error!\n");
            return 1;
        }

        fprintf(stderr, "Finished running read benchmark\n");


        ///// CLEAN UP and get STATS

        writeInt(controlTable, benchMapper.getKey("command"),
                 BenchMapper::EXIT);
        if (waitForAllWorkersToHitState(BenchMapper::EXIT) != 0) {
            fprintf(stderr, "Timed out waiting for workers to move to "
                    "EXIT.\n");
            return 1;
        }
        fprintf(stderr, "All workers in EXIT status\n");
        // Set to IDLE now so that new workers start correctly in
        // subsequent runs.
        writeInt(controlTable, benchMapper.getKey("command"),
                 BenchMapper::IDLE);

        uint64_t total_v0 = value_counter[0];
        uint64_t total_v1 = value_counter[1];
        for (int worker = 0; worker < numWorkers; worker++) {
            stringstream k;
            k << ("worker");
            k << worker;
            uint64_t tt = -1;
            uint64_t ct = -1;
            uint64_t v0 = -1;
            uint64_t v1 = -1;

            readInt(controlTable, benchMapper.getKey(k.str() +
                                                     "r_timetaken"), tt);
            readInt(controlTable,
                    benchMapper.getKey(k.str() + "r_commandcount"),
                    ct);
            readInt(controlTable, benchMapper.getKey(k.str() +
                                                     "valuecount0"), v0);
            total_v0 += v0;
            readInt(controlTable, benchMapper.getKey(k.str() +
                                                     "valuecount1"), v1);
            total_v1 += v1;
            double nsperop = static_cast<double>(tt)/static_cast<double>(ct);
            fprintf(stderr,
                    "Worker %3d performed %8lu  read operations in "
                    "%15lu ns @ %12.2f ns/op.\n",
                    worker, ct, tt, nsperop);

            readInt(controlTable, benchMapper.getKey(k.str() +
                                                     "w_timetaken"), tt);
            readInt(controlTable,
                    benchMapper.getKey(k.str() + "w_commandcount"),
                    ct);
            nsperop = static_cast<double>(tt)/static_cast<double>(ct);
            fprintf(stderr,
                    "Worker %3d performed %8lu write operations in "
                    "%15lu ns @ %12.2f ns/op.\n",
                    worker, ct, tt, nsperop);
        }
        fprintf(stderr, "Total count of read operations with value 0"
                " is %15lu.\n", total_v0);
        fprintf(stderr, "Total count of read operations with value 1"
                " is %15lu.\n", total_v1);
        fprintf(stderr, "Total read nanos seen"
                " is %15lu ns.\n", nanos);
        fprintf(stderr, "Total server-read-ops seen"
                " is %15lu.\n", s.totalReadRequests);
        fprintf(stderr, "Avg server-read-time seen"
                " is %15lu ns.\n", s.totalReadNanos);
        fprintf(stderr, "Total write nanos seen"
                " is %15lu ns.\n", w_nanos);
        fprintf(stderr, "Total server-write-ops seen"
                " is %15lu.\n", wstats.totalWriteRequests);
        fprintf(stderr, "Avg server-write-time seen"
                " is %15lu ns.\n", wstats.totalWriteNanos);

        fprintf(stderr, "Avg server-read "
                " is %15lu ns/read.\n",
                s.totalReadNanos/s.totalReadRequests);
        fprintf(stderr, "Avg server-write "
                " is %15lu ns/read.\n",
                wstats.totalWriteNanos/wstats.totalWriteRequests);
        fprintf(stderr, "Avg server-backup-sync "
                " is %15lu ns/read.\n",
                wstats.totalBackupSyncNanos/wstats.totalBackupSyncs);
        fprintf(stderr, "Avg server-wait-time per rpc "
                " is %15lu ns/read.\n",
                s.serverWaitNanos/s.totalReadRequests);


        printf("tputread ops/sec  %12lu\n",
               total_v0 * 1000 * 1000 * 1000 / nanos);
        printf("tputwrite ops/sec  %12lu\n",
               wstats.totalWriteRequests * 1000 * 1000 * 1000 /
               w_nanos);
        printf("tputwrite bytes/sec  %12lu\n",
               size * wstats.totalWriteRequests * 1000 * 1000 * 1000 /
               w_nanos);

        printf("tputgettx ops/sec  %12lu\n",
               s.totalReadRequests * 1000 * 1000 * 1000 /
               s.infrcGetTxBufferNanos);

        printf("serverread avgns  %12lu\n",
               s.totalReadNanos/s.totalReadRequests);
        printf("serverwrite avgns  %12lu\n",
               wstats.totalWriteNanos/wstats.totalWriteRequests);
        printf("serversyncnanos avgns  %12lu\n",
               wstats.totalBackupSyncNanos/wstats.totalBackupSyncs);
        printf("serversynccount ct  %12lu\n",
               wstats.totalBackupSyncs);
        printf("serverwait avgns  %12lu\n",
               s.serverWaitNanos/s.totalReadRequests);
        printf("infrcGetTxBuffer avgns  %12lu\n",
               s.infrcGetTxBufferNanos/s.infrcGetTxCount);
        printf("infrcGetTxCount ct %12lu\n",
               s.infrcGetTxCount);
        printf("readCount ct %12lu\n",
               s.totalReadRequests);
        printf("writeCount ct %12lu\n",
               wstats.totalWriteRequests);
        printf("infrcSendReplyNanos avgns  %12lu\n",
               s.infrcSendReplyNanos/s.totalReadRequests);
        printf("gtbPollCount ct %12lu\n",
               s.gtbPollCount);
        printf("gtbPollZeroNCount ct %12lu\n",
               s.gtbPollZeroNCount);
        printf("gtbPollNanos avgns  %.4f\n",
               double(s.gtbPollNanos)/double(s.gtbPollCount)); //NOLINT
        if ( s.gtbPollZeroNCount == 0 ) { // divide by zero avoidance
           s. gtbPollZeroNCount = 1;
        }
        printf("gtbPollZeroNanos avgns  %.4f\n",
               double(s.gtbPollZeroNanos)/double(s.gtbPollZeroNCount)); //NOLINT
        printf("gtbPollNonZeroNanos avgns  %.4f\n",
               double(s.gtbPollNonZeroNanos)/               //NOLINT
               double(s.gtbPollCount-s.gtbPollZeroNCount)); //NOLINT
        printf("gtbPollNonZeroNAvg ct  %3.4f\n",
               (double(s.gtbPollNonZeroNAvg))/              //NOLINT
               double(s.gtbPollCount-s.gtbPollZeroNCount)); //NOLINT
        printf("gtbPollNonZeroPct ct  %3.4f\n",
               100.0 -
               (double(s.gtbPollZeroNCount))/double(s.gtbPollCount)); //NOLINT
        printf("gtbPollEffNanos avgns  %12lu\n",
               long(double(s.gtbPollNanos)/      //NOLINT
                    double(s.infrcGetTxCount))); //NOLINT 
        return 0;

    } else {
        fprintf(stderr, "Bad execution mode specified - must be"
                "standalone or queen or worker.\n");
        return 1;
    }
    return 0;
} catch (RC::ClientException& e) {
    fprintf(stderr, "RAMCloud Client exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
