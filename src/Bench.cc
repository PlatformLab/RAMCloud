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

#include <string>
#include <sstream>
#include <map>

#include "TestUtil.h"
#include "RamCloud.h"
#include "BenchUtil.h"
#include "OptionParser.h"

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
            keymap[s.str() + "commandcount"] = i++;
            keymap[s.str() + "timetaken"] = i++;
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
bool multirow;
bool randomReads;
uint64_t count;
uint64_t size;
int cpu;
bool readOnly = false;
std::string executionMode("standalone");
int numWorkers = -1;
int workerId = -1;

RC::RamCloud *client;
uint32_t table;
uint32_t controlTable;
const BenchMapper benchMapper;

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

    client->createTable(tableName.c_str());
    table = client->openTable(tableName.c_str());

    std::stringstream ss;
    ss << "control." << tableName;
    controlTableName = ss.str();
    client->createTable(controlTableName.c_str());
    controlTable = client->openTable(controlTableName.c_str());

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
    client->ping();
    return RC::cyclesToNanoseconds(cycles);
}

#define BENCH(fname) bench(#fname, fname)

void
writeInt(uint32_t table, uint64_t key, uint64_t val)
{
    char buf[1000]; // TODO(nandu) use a better size
    int ret = snprintf(buf, sizeof(buf), "%lu", val);
    if (ret < 0) {
        fprintf(stderr, "sprintf error.");
        return;
    }
    client->write(table, key, &buf[0], ret);
}

void
readInt(uint32_t table, uint64_t key, uint64_t& val)
{
    RC::Buffer value;
    client->read(table, key, &value);
    string str(RAMCloud::toString(&value));
    stringstream ss;
    ss << str;
    ss >> val;
}

void
writeOne()
{
    char buf[size];
    memset(&buf[0], 0xFF, size);
    buf[size - 1] = 0;

    client->write(table, 0, &buf[0], size);
}

void
writeMany(void)
{
    char buf[size];
    memset(&buf[0], 0xFF, size);
    buf[size - 1] = 0;

    for (uint64_t i = 0; i < count; i++)
        client->write(table, i, &buf[0], size);
}

void
readMany()
{
    uint64_t key;

    for (uint64_t i = 0; i < count; i++) {
        RC::Buffer value;
        key = randomReads ? generateRandom() % count : i;
        client->read(table, multirow ? key : 0, &value);
    }
}

bool
checkAllWorkersInSameState(BenchMapper::WORKER_STATE state) {
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
                            uint64_t timeout = 10) {
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

    printf("Reads: %lu, Size: %lu, Multirow: %d, RandomReads: %d\n",
           count, size, multirow, randomReads);

    setup();

    fprintf(stderr, "Started Benching!\n");

    if (executionMode.compare("standalone") == 0) {
        if (!readOnly) {
            if (multirow) {
                BENCH(writeMany);
            } else {
                BENCH(writeOne);
            }
        }
        BENCH(readMany);
    } else if (executionMode.compare("worker") == 0) {
        fprintf(stderr, "worker mode - workerId %d\n", workerId);
        stringstream k;
        k << ("worker");
        k << workerId;
        string keyprefix = k.str();

        // Set up initial objects for read loads.
        BENCH(writeOne);

        uint64_t overallcount = 0;
        uint64_t overalltimetaken = 0;
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
                overalltimetaken += (BENCH(readMany));
                overallcount += count;
                break;
            case BenchMapper::WRITE:
                overalltimetaken += (BENCH(writeMany));
                overallcount += count;
                break;
            case BenchMapper::IDLE:
                usleep(10);
                break;
            case BenchMapper::EXIT:
                fprintf(stderr, "worker exiting\n");
                writeInt(controlTable,
                         benchMapper.getKey(keyprefix + "commandcount"),
                         overallcount);
                writeInt(controlTable,
                         benchMapper.getKey(keyprefix + "timetaken"),
                         overalltimetaken);
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

        BENCH(writeOne);
        BENCH(readMany);

        fprintf(stderr, "Finished running benchmark\n");

        bool stillReading =
            checkAllWorkersInSameState(BenchMapper::READ);
        if (!stillReading) {
            fprintf(stderr, "Workers finished sooner than queen"
                    " expected. Error!\n");
            return 1;
        }

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

        for (int worker = 0; worker < numWorkers; worker++) {
            stringstream k;
            k << ("worker");
            k << worker;
            uint64_t tt = -1;
            uint64_t ct = -1;
            readInt(controlTable, benchMapper.getKey(k.str() +
                                                     "timetaken"), tt);
            readInt(controlTable, benchMapper.getKey(k.str() +
                                                     "commandcount"), ct);
            double nsperop = static_cast<double>(tt)/static_cast<double>(ct);
            fprintf(stderr, "Worker %3d performed %12lu operations in %15lu "
                    "nanoseconds @ %8.2f ns/op.\n", worker, ct, tt, nsperop);

        }
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
