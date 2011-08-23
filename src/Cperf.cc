/* Copyright (c) 2011 Stanford University
 * Copyright (c) 2011 Facebook
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

// This program runs a RAMCloud client along with a collection of benchmarks
// for measuring the performance of a RAMCloud cluster.  This file works in
// conjunction with cperf.py, which starts up the cluster servers along with
// one or more instances of this program. This file contains the low-level
// benchmark code.

#include <boost/program_options.hpp>
#include <boost/version.hpp>
#include <iostream>
namespace po = boost::program_options;

#include "RamCloud.h"
#include "Cycles.h"

using namespace RAMCloud;

// Used to invoke RAMCloud operations.
static RamCloud* cluster;

// Total number of clients that will be participating in this test.
static int numClients;

// Index of this client among all of the participating clients (between
// 0 and numClients-1).  Client 0 acts as master to control the overall
// flow of the test; the other clients are slaves that respond to
// commands from the master.
static int clientIndex;

// Identifier for table that is used for test-specific data.
uint32_t dataTable = -1;

// Identifier for table that is used to communicate between the master
// and slaves to coordinate execution of tests.
uint32_t controlTable = -1;

// The locations of objects in controlTable; each of these values is an
// offset relative to the base for a particular client, as computed by
// controlId.

enum Id {
    STATE =  0,                      // Current state of this client.
    COMMAND = 1,                     // Command issued by master for
                                     // this client.
    DOC = 2,                         // Documentation string in master's
                                     // regions; used in log messages.
};

//----------------------------------------------------------------------
// Utility functions used by the test functions
//----------------------------------------------------------------------

/**
 * Print a performance measurement consisting of a time value.
 *
 * \param name
 *      Symbolic name for the measurement, in the form test::value
 *      where \c test is the name of the test that generated the result,
 *      \c value is a name for the particular measurement.
 * \param seconds
 *      Time measurement, in seconds.
 * \param description
 *      Longer string (but not more than 20-30 chars) with a human-
 *      readable explanation of what the value refers to.
 */
void
printTime(const char* name, double seconds, const char* description)
{
    printf("%-20s ", name);
    if (seconds < 1.0e-06) {
        printf("%5.1f ns  ", 1e09*seconds);
    } else if (seconds < 1.0e-03) {
        printf("%5.1f us  ", 1e06*seconds);
    } else if (seconds < 1.0) {
        printf("%5.1f ms  ", 1e03*seconds);
    } else {
        printf("%5.1f s   ", seconds);
    }
    printf("  %s\n", description);
}

/**
 * Print a performance measurement consisting of a bandwidth.
 *
 * \param name
 *      Symbolic name for the measurement, in the form test::value
 *      where \c test is the name of the test that generated the result,
 *      \c value is a name for the particular measurement.
 * \param bandwidth
 *      Measurement in units of bytes/sec.
 * \param description
 *      Longer string (but not more than 20-30 chars) with a human-
 *      readable explanation of what the value refers to.
 */
void
printBandwidth(const char* name, double bandwidth, const char* description)
{
    printf("%-20s ", name);
    if (bandwidth >= 1.0e09) {
        printf("%5.1f GB/s", 1e-09*bandwidth);
    } else if (bandwidth >= 1.0e06) {
        printf("%5.1f MB/s", 1e-06*bandwidth);
    } else if (bandwidth >= 1.0e03) {
        printf("%5.1f KB/s", 1e-03*bandwidth);
    } else {
        printf("%5.1f B/s ", bandwidth);
    }
    printf("  %s\n", description);
}

/**
 * Time how long it takes to read a particular object repeatedly.
 *
 * \param tableId
 *      Table containing the object.
 * \param objectId
 *      Identifier of the object within its table.
 * \param ms
 *      Read the object repeatedly until this many total ms have
 *      elapsed.
 * \param value
 *      The contents of the object will be stored here, in case
 *      the caller wants to examine them.
 *
 * \return
 *      The average time to read the object, in seconds.
 */
double
timeRead(uint32_t tableId, uint64_t objectId, double ms, Buffer& value)
{
    uint64_t runCycles = Cycles::fromSeconds(ms/1e03);

    // Read the value once just to warm up all the caches everywhere.
    cluster->read(tableId, objectId, &value);

    uint64_t start = Cycles::rdtsc();
    uint64_t elapsed;
    int count = 0;
    while (true) {
        for (int i = 0; i < 10; i++) {
            cluster->read(tableId, objectId, &value);
        }
        count += 10;
        elapsed = Cycles::rdtsc() - start;
        if (elapsed >= runCycles)
            break;
    }
    return Cycles::toSeconds(elapsed)/count;
}

/**
 * Time how long it takes to write a particular object repeatedly.
 *
 * \param tableId
 *      Table containing the object.
 * \param objectId
 *      Identifier of the object within its table.
 * \param value
 *      Pointer to first byte of contents to write into the object.
 * \param length
 *      Size of data at \c value.
 * \param ms
 *      Write the object repeatedly until this many total ms have
 *      elapsed.
 *
 * \return
 *      The average time to write the object, in seconds.
 */
double
timeWrite(uint32_t tableId, uint64_t objectId, const void* value,
        uint32_t length, double ms)
{
    uint64_t runCycles = Cycles::fromSeconds(ms/1e03);

    // Write the value once just to warm up all the caches everywhere.
    cluster->write(tableId, objectId, value, length);

    uint64_t start = Cycles::rdtsc();
    uint64_t elapsed;
    int count = 0;
    while (true) {
        for (int i = 0; i < 10; i++) {
            cluster->write(tableId, objectId, value, length);
        }
        count += 10;
        elapsed = Cycles::rdtsc() - start;
        if (elapsed >= runCycles)
            break;
    }
    return Cycles::toSeconds(elapsed)/count;
}

/**
 * Fill a buffer with an ASCII value that can be checked later to ensure
 * that no data has been lost or corrupted.  A particular tableId and
 * objectId are incorporated into the value (under the assumption that
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
 * \param objectId
 *      This object identifier will be reflected in the value placed in the
 *      buffer.
 */
void
fillBuffer(Buffer& buffer, uint32_t size, uint32_t tableId, uint64_t objectId)
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
            "| %d: tableId 0x%x, objectId 0x%lx %s", position, tableId,
            objectId, "0123456789012345678901234567890123456789");
        uint32_t chunkLength = sizeof(chunk) - 1;
        if (chunkLength > bytesLeft) {
            chunkLength = bytesLeft;
        }
        memcpy(new(&buffer, APPEND) char[chunkLength], chunk, chunkLength);
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
 * \param objectId
 *      This object identifier should be reflected in the buffer's data.
 *
 * \return
 *      True means the buffer has the "expected" contents; false means
 *      there was an error.
 */
bool
checkBuffer(Buffer& buffer, uint32_t expectedLength, uint32_t tableId,
        uint64_t objectId)
{
    uint32_t length = buffer.getTotalLength();
    if (length != expectedLength) {
        RAMCLOUD_LOG(ERROR, "corrupted data: expected %u bytes, "
                "found %u bytes", expectedLength, length);
        return false;
    }
    Buffer comparison;
    fillBuffer(comparison, expectedLength, tableId, objectId);
    for (uint32_t i = 0; i < expectedLength; i++) {
        char c1 = *buffer.getOffset<char>(i);
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
                    static_cast<const char*>(buffer.getRange(start,
                    length)), suffix);
            return false;
        }
    }
    return true;
}

/**
 * Compute the objectId for a particular control value in a particular client.
 *
 * \param client
 *      Index of the desired client.
 * \param id
 *      Control word for the particular client.
 */
uint64_t
objectId(int client, Id id)
{
    return (client << 8) + id;
}

/**
 * Slaves invoke this function to indicate their current state.
 *
 * \param state
 *      A string identifying what the slave is doing now, such as "idle".
 */
void
setSlaveState(const char* state)
{
    cluster->write(controlTable, objectId(clientIndex, STATE), state);
}

/**
 * Read the value of an object and place it in a buffer as a null-terminated
 * string.
 *
 * \param tableId
 *      Identifier of the table containing the object.
 * \param objectId
 *      Identifier of the object within the table.
 * \param value
 *      Buffer in which to store the object's value.
 * \param size
 *      Size of buffer.
 *
 * \return
 *      The return value is a pointer to buffer, which contains the contents
 *      of the specified object, null-terminated and truncated if needed to
 *      make it fit in the buffer.
 */
char*
readObject(uint32_t tableId, uint64_t objectId, char* value, uint32_t size)
{
    Buffer buffer;
    cluster->read(tableId, objectId, &buffer);
    uint32_t actual = buffer.getTotalLength();
    if (size <= actual) {
        actual = size - 1;
    }
    buffer.copy(0, size, value);
    value[actual] = 0;
    return value;
}

/**
 * A slave invokes this function to wait for the master to issue it a
 * command other than "idle"; the string value of the command is returned.
 *
 * \param buffer
 *      Buffer in which to store the state.
 * \param size
 *      Size of buffer.
 * 
 * \return
 *      The return value is a pointer to a buffer, which now holds the
 *      command.
 */
const char*
getCommand(char* buffer, uint32_t size)
{
    while (true) {
        try {
            readObject(controlTable, objectId(clientIndex, COMMAND),
                    buffer, size);
            if (strcmp(buffer, "idle") != 0) {
                // Delete the command value so we don't process the same
                // command twice.
                cluster->remove(controlTable, objectId(clientIndex, COMMAND));
                return buffer;
            }
        }
        catch (TableDoesntExistException& e) {
        }
        catch (ObjectDoesntExistException& e) {
        }
        usleep(10000);
    }
}

/**
 * The master invokes this function to wait for a slave to respond
 * to a command and enter a particular state.  Give up if the slave
 * doesn't enter the desired state within a short time period.
 *
 * \param slave
 *      Index of the slave (1 corresponds to the first slave).
 * \param state
 *      A string identifying the desired state for the slave.
 */
void
waitSlave(int slave, const char* state)
{
    uint64_t start = Cycles::rdtsc();
    char buffer[20];
    while (true) {
        try {
            readObject(controlTable, objectId(slave, STATE), buffer,
                    sizeof(buffer));
            if (strcmp(buffer, state) == 0) {
                return;
            }
            double elapsed = Cycles::toSeconds(Cycles::rdtsc() - start);
            if (elapsed > 1.0) {
                // Slave is taking too long; time out.
                RAMCLOUD_LOG(ERROR, "Slave %d did not enter state '%s'; "
                        "current state is '%s'", slave, state, buffer);
                return;
            }
        }
        catch (ObjectDoesntExistException& e) {
        }
    }
}

/**
 * Issue a command to one or more slaves and wait for them to receive
 * the command.
 *
 * \param command
 *      A string identifying what the slave should do next.  If NULL
 *      then no command is sent; we just wait for the slaves to reach
 *      the given state.
 * \param state
 *      The state that each slave will enter once it has received the
 *      command.  NULL means don't wait for the slaves to receive the
 *      command.
 * \param firstSlave
 *      Index of the first slave to interact with.
 * \param numSlaves
 *      Total number of slaves to command.
 */
void
sendCommand(const char* command, const char* state, int firstSlave,
        int numSlaves = 1)
{
    if (command != NULL) {
        for (int i = 0; i < numSlaves; i++) {
            cluster->write(controlTable, objectId(firstSlave+i, COMMAND),
                    command);
        }
    }
    if (state != NULL) {
        for (int i = 0; i < numSlaves; i++) {
            waitSlave(firstSlave+i, state);
        }
    }
}

//----------------------------------------------------------------------
// Test functions start here
//----------------------------------------------------------------------

// Basic read and write times for objects of different sizes
void
basic()
{
    if (clientIndex != 0)
        return;
    Buffer input, output;
    int sizes[] = {100, 1000, 10000, 100000, 1000000};
    const char* ids[] = {"100", "1K", "10K", "100K", "1M"};
    char name[50], description[50];

    for (int i = 0; i < 5; i++) {
        int size = sizes[i];
        fillBuffer(input, size, dataTable, 44);
        cluster->write(dataTable, 44, input.getRange(0, size), size);
        Buffer output;
        double t = timeRead(dataTable, 44, 100, output);
        checkBuffer(output, size, dataTable, 44);

        snprintf(name, sizeof(name), "basic::read%s", ids[i]);
        snprintf(description, sizeof(description), "read single %sB object",
                ids[i]);
        printTime(name, t, description);
        snprintf(name, sizeof(name), "basic::readBw%s", ids[i]);
        snprintf(description, sizeof(description),
                "bandwidth reading %sB object", ids[i]);
        printBandwidth(name, size/t, description);
    }

    for (int i = 0; i < 5; i++) {
        int size = sizes[i];
        fillBuffer(input, size, dataTable, 44);
        cluster->write(dataTable, 44, input.getRange(0, size), size);
        Buffer output;
        double t = timeWrite(dataTable, 44, input.getRange(0, size),
                size, 100);

        // Make sure the object was properly written.
        cluster->read(dataTable, 44, &output);
        checkBuffer(output, size, dataTable, 44);

        snprintf(name, sizeof(name), "basic::write%s", ids[i]);
        snprintf(description, sizeof(description),
                "write single %sB object", ids[i]);
        printTime(name, t, description);
        snprintf(name, sizeof(name), "basic::writeBw%s", ids[i]);
        snprintf(description, sizeof(description),
                "bandwidth writing %sB object", ids[i]);
        printBandwidth(name, size/t, description);
    }
}

// Measure the time to broadcast a short value from a master to multiple slaves.
// This benchmark is also useful as a mechanism for exercising the master-slave
// communication mechanisms.
void
broadcast()
{
    if (clientIndex > 0) {
        while (true) {
            char command[20];
            char message[200];
            getCommand(command, sizeof(command));
            if (strcmp(command, "read") == 0) {
                setSlaveState("waiting");
                // Wait for a non-empty DOC string to appear.
                while (true) {
                    readObject(controlTable, objectId(0, DOC), message,
                            sizeof(message));
                    if (message[0] != 0) {
                        break;
                    }
                }
                setSlaveState(message);
            } else if (strcmp(command, "done") == 0) {
                setSlaveState("done");
                RAMCLOUD_LOG(NOTICE, "finished with %s", message);
                return;
            } else {
                RAMCLOUD_LOG(ERROR, "unknown command %s", command);
                return;
            }
        }
    }

    // RAMCLOUD_LOG(NOTICE, "master starting");
    uint64_t totalTime = 0;
    int count = 100;
    for (int i = 0; i < count; i++) {
        char message[30];
        snprintf(message, sizeof(message), "message %d", i);
        cluster->write(controlTable, objectId(clientIndex, DOC), "");
        sendCommand("read", "waiting", 1, numClients-1);
        uint64_t start = Cycles::rdtsc();
        cluster->write(controlTable, objectId(clientIndex, DOC),
                message);
        for (int slave = 1; slave < numClients; slave++) {
            waitSlave(slave, message);
        }
        uint64_t thisRun = Cycles::rdtsc() - start;
        totalTime += thisRun;
    }
    sendCommand("done", "done", 1, numClients-1);
    char description[50];
    snprintf(description, sizeof(description),
            "broadcast message to %d slaves", numClients-1);
    printTime("broadcast", Cycles::toSeconds(totalTime)/count, description);
}

// This benchmark measures the latency and server throughput for reads
// when several clients are simultaneously reading the same object.
void
readLoaded()
{
    if (clientIndex > 0) {
        // Slaves execute the following code, which creates load by
        // repeatedly reading a particular object.
        while (true) {
            char command[20];
            char doc[200];
            getCommand(command, sizeof(command));
            if (strcmp(command, "run") == 0) {
                readObject(controlTable, objectId(0, DOC), doc, sizeof(doc));
                setSlaveState("running");

                // Although the main purpose here is to generate load, we
                // also measure performance, which can be checked to ensure
                // that all clients are seeing roughly the same performance.
                // Only measure performance when the size of the object is
                // nonzero (this indicates that all clients are active)
                uint64_t start = 0;
                Buffer buffer;
                int count = 0;
                int size = 0;
                while (true) {
                    cluster->read(dataTable, 111, &buffer);
                    int currentSize = buffer.getTotalLength();
                    if (currentSize != 0) {
                        if (start == 0) {
                            start = Cycles::rdtsc();
                            size = currentSize;
                        }
                        count++;
                    } else {
                        if (start != 0)
                            break;
                    }
                }
                RAMCLOUD_LOG(NOTICE, "Average latency (size %d): %.1fus (%s)",
                        size, Cycles::toSeconds(Cycles::rdtsc() - start)
                        *1e06/count, doc);
                setSlaveState("idle");
            } else if (strcmp(command, "done") == 0) {
                setSlaveState("done");
                return;
            } else {
                RAMCLOUD_LOG(ERROR, "unknown command %s", command);
                return;
            }
        }
    }

    // The master executes the following code, which starts up zero or more
    // slaves to generate load, then times the performance of reading.
    int size = 100;
    printf("# RAMCloud read performance as a function of load (1 or more\n");
    printf("# clients all reading a single %d-byte object repeatedly).'\n",
            size);
    printf("# Generated by 'cperf readLoaded'\n");
    printf("#\n");
    printf("# numClients  readLatency(us)  throughput(total kreads/sec)\n");
    printf("#----------------------------------------------------------\n");
    for (int numSlaves = 0; numSlaves < numClients; numSlaves++) {
        char message[100];
        Buffer input, output;
        snprintf(message, sizeof(message), "%d active clients", numSlaves+1);
        cluster->write(controlTable, objectId(0, DOC), message);
        cluster->write(dataTable, 111, "");
        sendCommand("run", "running", 1, numSlaves);
        fillBuffer(input, size, dataTable, 111);
        cluster->write(dataTable, 111, input.getRange(0, size), size);
        double t = timeRead(dataTable, 111, 100, output);
        cluster->write(dataTable, 111, "");
        checkBuffer(output, size, dataTable, 111);
        printf("%5d     %10.1f          %8.0f\n", numSlaves+1, t*1e06,
                (numSlaves+1)/(1e03*t));
        sendCommand(NULL, "idle", 1, numSlaves);
    }
    sendCommand("done", "done", 1, numClients-1);
}

// Read an object that doesn't exist. This excercies some exception paths that
// are supposed to be fast. This comes up, for example, in workloads in which a
// RAMCloud is used as a cache with frequent cache misses.
void
readNotFound()
{
    if (clientIndex != 0)
        return;

    uint64_t runCycles = Cycles::fromSeconds(.1);

    // Similar to timeRead but catches the exception
    uint64_t start = Cycles::rdtsc();
    uint64_t elapsed;
    int count = 0;
    while (true) {
        for (int i = 0; i < 10; i++) {
            Buffer output;
            try {
                cluster->read(dataTable, 55, &output);
            } catch (const ObjectDoesntExistException& e) {
                continue;
            }
            throw Exception(HERE, "Object exists?");
        }
        count += 10;
        elapsed = Cycles::rdtsc() - start;
        if (elapsed >= runCycles)
            break;
    }
    double t = Cycles::toSeconds(elapsed)/count;

    printTime("readNotFound", t, "read object that doesn't exist");
}

// The following struct and table define each performance test in terms of
// a string name and a function that implements the test.
struct TestInfo {
    const char* name;             // Name of the performance test; this is
                                  // what gets typed on the command line to
                                  // run the test.
    void (*func)();               // Function that implements the test.
};
TestInfo tests[] = {
    {"basic", basic},
    {"broadcast", broadcast},
    {"readLoaded", readLoaded},
    {"readNotFound", readNotFound},
};

int
main(int argc, char *argv[])
try
{
    // Parse command-line options.
    vector<string> testNames;
    string coordinatorLocator;
    po::options_description desc("Allowed options");
    desc.add_options()
        ("clientIndex", po::value<int>(&clientIndex)->default_value(0),
                "Index of this client (first client is 0)")
        ("coordinator,C",
#if BOOST_VERSION >= 104200 // required flag introduced in Boost 1.42
                po::value<string>(&coordinatorLocator)->required(),
#else
                po::value<string>(&coordinatorLocator),
#endif
                "Service locator for the cluster coordinator (required)")
        ("help,h", "Print this help message")
        ("numClients", po::value<int>(&numClients)->default_value(1),
                "Total number of clients running")
        ("testName", po::value<vector<string>>(&testNames),
                "Name(s) of test(s) to run");
    po::positional_options_description desc2;
    desc2.add("testName", -1);
    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).
            options(desc).positional(desc2).run(), vm);
    po::notify(vm);
    // error checks need to come after the notify call
    if (vm.count("help") || coordinatorLocator.empty()) {
        std::cout << desc << '\n';
        exit(0);
    }

    cluster = new RamCloud(coordinatorLocator.c_str());
    cluster->createTable("data");
    dataTable = cluster->openTable("data");
    cluster->createTable("control");
    controlTable = cluster->openTable("control");

    if (testNames.size() == 0) {
        // No test names specified; run all tests.
        foreach (TestInfo& info, tests) {
            info.func();
        }
    } else {
        // Run only the tests that were specified on the command line.
        foreach (string& name, testNames) {
            bool foundTest = false;
            foreach (TestInfo& info, tests) {
                if (name.compare(info.name) == 0) {
                    foundTest = true;
                    info.func();
                    break;
                }
            }
            if (!foundTest) {
                printf("No test named '%s'\n", name.c_str());
            }
        }
    }
}
catch (std::exception& e) {
    RAMCLOUD_LOG(ERROR, "%s", e.what());
}
