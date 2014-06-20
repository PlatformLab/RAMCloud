/* Copyright (c) 2011-2012 Stanford University
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
// conjunction with clusterperf.py, which starts up the cluster servers
// along with one or more instances of this program. This file contains the
// low-level benchmark code.
//
// TO ADD A NEW BENCHMARK:
// 1. Decide on a symbolic name for the new test.
// 2. Write the function that implements the benchmark.  It goes in the
//    section labeled "test functions" below, in alphabetical order.  The
//    name of the function should be the same as the name of the test.
//    Tests generally work in one of two ways:
//    * The first way is to generate one or more individual metrics;
//      "basic" and "readNotFound" are examples of this style.  Be sure
//      to print results in the same way as existing tests, for consistency.
//    * The second style of test is one that generates a graph;  "readLoaded"
//      is an example of this style.  The test should output graph data in
//      gnuplot format (comma-separated values), with comments at the
//      beginning describing the data and including the name of the test
//      that generated it.
//  3. Add an entry for the new test in the "tests" table below; this is
//     used to dispatch to the test.
//  4. Add code for this test to clusterperf.py, following the instructions
//     in that file.

#include <boost/program_options.hpp>
#include <boost/version.hpp>
#include <iostream>
namespace po = boost::program_options;

#include "assert.h"
#include "RamCloud.h"
#include "CycleCounter.h"
#include "Cycles.h"
#include "KeyUtil.h"
#include "PerfCounter.h"

using namespace RAMCloud;

// Shared state for client library.
Context context(true);

// Used to invoke RAMCloud operations.
static RamCloud* cluster;

// Total number of clients that will be participating in this test.
static int numClients;

// Index of this client among all of the participating clients (between
// 0 and numClients-1).  Client 0 acts as master to control the overall
// flow of the test; the other clients are slaves that respond to
// commands from the master.
static int clientIndex;

// Value of the "--count" command-line option: used by some tests
// to determine how many times to invoke a particular operation.
static int count;

// Value of the "--size" command-line option: used by some tests to
// determine the number of bytes in each object.  -1 means the option
// wasn't specified, so each test should pick an appropriate default.
static int objectSize;

// Value of the "--numTables" command-line option: used by some tests
// to specify the number of tables to create.
static int numTables;

// Value of the "--numIndexlet" command-line option: used by some tests
// to specify the number of indexlets to create.
static int numIndexlet;

// Value of the "--numIndexes" command-line option: used by some tests
// to specify the number of indexes/object.
static int numIndexes;

// Value of the "--warmup" command-line option: in some tests this
// determines how many times to invoke the operation before starting
// measurements (e.g. to make sure that caches are loaded).
static int warmupCount;

// Identifier for table that is used for test-specific data.
uint64_t dataTable = -1;

// Identifier for table that is used to communicate between the master
// and slaves to coordinate execution of tests.
uint64_t controlTable = -1;

// The locations of objects in controlTable; each of these values is an
// offset relative to the base for a particular client, as computed by
// controlId.

enum Id {
    STATE =  0,                      // Current state of this client.
    COMMAND = 1,                     // Command issued by master for
                                     // this client.
    DOC = 2,                         // Documentation string in master's
                                     // regions; used in log messages.
    METRICS = 3,                     // Statistics returned from slaves
                                     // to masters.
};

#define MAX_METRICS 8

// The following type holds metrics for all the clients.  Each inner vector
// corresponds to one metric and contains a value from each client, indexed
// by clientIndex.
typedef std::vector<std::vector<double>> ClientMetrics;

//----------------------------------------------------------------------
// Utility functions used by the test functions
//----------------------------------------------------------------------

/**
 * Generate a random string.
 *
 * \param str
 *      Pointer to location where the string generated will be stored.
 * \param length
 *      Length of the string to be generated in bytes.
 */
void
genRandomString(char* str, const int length) {
    static const char alphanum[] =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    for (int i = 0; i < length; ++i) {
        str[i] = alphanum[generateRandom() % (sizeof(alphanum) - 1)];
    }
}

/**
 * Print a performance measurement consisting of a time value.
 *
 * \param name
 *      Symbolic name for the measurement, in the form test.value
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
        printf("%5.1f ns   ", 1e09*seconds);
    } else if (seconds < 1.0e-03) {
        printf("%5.1f us   ", 1e06*seconds);
    } else if (seconds < 1.0) {
        printf("%5.1f ms   ", 1e03*seconds);
    } else {
        printf("%5.1f s    ", seconds);
    }
    printf("  %s\n", description);
}

/**
 * Print a performance measurement consisting of a bandwidth.
 *
 * \param name
 *      Symbolic name for the measurement, in the form test.value
 *      where \c test is the name of the test that generated the result,
 *      \c value is a name for the particular measurement.
 * \param bandwidth
 *      Measurement in units of bytes/second.
 * \param description
 *      Longer string (but not more than 20-30 chars) with a human-
 *      readable explanation of what the value refers to.
 */
void
printBandwidth(const char* name, double bandwidth, const char* description)
{
    double gb = 1024.0*1024.0*1024.0;
    double mb = 1024.0*1024.0;
    double kb = 1024.0;
    printf("%-20s ", name);
    if (bandwidth > gb) {
        printf("%5.1f GB/s ", bandwidth/gb);
    } else if (bandwidth > mb) {
        printf("%5.1f MB/s ", bandwidth/mb);
    } else if (bandwidth >kb) {
        printf("%5.1f KB/s ", bandwidth/kb);
    } else {
        printf("%5.1f B/s  ", bandwidth);
    }
    printf("  %s\n", description);
}

/**
 * Print a performance measurement consisting of a rate.
 *
 * \param name
 *      Symbolic name for the measurement, in the form test.value
 *      where \c test is the name of the test that generated the result,
 *      \c value is a name for the particular measurement.
 * \param value
 *      Measurement in units 1/second.
 * \param description
 *      Longer string (but not more than 20-30 chars) with a human-
 *      readable explanation of what the value refers to.
 */
void
printRate(const char* name, double value, const char* description)
{
    printf("%-20s  ", name);
    if (value > 1e09) {
        printf("%5.1f G/s  ", value/1e09);
    } else if (value > 1e06) {
        printf("%5.1f M/s  ", value/1e06);
    } else if (value > 1e03) {
        printf("%5.1f K/s  ", value/1e03);
    } else {
        printf("%5.1f /s   ", value);
    }
    printf("  %s\n", description);
}

/**
 * Print a performance measurement consisting of a percentage.
 *
 * \param name
 *      Symbolic name for the measurement, in the form test.value
 *      where \c test is the name of the test that generated the result,
 *      \c value is a name for the particular measurement.
 * \param value
 *      Measurement in units of %.
 * \param description
 *      Longer string (but not more than 20-30 chars) with a human-
 *      readable explanation of what the value refers to.
 */
void
printPercent(const char* name, double value, const char* description)
{
    printf("%-20s    %.1f %%      %s\n", name, value, description);
}

/**
 * Time how long it takes to do an indexed write/overwrite.
 *
 * \param tableId
 *      Table containing the object.
 * \param numKeys
 *      Number of keys in the object
 * \param keyList
 *      Information about all the keys in the object
 * \param buf
 *      Pointer to the object's value
 * \param length
 *      Size in bytes of the object's value.
 * \param [out] writeTimes
 *      Records individual experiment indexed write times
 * \param [out] overWriteTimes
 *      Records individual experiment indexed overwrite times
 */
void
timeIndexWrite(uint64_t tableId, uint8_t numKeys, KeyInfo *keyList,
               const void* buf, uint32_t length,
               std::vector<double>& writeTimes,
               std::vector<double>& overWriteTimes)
{
    //warming up
    cluster->write(tableId, numKeys, keyList, buf, length);
    cluster->remove(tableId, keyList[0].key, keyList[0].keyLength);
    Cycles::sleep(100);

    uint64_t timeWrite = 0;
    uint64_t timeOverwrite = 0;
    uint64_t timeTaken = 0;
    // record many measurements for each point and then take
    // relevant statistics
    int count = 1000;

    // record the individual times as well
    writeTimes.resize(count);
    overWriteTimes.resize(count);

    uint64_t start;
    for (int i = 0; i < count; i++) {
        start = Cycles::rdtsc();
        cluster->write(tableId, numKeys, keyList, buf, length);
        timeTaken = Cycles::rdtsc() - start;
        timeWrite += timeTaken;

        writeTimes[i] = Cycles::toSeconds(timeTaken);

        Cycles::sleep(100);

        start = Cycles::rdtsc();
        cluster->write(tableId, numKeys, keyList, buf, length);
        timeTaken = Cycles::rdtsc() - start;
        timeOverwrite += timeTaken;

        overWriteTimes[i] = Cycles::toSeconds(timeTaken);

        Cycles::sleep(100);

        cluster->remove(tableId, keyList[0].key, keyList[0].keyLength);
        Cycles::sleep(100);
    }

    //final write to facilitate lookup afterwords
    cluster->write(tableId, numKeys, keyList, buf, length);
}

/**
 * Measure lookup and lookup+indexedRead times
 *
 * \param tableId
 *      Id of the table in which lookup is to be done.
 * \param indexId
 *      Id of the index for which keys have to be compared.
 * \param pk
 *      Primary key of the object that will be returned by
 *      the indexedRead operation. This is just used for sanity
 *      checking
 * \param firstKey
 *      Starting key for the key range in which keys are to be matched.
 *      The key range includes the firstKey.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param firstKeyLength
 *      Length in bytes of the firstKey.
 * \param firstAllowedKeyHash
 *      Smallest primary key hash value allowed for firstKey.
 * \param lastKey
 *      Ending key for the key range in which keys are to be matched.
 *      The key range includes the lastKey.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param lastKeyLength
 *      Length in byes of the lastKey.
 * \param [out] lookupTimes
 *      Records individual experiment lookup timess
 * \param [out] lookupReadTimes
 *      Records individual experiment lookup+indexedRead times
 */
void
timeLookupAndIndexedRead(uint64_t tableId, uint8_t indexId, Key& pk,
                         const void* firstKey, uint16_t firstKeyLength,
                         uint64_t firstAllowedKeyHash,
                         const void* lastKey, uint16_t lastKeyLength,
                         std::vector<double>& lookupTimes,
                         std::vector<double>& lookupReadTimes)
{
    Buffer responseBufferWarmup;
    uint32_t numHashesWarmup;
    uint16_t nextKeyLengthWarmup;
    uint64_t nextKeyHashWarmup;
    //warming up
    cluster->lookupIndexKeys(tableId, indexId, firstKey, firstKeyLength,
                        firstAllowedKeyHash, lastKey, lastKeyLength,
                        &responseBufferWarmup, &numHashesWarmup,
                        &nextKeyLengthWarmup, &nextKeyHashWarmup);
    // record many measurements for each point and then take
    // relevant statistics
    int count = 1000;
    uint64_t timeTaken = 0;
    lookupTimes.resize(count);
    lookupReadTimes.resize(count);

    uint64_t start;
    for (int i = 0; i < count; i++) {

        Buffer responseBuffer;
        uint32_t numHashes;
        uint16_t nextKeyLength;
        uint64_t nextKeyHash;

        start = Cycles::rdtsc();
        cluster->lookupIndexKeys(tableId, indexId, firstKey, firstKeyLength,
                    firstAllowedKeyHash, lastKey, lastKeyLength,
                    &responseBuffer, &numHashes, &nextKeyLength, &nextKeyHash);

        timeTaken = Cycles::rdtsc() - start;

        lookupTimes[i] = Cycles::toSeconds(timeTaken);

        // verify
        uint32_t lookupOffset;
        if (numHashes != 1)
            printf("failed object, secKey:%s numHashes:%d\n",
                   static_cast<const char *>(lastKey), numHashes);
        assert(numHashes > 0);
        lookupOffset = sizeof32(WireFormat::LookupIndexKeys::Response);
        assert(pk.getHash() ==
                        *responseBuffer.getOffset<uint64_t>(lookupOffset));

        Buffer pKHashes;
        pKHashes.emplaceAppend<uint64_t>(pk.getHash());
        Buffer readResp;
        uint32_t numObjects;

        start = Cycles::rdtsc();

        cluster->indexedRead(tableId, numHashes, &pKHashes, indexId,
                    firstKey, firstKeyLength, lastKey, lastKeyLength,
                    &readResp, &numObjects);

        timeTaken += Cycles::rdtsc() - start;

        lookupReadTimes[i] = Cycles::toSeconds(timeTaken);
    }
}

/**
 * Time how long it takes to read a set of objects in one multiRead
 * operation repeatedly.
 *
 * \param requests
 *      The set of ReadObjects that encapsulate information about objects
 *      to be read.
 * \param numObjects
 *      The number of objects to be read in a single multiRead operation.
 *
 * \return
 *      The average time, in seconds, to read all the objects in a single
 *      multiRead operation.
 */
double
timeMultiRead(MultiReadObject** requests, int numObjects)
{
    // Do the multiRead once just to warm up all the caches everywhere.
    cluster->multiRead(requests, numObjects);

    uint64_t runCycles = Cycles::fromSeconds(500/1e03);
    uint64_t start = Cycles::rdtsc();
    uint64_t elapsed;
    int count = 0;
    while (true) {
        for (int i = 0; i < 10; i++) {
            cluster->multiRead(requests, numObjects);
        }
        count += 10;
        elapsed = Cycles::rdtsc() - start;
        if (elapsed >= runCycles)
            break;
    }
    return Cycles::toSeconds(elapsed)/count;
}

/**
 * Time how long it takes to write a set of objects in one multiWrite
 * operation repeatedly.
 *
 * \param requests
 *      The set of WriteObjects that encapsulate information about objects
 *      to be written.
 * \param numObjects
 *      The number of objects to be written in a single multiWrite operation.
 *
 * \return
 *      The average time, in seconds, to write all the objects in a single
 *      multiWrite operation.
 */
double
timeMultiWrite(MultiWriteObject** requests, int numObjects)
{
    uint64_t runCycles = Cycles::fromSeconds(500/1e03);
    uint64_t start = Cycles::rdtsc();
    uint64_t elapsed;
    int count = 0;
    while (true) {
        for (int i = 0; i < 10; i++) {
            cluster->multiWrite(requests, numObjects);
        }
        count += 10;
        elapsed = Cycles::rdtsc() - start;
        if (elapsed >= runCycles)
            break;
    }
    return Cycles::toSeconds(elapsed)/count;
}

/**
 * Time how long it takes to read a particular object repeatedly.
 *
 * \param tableId
 *      Table containing the object.
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 * \param keyLength
 *      Size in bytes of the key.
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
timeRead(uint64_t tableId, const void* key, uint16_t keyLength,
         double ms, Buffer& value)
{
    uint64_t runCycles = Cycles::fromSeconds(ms/1e03);

    // Read the value once just to warm up all the caches everywhere.
    cluster->read(tableId, key, keyLength, &value);

    uint64_t start = Cycles::rdtsc();
    uint64_t elapsed;
    int count = 0;
    while (true) {
        for (int i = 0; i < 10; i++) {
            cluster->read(tableId, key, keyLength, &value);
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
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 * \param keyLength
 *      Size in bytes of the key.
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
timeWrite(uint64_t tableId, const void* key, uint16_t keyLength,
          const void* value, uint32_t length, double ms)
{
    uint64_t runCycles = Cycles::fromSeconds(ms/1e03);

    // Write the value once just to warm up all the caches everywhere.
    cluster->write(tableId, key, keyLength, value, length);

    uint64_t start = Cycles::rdtsc();
    uint64_t elapsed;
    int count = 0;
    while (true) {
        for (int i = 0; i < 10; i++) {
            cluster->write(tableId, key, keyLength, value, length);
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
 * \param offset
 *      Check the data starting at this offset in the buffer.
 * \param expectedLength
 *      The buffer should contain this many bytes starting at offset.
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
checkBuffer(Buffer* buffer, uint32_t offset, uint32_t expectedLength,
        uint64_t tableId, const void* key, uint16_t keyLength)
{
    uint32_t length = buffer->getTotalLength();
    if (length != (expectedLength + offset)) {
        RAMCLOUD_LOG(ERROR, "corrupted data: expected %u bytes, "
                "found %u bytes", expectedLength, length - offset);
        return false;
    }
    Buffer comparison;
    fillBuffer(comparison, expectedLength, tableId, key, keyLength);
    for (uint32_t i = 0; i < expectedLength; i++) {
        char c1 = *buffer->getOffset<char>(offset + i);
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
                    static_cast<const char*>(buffer->getRange(offset + start,
                    length)), suffix);
            return false;
        }
    }
    return true;
}

/**
 * Compute the value to be used to construct a key for a particular
 * control value in a particular client.
 *
 * \param client
 *      Index of the desired client.
 * \param id
 *      Control word for the particular client.
 */
MakeKey
keyVal(int client, Id id)
{
    return MakeKey((client << 8) + id);
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
    MakeKey key(keyVal(clientIndex, STATE));
    cluster->write(controlTable, key.get(), key.length(), state);
}

/**
 * Read the value of an object and place it in a buffer as a null-terminated
 * string.
 *
 * \param tableId
 *      Identifier of the table containing the object.
 * \param key
 *      Variable length key that uniquely identifies the object within table.
 * \param keyLength
 *      Size in bytes of the key.
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
readObject(uint64_t tableId, const void* key, uint16_t keyLength,
           char* value, uint32_t size)
{
    Buffer buffer;
    cluster->read(tableId, key, keyLength, &buffer);
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
            MakeKey key(keyVal(clientIndex, COMMAND));
            readObject(controlTable, key.get(), key.length(), buffer, size);
            if (strcmp(buffer, "idle") != 0) {
                // Delete the command value so we don't process the same
                // command twice.
                cluster->remove(controlTable, key.get(), key.length());
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
 * Wait for a particular object to come into existence and, optionally,
 * for it to take on a particular value.  Give up if the object doesn't
 * reach the desired state within a short time period.
 *
 * \param tableId
 *      Identifier of the table containing the object.
 * \param key
 *      Variable length key that uniquely identifies the object within table.
 * \param keyLength
 *      Size in bytes of the key.
 * \param desired
 *      If non-null, specifies a string value; this function won't
 *      return until the object's value matches the string.
 * \param value
 *      The actual value of the object is returned here.
 * \param timeout
 *      Seconds to wait before giving up and throwing an Exception.
 */
void
waitForObject(uint64_t tableId, const void* key, uint16_t keyLength,
              const char* desired, Buffer& value, double timeout = 1.0)
{
    uint64_t start = Cycles::rdtsc();
    size_t length = desired ? strlen(desired) : -1;
    while (true) {
        try {
            cluster->read(tableId, key, keyLength, &value);
            if (desired == NULL) {
                return;
            }
            const char *actual = value.getStart<char>();
            if ((length == value.getTotalLength()) &&
                    (memcmp(actual, desired, length) == 0)) {
                return;
            }
            double elapsed = Cycles::toSeconds(Cycles::rdtsc() - start);
            if (elapsed > timeout) {
                // Slave is taking too long; time out.
                throw Exception(HERE, format(
                        "Object <%lu, %.*s> didn't reach desired state '%s' "
                        "(actual: '%.*s')",
                        tableId, keyLength, reinterpret_cast<const char*>(key),
                        desired, downCast<int>(value.getTotalLength()),
                        actual));
                exit(1);
            }
        }
        catch (TableDoesntExistException& e) {
        }
        catch (ObjectDoesntExistException& e) {
        }
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
 * \param timeout
 *      Seconds to wait before giving up and throwing an Exception.
 */
void
waitSlave(int slave, const char* state, double timeout = 1.0)
{
    Buffer value;
    MakeKey key(keyVal(slave, STATE));
    waitForObject(controlTable, key.get(), key.length(), state, value, timeout);
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
            MakeKey key(keyVal(firstSlave+i, COMMAND));
            cluster->write(controlTable, key.get(), key.length(), command);
        }
    }
    if (state != NULL) {
        for (int i = 0; i < numSlaves; i++) {
            waitSlave(firstSlave+i, state);
        }
    }
}

/**
 * Create one or more tables, each on a different master, and create one
 * object in each table.
 *
 * \param numTables
 *      How many tables to create.
 * \param objectSize
 *      Number of bytes in the object to create each table.
 * \param key
 *      Key to use for the created object in each table.
 * \param keyLength
 *      Size in bytes of the key.
 *
 */
uint64_t*
createTables(int numTables, int objectSize, const void* key, uint16_t keyLength)
{
    uint64_t* tableIds = new uint64_t[numTables];

    // Create the tables in backwards order to reduce possible correlations
    // between clients, tables, and servers (if we have 60 clients and 60
    // servers, with clients and servers colocated and client i accessing
    // table i, we wouldn't want each client reading a table from the
    // server on the same machine).
    for (int i = numTables-1; i >= 0;  i--) {
        char tableName[20];
        snprintf(tableName, sizeof(tableName), "table%d", i);
        cluster->createTable(tableName);
        tableIds[i] = cluster->getTableId(tableName);
        Buffer data;
        fillBuffer(data, objectSize, tableIds[i], key, keyLength);
        cluster->write(tableIds[i], key, keyLength,
                data.getRange(0, objectSize), objectSize);
    }
    return tableIds;
}

/**
 * Slaves invoke this method to return one or more performance measurements
 * back to the master.
 *
 * \param m0
 *      A performance measurement such as latency or bandwidth.  The precise
 *      meaning is defined by each individual test, and most tests only use
 *      a subset of the possible metrics.
 * \param m1
 *      Another performance measurement.
 * \param m2
 *      Another performance measurement.
 * \param m3
 *      Another performance measurement.
 * \param m4
 *      Another performance measurement.
 * \param m5
 *      Another performance measurement.
 * \param m6
 *      Another performance measurement.
 * \param m7
 *      Another performance measurement.
 */
void
sendMetrics(double m0, double m1 = 0.0, double m2 = 0.0, double m3 = 0.0,
        double m4 = 0.0, double m5 = 0.0, double m6 = 0.0, double m7 = 0.0)
{
    double metrics[MAX_METRICS];
    metrics[0] = m0;
    metrics[1] = m1;
    metrics[2] = m2;
    metrics[3] = m3;
    metrics[4] = m4;
    metrics[5] = m5;
    metrics[6] = m6;
    metrics[7] = m7;
    MakeKey key(keyVal(clientIndex, METRICS));
    cluster->write(controlTable, key.get(), key.length(),
            metrics, sizeof(metrics));
}

/**
 * Masters invoke this method to retrieved performance measurements from
 * slaves.  This method waits for slaves to fill in their metrics, if they
 * haven't already.
 *
 * \param metrics
 *      This vector of vectors is cleared and then filled with the slaves'
 *      performance data.  Each inner vector corresponds to one metric
 *      and contains a value from each of the slaves.
 * \param clientCount
 *      Metrics will be read from this many clients, starting at 0.
 */
void
getMetrics(ClientMetrics& metrics, int clientCount)
{
    // First, reset the result.
    metrics.clear();
    metrics.resize(MAX_METRICS);
    for (int i = 0; i < MAX_METRICS; i++) {
        metrics[i].resize(clientCount);
        for (int j = 0; j < clientCount; j++) {
            metrics[i][j] = 0.0;
        }
    }

    // Iterate over all the slaves to fetch metrics from each.
    for (int client = 0; client < clientCount; client++) {
        Buffer metricsBuffer;
        MakeKey key(keyVal(client, METRICS));
        waitForObject(controlTable, key.get(), key.length(),
                NULL, metricsBuffer);
        const double* clientMetrics = static_cast<const double*>(
                metricsBuffer.getRange(0,
                MAX_METRICS*sizeof32(double)));  // NOLINT
        for (int i = 0; i < MAX_METRICS; i++) {
            metrics[i][client] = clientMetrics[i];
        }
    }
}

/**
 * Return the largest element in a vector.
 *
 * \param data
 *      Input values.
 */
double
max(std::vector<double>& data)
{
    double result = data[0];
    for (int i = downCast<int>(data.size())-1; i > 0; i--) {
        if (data[i] > result)
            result = data[i];
    }
    return result;
}

/**
 * Return the smallest element in a vector.
 *
 * \param data
 *      Input values.
 */
double
min(std::vector<double>& data)
{
    double result = data[0];
    for (int i = downCast<int>(data.size())-1; i > 0; i--) {
        if (data[i] < result)
            result = data[i];
    }
    return result;
}

/**
 * Return the sum of the elements in a vector.
 *
 * \param data
 *      Input values.
 */
double
sum(std::vector<double>& data)
{
    double result = 0.0;
    for (int i = downCast<int>(data.size())-1; i >= 0; i--) {
        result += data[i];
    }
    return result;
}

/**
 * Return the average of the elements in a vector.
 *
 * \param data
 *      Input values.
 */
double
average(std::vector<double>& data)
{
    double result = 0.0;
    int length = downCast<int>(data.size());
    for (int i = length-1; i >= 0; i--) {
        result += data[i];
    }
    return result / length;
}

/**
 * Print the elements of a vector
 *
 * \param data
 *      Vector whose elements need to be printed
 */
void
printVector(std::vector<double>& data)
{
    for (std::vector<double>::iterator it = data.begin();
                                    it != data.end(); ++it)
        printf("%lf\n", 1e06*(*it));
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
    const char* key = "123456789012345678901234567890";
    uint16_t keyLength = downCast<uint16_t>(strlen(key));
    char name[50], description[50];

    for (int i = 0; i < 5; i++) {
        int size = sizes[i];
        fillBuffer(input, size, dataTable, key, keyLength);
        cluster->write(dataTable, key, keyLength,
                input.getRange(0, size), size);
        Buffer output;
        double t = timeRead(dataTable, key, keyLength, 100, output);
        checkBuffer(&output, 0, size, dataTable, key, keyLength);

        snprintf(name, sizeof(name), "basic.read%s", ids[i]);
        snprintf(description, sizeof(description),
                "read single %sB object with %uB key", ids[i], keyLength);
        printTime(name, t, description);
        snprintf(name, sizeof(name), "basic.readBw%s", ids[i]);
        snprintf(description, sizeof(description),
                "bandwidth reading %sB object with %uB key", ids[i], keyLength);
        printBandwidth(name, size/t, description);
    }

    for (int i = 0; i < 5; i++) {
        int size = sizes[i];
        fillBuffer(input, size, dataTable, key, keyLength);
        cluster->write(dataTable, key, keyLength,
                input.getRange(0, size), size);
        Buffer output;
        double t = timeWrite(dataTable, key, keyLength,
                input.getRange(0, size), size, 100);
        // Make sure the object was properly written.
        cluster->read(dataTable, key, keyLength, &output);
        checkBuffer(&output, 0, size, dataTable, key, keyLength);

        snprintf(name, sizeof(name), "basic.write%s", ids[i]);
        snprintf(description, sizeof(description),
                "write single %sB object with %uB key", ids[i], keyLength);
        printTime(name, t, description);
        snprintf(name, sizeof(name), "basic.writeBw%s", ids[i]);
        snprintf(description, sizeof(description),
                "bandwidth writing %sB object with %uB key", ids[i], keyLength);
        printBandwidth(name, size/t, description);
    }
}

// Measure the time to broadcast a short value from a master to multiple slaves
// using RAMCloud objects.  This benchmark is also useful as a mechanism for
// exercising the master-slave communication mechanisms.
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
                    MakeKey key(keyVal(0, DOC));
                    readObject(controlTable, key.get(), key.length(),
                            message, sizeof(message));
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
        MakeKey key(keyVal(clientIndex, DOC));
        cluster->write(controlTable, key.get(), key.length(), "");
        sendCommand("read", "waiting", 1, numClients-1);
        uint64_t start = Cycles::rdtsc();
        cluster->write(controlTable, key.get(), key.length(), message);
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

/**
 * This method contains the core of all the "multiRead" tests.
 * It writes objsPerMaster objects on numMasters servers
 * and reads them back in one multiRead operation.
 *
 * \param dataLength
 *      Length of data for each object to be written.
 * \param keyLength
 *      Length of key for each object to be written.
 * \param numMasters
 *      The number of master servers across which the objects written
 *      should be distributed.
 * \param objsPerMaster
 *      The number of objects to be written to each master server.
 * \param randomize
 *      Randomize the order of requests sent from the client.
 *      Note: Randomization can cause bad cache effects on the client
 *      and cause slower than normal operation.
 *
 * \return
 *      The average time, in seconds, to read all the objects in a single
 *      multiRead operation.
 */
double
doMultiRead(int dataLength, uint16_t keyLength,
            int numMasters, int objsPerMaster,
            bool randomize = false)
{
    if (clientIndex != 0)
        return 0;
    Buffer input;

    MultiReadObject requestObjects[numMasters][objsPerMaster];
    MultiReadObject* requests[numMasters][objsPerMaster];
    Tub<ObjectBuffer> values[numMasters][objsPerMaster];
    char keys[numMasters][objsPerMaster][keyLength];

    uint64_t* tableIds = createTables(numMasters, dataLength, "0", 1);

    for (int tableNum = 0; tableNum < numMasters; tableNum++) {
        for (int i = 0; i < objsPerMaster; i++) {
            genRandomString(keys[tableNum][i], keyLength);
            fillBuffer(input, dataLength, tableIds[tableNum],
                    keys[tableNum][i], keyLength);

            // Write each object to the cluster
            cluster->write(tableIds[tableNum], keys[tableNum][i], keyLength,
                    input.getRange(0, dataLength), dataLength);

            // Create read object corresponding to each object to be
            // used in the multiread request later.
            requestObjects[tableNum][i] =
                    MultiReadObject(tableIds[tableNum],
                    keys[tableNum][i], keyLength, &values[tableNum][i]);
            requests[tableNum][i] = &requestObjects[tableNum][i];
        }
    }

    // Scramble the requests. Checking code below it stays valid
    // since the value buffer is a pointer to a Buffer in the request.
    if (randomize) {
        uint64_t numRequests = numMasters*objsPerMaster;
        MultiReadObject** reqs = *requests;

        for (uint64_t i = 0; i < numRequests; i++) {
            uint64_t rand = generateRandom() % numRequests;

            MultiReadObject* tmp = reqs[i];
            reqs[i] = reqs[rand];
            reqs[rand] = tmp;
        }
    }

    double latency = timeMultiRead(*requests, numMasters*objsPerMaster);

    // Check that the values read were the same as the values written.
    for (int tableNum = 0; tableNum < numMasters; ++tableNum) {
        for (int i = 0; i < objsPerMaster; i++) {
            ObjectBuffer* output = values[tableNum][i].get();
            uint16_t offset;
            output->getValueOffset(&offset);
            checkBuffer(output, offset, dataLength, tableIds[tableNum],
                    keys[tableNum][i], keyLength);
        }
    }

    return latency;
}

/**
 * This method contains the core of all the "multiWrite" tests.
 * It writes objsPerMaster objects on numMasters servers.
 *
 * \param dataLength
 *      Length of data for each object to be written.
 * \param keyLength
 *      Length of key for each object to be written.
 * \param numMasters
 *      The number of master servers across which the objects written
 *      should be distributed.
 * \param objsPerMaster
 *      The number of objects to be written to each master server.
 * \param randomize
 *      Randomize the order of requests sent from the client.
 *      Note: Randomization can cause bad cache effects on the client
 *      and cause slower than normal operation.
 *
 * \return
 *      The average time, in seconds, to read all the objects in a single
 *      multiRead operation.
 */
double
doMultiWrite(int dataLength, uint16_t keyLength,
            int numMasters, int objsPerMaster,
            bool randomize = false)
{
    if (clientIndex != 0)
        return 0;

    // MultiWrite Objects
    MultiWriteObject writeRequestObjects[numMasters][objsPerMaster];
    MultiWriteObject* writeRequests[numMasters][objsPerMaster];
    Buffer values[numMasters][objsPerMaster];
    char keys[numMasters][objsPerMaster][keyLength];

    uint64_t* tableIds = createTables(numMasters, dataLength, "0", 1);

    for (int tableNum = 0; tableNum < numMasters; tableNum++) {
        for (int i = 0; i < objsPerMaster; i++) {
            genRandomString(keys[tableNum][i], keyLength);
            fillBuffer(values[tableNum][i], dataLength,
                    tableIds[tableNum], keys[tableNum][i], keyLength);

            // Create write object corresponding to each object to be
            // used in the multiWrite request later.
            writeRequestObjects[tableNum][i] =
                MultiWriteObject(tableIds[tableNum],
                        keys[tableNum][i], keyLength,
                        values[tableNum][i].getRange(0, dataLength),
                        dataLength);
            writeRequests[tableNum][i] = &writeRequestObjects[tableNum][i];
        }
    }

    // Scramble the requests. Checking code below it stays valid
    // since the value buffer is a pointer to a Buffer in the request.
    if (randomize) {
        uint64_t numRequests = numMasters*objsPerMaster;
        MultiWriteObject ** wreqs = *writeRequests;

        for (uint64_t i = 0; i < numRequests; i++) {
            uint64_t rand = generateRandom() % numRequests;

            MultiWriteObject* wtmp = wreqs[i];
            wreqs[i] = wreqs[rand];
            wreqs[rand] = wtmp;
        }
    }

    double latency = timeMultiWrite(*writeRequests, numMasters*objsPerMaster);

    // TODO(syang0) currently values written are unchecked, someone should do
    // this with a multiread.

    return latency;
}

// Basic index write/overwrite, lookups and indexedRead operation times.
// All objects have just one secondary key and all keys are 30 bytes long.
void
indexBasic()
{
    if (clientIndex != 0)
        return;

    // all keys (including primary key) will be 30 bytes long
    const uint32_t keyLength = 30;
    uint8_t indexId = 1;
    uint8_t numIndexlets = 1;
    cluster->createIndex(dataTable, indexId, 0, numIndexlets);

    // number of objects in the table and in the index
    int indexSizes[] = {1, 10, 100, 1000, 10000, 100000, 1000000};
    int maxNumObjects = indexSizes[6];

    // each object has only 1 secondary key because we are only measuring basic
    // indexing performance.
    uint8_t numKeys = 2;
    int size = 100; // value size
    uint64_t firstAllowedKeyHash = 0;

    printf("# RAMCloud index write, overwrite, lookup and read performance"
            " with varying number of objects.\n"
            "# All keys are 30 bytes and the value of the object is fixed"
            " to be 100 bytes.\n"
            "# Write and overwrite latencies are measured for the 'nth' object"
            " insertion where the size of the\n"
            "# table is 'n-1'. Lookup and indexedRead latencies are measured"
            " when the size of the index is 'n'.\n"
            "# All latency measurements are printed as 10 percentile/ "
            "median/ 90 percentile.\n");

    printf("# Generated by 'clusterperf.py indexBasic'\n#\n"
           "#       n       write latency(us)        overwrite latency(us)"
           "       lookup latency(us)       lookup+read latency(us)\n"
           "#----------------------------------------------------------"
           "------------------------------------------------------------\n");

    for (int i = 0, k = 0; i < maxNumObjects; i++) {

        char primaryKey[keyLength];
        snprintf(primaryKey, sizeof(primaryKey), "%dp%0*d", i, keyLength, 0);

        char secondaryKey[keyLength];
        snprintf(secondaryKey, sizeof(secondaryKey), "b%ds%0*d", i,
                 keyLength, 0);

        KeyInfo keyList[2];
        keyList[0].keyLength = keyLength;
        keyList[0].key = primaryKey;
        keyList[1].keyLength = keyLength;
        keyList[1].key = secondaryKey;

        Buffer input;
        fillBuffer(input, size, dataTable,
                   keyList[0].key, keyList[0].keyLength);

        std::vector<double> timeWrites, timeOverWrites, timeLookups,
                            timeLookupIndexedReads;
        bool measureFlag = false;

        // record/measure only points that are in indexSizes[]
        if ((i + 1) == indexSizes[k]) {
            timeIndexWrite(dataTable, numKeys, keyList, input.getRange(0, size),
                           size, timeWrites, timeOverWrites);
            measureFlag = true;
        } else {
            cluster->write(dataTable, numKeys, keyList,
                           input.getRange(0, size), size);
        }

        if (measureFlag) {
            // measuere both lookup and lookup+indexedRead operations
            Key pk(dataTable, keyList[0].key, keyList[0].keyLength);
            timeLookupAndIndexedRead(dataTable, indexId, pk, keyList[1].key,
                keyList[1].keyLength, firstAllowedKeyHash, keyList[1].key,
                keyList[1].keyLength, timeLookups, timeLookupIndexedReads);

            // measurements for 'i+1'th object (current size of table/index = i)

            std::sort(timeWrites.begin(),
                      timeWrites.end());
            std::sort(timeOverWrites.begin(),
                      timeOverWrites.end());
            std::sort(timeLookups.begin(),
                      timeLookups.end());
            std::sort(timeLookupIndexedReads.begin(),
                      timeLookupIndexedReads.end());

            printf("%9d %9.1f/%6.1f/%6.1f %13.1f/%6.1f/%6.1f ",
                   indexSizes[k],
                   timeWrites[timeWrites.size()/10] *1e6,
                   timeWrites[timeWrites.size()/2] *1e6,
                   timeWrites[timeWrites.size()*9/10] *1e6,
                   timeOverWrites[timeOverWrites.size()/10] *1e6,
                   timeOverWrites[timeOverWrites.size()/2] *1e6,
                   timeOverWrites[timeOverWrites.size()*9/10] *1e6);

            printf("%10.1f/%6.1f/%6.1f %12.1f/%6.1f/%6.1f\n",
                   timeLookups[timeLookups.size()/10] *1e6,
                   timeLookups[timeLookups.size()/2] *1e6,
                   timeLookups[timeLookups.size()*9/10] *1e6,
                   timeLookupIndexedReads[timeLookupIndexedReads.size()/10]*1e6,
                   timeLookupIndexedReads[timeLookupIndexedReads.size()/2]*1e6,
                   timeLookupIndexedReads[timeLookupIndexedReads.size()*9/10]*
                    1e6);
            k++;
            printf("\n");
        }
    }
    cluster->dropIndex(dataTable, indexId);
}

// Index write and overwrite times for varying number of indexes/object
void
indexMultiple()
{
    if (clientIndex != 0)
        return;

    const uint32_t keyLength = 30;
    int numObjects = 1000; // size of the table/index
    // includes the primary key
    uint8_t maxNumKeys = static_cast<uint8_t>(numIndexes + 1);

    printf("# RAMCloud write/overwrite performance for %dth object "
            "insertion with varying number of index keys.\n"
            "# The size of the table is %d objects and is constant"
            " for this experiment. The latency measurements\n"
            "# are printed as 10 percentile/ median/ 90 percentile\n",
            numObjects, numObjects-1);
    printf("# Generated by 'clusterperf.py indexMultiple'\n#\n"
           "# Num secondary keys/obj        write latency (us)"
           "        overwrite latency (us)\n"
           "#---------------------------------------------------"
           "------------------------------\n");

    for (uint8_t numKeys = 0; numKeys < maxNumKeys; numKeys++) {

        // numKeys refers to the number of secondary keys
        char tableName[20];
        snprintf(tableName, sizeof(tableName), "table%d", numKeys);
        uint64_t indexTable = cluster->createTable(tableName);

        for (uint8_t z = 1; z <= numKeys; z++)
            cluster->createIndex(indexTable, z, 0);

        // records measurements for one specific value of numKeys
        std::vector<double> timeWrites, timeOverWrites;

        for (int i = 0; i < numObjects; i++) {

            KeyInfo keyList[numKeys+1];
            char key[numKeys+1][keyLength];

            // primary key
            snprintf(key[0], sizeof(key[0]), "%dp%d%0*d",
                     numKeys, i, keyLength, 0);
            keyList[0].keyLength = keyLength;
            keyList[0].key = key[0];
            for (int j = 1; j < numKeys + 1; j++) {
                snprintf(key[j], sizeof(key[j]), "b%ds%d%d%0*d",
                         numKeys, i, j, keyLength, 0);
                keyList[j].keyLength = keyLength;
                keyList[j].key = key[j];
            }

            // Keeping value size constant = 100 bytes
            uint32_t size = 100;
            char value[size];
            snprintf(value, sizeof(value), "Value %0*d", size, 0);

            // do the measurement only for the last object insertion
            if (i == numObjects - 1)
                timeIndexWrite(indexTable, (uint8_t)(numKeys+1), keyList,
                               value, sizeof32(value), timeWrites,
                               timeOverWrites);
            else
                cluster->write(indexTable, (uint8_t)(numKeys+1), keyList,
                               value, sizeof32(value));

            // verify
            Key pkey(indexTable, keyList[0].key, keyList[0].keyLength);
            for (uint8_t z = 1; z <= numKeys; z++) {
                Buffer lookupResp;
                uint32_t numHashes;
                uint16_t nextKeyLength;
                uint64_t nextKeyHash;
                uint32_t lookupOffset;
                cluster->lookupIndexKeys(indexTable, z, keyList[z].key,
                    keyList[z].keyLength, 0, keyList[z].key,
                    keyList[z].keyLength, &lookupResp, &numHashes,
                    &nextKeyLength, &nextKeyHash);

                assert(1 == numHashes);
                lookupOffset = sizeof32(
                                    WireFormat::LookupIndexKeys::Response);
                assert(pkey.getHash()==
                            *lookupResp.getOffset<uint64_t>(lookupOffset));
            }
        }

        for (uint8_t z = 1; z <= numKeys; z++)
            cluster->dropIndex(indexTable, z);
        cluster->dropTable(tableName);

        // note that this modifies the underlying vector.
        std::sort(timeWrites.begin(), timeWrites.end());
        std::sort(timeOverWrites.begin(), timeOverWrites.end());

        printf("%24d %11.1f/%6.1f/%6.1f %14.1f/%6.1f/%6.1f\n", numKeys,
               timeWrites[timeWrites.size()/10] *1e6,
               timeWrites[timeWrites.size()/2] *1e6,
               timeWrites[timeWrites.size()*9/10] *1e6,
               timeOverWrites[timeOverWrites.size()/10] *1e6,
               timeOverWrites[timeOverWrites.size()/2] *1e6,
               timeOverWrites[timeOverWrites.size()*9/10] *1e6);
    }
}

/**
 * This method contains the core of the "indexScalability" test; it is
 * shared by the master and slaves and measure lookup index operations
 * throughput. Please make sure that the dataTable is also split into multiple
 * tablets to ensure read don't bottleneck.
 *
 * \param range
 *      Range of identifiers [1, range] for the indexlets available for
 *      the test.
 * \param numObjects
 *      Total number of objects present in each indexlet.
 * \param docString
 *      Information provided by the master about this run; used
 *      in log messages.
 */
void
indexScalabilityCommonLookup(uint8_t range, int numObjects, char *docString)
{
    double ms = 1000;
    uint64_t runCycles = Cycles::fromSeconds(ms/1e03);
    uint64_t lookupStart, lookupEnd;
    uint64_t elapsed = 0;
    int count = 0;

    while (true) {

        int numRequests = range;
        Buffer lookupResp[numRequests];
        uint32_t numHashes[numRequests];
        uint16_t nextKeyLength[numRequests];
        uint64_t nextKeyHash[numRequests];
        char primaryKey[numRequests][30];
        char secondaryKey[numRequests][30];

        Tub<LookupIndexKeysRpc> rpcs[numRequests];

        for (int i =0; i < numRequests; i++){
            char firstKey = static_cast<char>(('a') +
                                    static_cast<int>(generateRandom() % range));
            int randObj = static_cast<int>(generateRandom() % numObjects);

            snprintf(primaryKey[i], sizeof(primaryKey[i]), "%c:%dp%0*d",
                                                firstKey, randObj, 30, 0);

            snprintf(secondaryKey[i], sizeof(secondaryKey[i]), "%c:%ds%0*d",
                        firstKey, randObj, 30, 0);
        }

        lookupStart = Cycles::rdtsc();
        for (int i =0; i < numRequests; i++){
            rpcs[i].construct(cluster, dataTable, (uint8_t)1, secondaryKey[i],
                    (uint16_t)30, (uint16_t)0,
                    secondaryKey[i], (uint16_t)30, &lookupResp[i]);
        }

        for (int i = 0; i < numRequests; i++){
            if (rpcs[i])
              rpcs[i]->wait(&numHashes[i], &nextKeyLength[i], &nextKeyHash[i]);
        }
        lookupEnd = Cycles::rdtsc();

        for (int i =0; i < numRequests; i++){
            Key pk(dataTable, primaryKey[i], 30);
            uint32_t lookupOffset;
            lookupOffset = sizeof32(WireFormat::LookupIndexKeys::Response);
            assert(numHashes[i] == 1);
            assert(pk.getHash()==
                            *lookupResp[i].getOffset<uint64_t>(lookupOffset));
        }

        uint64_t latency = lookupEnd - lookupStart;
        count = count + numRequests;
        elapsed += latency;
        if (elapsed >= runCycles)
            break;
    }

    double thruput = count/Cycles::toSeconds(elapsed);
    sendMetrics(thruput);
    if (clientIndex != 0) {
        RAMCLOUD_LOG(NOTICE,
        "Client:%d %s: throughput: %.1f lookups/sec",
        clientIndex, docString, thruput);
    }
}

/**
 * This method contains the core of the "indexScalability" test; it is
 * shared by the master and slaves and measure lookup and read index operations
 * throughput. Please make sure that the dataTable is also split into multiple
 * tablets to ensure read don't bottleneck.
 *
 * \param range
 *      Range of identifiers [1, range] for the indexlets available for
 *      the test.
 * \param numObjects
 *      Total number of objects present in each indexlet.
 * \param docString
 *      Information provided by the master about this run; used
 *      in log messages.
 */
void
indexScalabilityCommonLookupRead(uint8_t range, int numObjects, char *docString)
{
    double ms = 1000;
    uint64_t runCycles = Cycles::fromSeconds(ms/1e03);
    uint64_t lookupStart;
    uint64_t readEnd;
    uint64_t elapsed = 0;
    int count = 0;

    while (true) {

        int numRequests = range;
        Buffer lookupResp[numRequests];
        uint32_t numHashes[numRequests];
        uint16_t nextKeyLength[numRequests];
        uint64_t nextKeyHash[numRequests];
        char primaryKey[numRequests][30];
        char secondaryKey[numRequests][30];

        Tub<IndexedReadRpc> readRpcs[numRequests];
        Tub<LookupIndexKeysRpc> rpcs[numRequests];

        uint32_t readNumObjects[numRequests];
        Buffer pKHashes[numRequests];
        Buffer readResp[numRequests];

        for (int i = 0; i < numRequests; i++) {
            char firstKey = static_cast<char>(('a') +
                                    static_cast<int>(generateRandom() % range));
            int randObj = static_cast<int>(generateRandom() % numObjects);

            snprintf(primaryKey[i], sizeof(primaryKey[i]), "%c:%dp%0*d",
                                                firstKey, randObj, 30, 0);

            snprintf(secondaryKey[i], sizeof(secondaryKey[i]), "%c:%ds%0*d",
                        firstKey, randObj, 30, 0);
            Key pk(dataTable, primaryKey[i], 30);
            pKHashes[i].emplaceAppend<uint64_t>(pk.getHash());
        }

        lookupStart = Cycles::rdtsc();
        for (int i =0; i < numRequests; i++){
            rpcs[i].construct(cluster, dataTable, (uint8_t)1, secondaryKey[i],
                    (uint16_t)30, (uint16_t)0,
                    secondaryKey[i], (uint16_t)30, &lookupResp[i]);
        }

        for (int i = 0; i < numRequests; i++){
            if (rpcs[i]) {
              rpcs[i]->wait(&numHashes[i], &nextKeyLength[i], &nextKeyHash[i]);
              readRpcs[i].construct(cluster, dataTable, numHashes[i],
                  &pKHashes[i], (uint8_t)1, secondaryKey[i], (uint16_t)30,
                  secondaryKey[i], (uint16_t)30, &readResp[i]);
            }
        }

        for (int i = 0; i < numRequests; i++){
            if (readRpcs[i])
                readRpcs[i]->wait(&readNumObjects[i]);
        }
        readEnd = Cycles::rdtsc();

        //verify
        for (int i = 0; i < numRequests; i++) {
            Key pk(dataTable, primaryKey[i], 30);
            uint32_t lookupOffset;
            lookupOffset = sizeof32(WireFormat::LookupIndexKeys::Response);
            assert(numHashes[i] == 1);
            assert(readNumObjects[i] == 1);
            assert(pk.getHash() ==
                            *lookupResp[i].getOffset<uint64_t>(lookupOffset));
        }

        uint64_t latency = readEnd - lookupStart;
        count = count + numRequests;
        elapsed += latency;
        if (elapsed >= runCycles)
            break;
    }

    double thruput = count/Cycles::toSeconds(elapsed);
    sendMetrics(thruput);
    if (clientIndex != 0) {
        RAMCLOUD_LOG(NOTICE,
        "Client:%d %s: throughput: %.1f lookups/sec",
                                clientIndex, docString, thruput);
    }
}

// In this test all of the clients repeatedly lookup and/or read objects
// from a collection of indexlets on a single table.  For each lookup/read a
// client chooses an indexlet at random.
void
indexScalability()
{
    uint8_t numIndexlets = (uint8_t)numIndexlet;
    int numObjectsPerIndexlet = 1000;

    if (clientIndex > 0) {
        while (true) {
            char command[20];
            char doc[200];
            getCommand(command, sizeof(command));
            if (strcmp(command, "run") == 0) {
                MakeKey controlKey(keyVal(0, DOC));
                readObject(controlTable, controlKey.get(), controlKey.length(),
                        doc, sizeof(doc));
                setSlaveState("running");
                indexScalabilityCommonLookup(numIndexlets,
                                            numObjectsPerIndexlet, doc);
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

    //dataset parameters
    uint8_t indexId = 1;
    uint8_t numKeys = 2;
    uint64_t firstAllowedKeyHash = 0;
    int size = 100;

    //insert objects in dataset
    cluster->createIndex(dataTable, indexId, 0, numIndexlets);
    for (int j = 0; j < numIndexlets; j++) {

        char firstKey = static_cast<char>('a'+j);
        for (int i = 0; i < numObjectsPerIndexlet; i++) {
            char primaryKey[30];
            snprintf(primaryKey, sizeof(primaryKey), "%c:%dp%0*d",
                                                firstKey, i, 30, 0);

            char secondaryKey[30];
            snprintf(secondaryKey, sizeof(secondaryKey), "%c:%ds%0*d",
                                                    firstKey, i, 30, 0);

            KeyInfo keyList[2];
            keyList[0].keyLength = 30;
            keyList[0].key = primaryKey;
            keyList[1].keyLength = 30;
            keyList[1].key = secondaryKey;

            Buffer input;
            fillBuffer(input, size, dataTable,
                       keyList[0].key, keyList[0].keyLength);

            cluster->write(dataTable, numKeys, keyList,
                                            input.getRange(0, size), size);

            Key pk(dataTable, keyList[0].key, keyList[0].keyLength);
            Buffer lookupResp;
            uint32_t numHashes;
            uint16_t nextKeyLength;
            uint64_t nextKeyHash;
            cluster->lookupIndexKeys(dataTable, indexId, keyList[1].key,
                keyList[1].keyLength, firstAllowedKeyHash, keyList[1].key,
                keyList[1].keyLength, &lookupResp, &numHashes, &nextKeyLength,
                &nextKeyHash);

            //verify
            uint32_t lookupOffset;
            assert(numHashes == 1);
            lookupOffset = sizeof32(WireFormat::LookupIndexKeys::Response);
            assert(pk.getHash() ==
                            *lookupResp.getOffset<uint64_t>(lookupOffset));
        }
    }

    // Vary the number of clients and repeat the test for each number.
    printf("# RAMCloud index scalability when 1 or more clients lookup/read\n");
    printf("# %d-byte objects with 30-byte keys chosen at random from\n"
           "# %d indexlets.\n", size, numIndexlets);
    printf("# Generated by 'clusterperf.py indexScalability'\n");
    printf("#\n");
    printf("# numClients  throughput(klookups/sec)\n");
    printf("#-------------------------------------\n");
    fflush(stdout);
    double maximum = 0.0;
    for (int numActive = 1; numActive <= numClients; numActive++) {
        char doc[100];
        snprintf(doc, sizeof(doc), "%d active clients", numActive);
        MakeKey key(keyVal(0, DOC));
        cluster->write(controlTable, key.get(), key.length(), doc);
        sendCommand("run", "running", 1, numActive-1);
        indexScalabilityCommonLookup(numIndexlets, numObjectsPerIndexlet, doc);
        sendCommand(NULL, "idle", 1, numActive-1);
        ClientMetrics metrics;
        getMetrics(metrics, numActive);
        double thruput = sum(metrics[0])/1e03;
        if (thruput > maximum)
            maximum = thruput;
        printf("%3d               %6.0f\n", numActive, thruput);
        fflush(stdout);
    }
    sendCommand("done", "done", 1, numClients-1);
    cluster->dropIndex(dataTable, indexId);
}

// This benchmark measures the multiread times for 100B objects with 30B keys
// distributed across multiple master servers such that there is one
// object located on each master server.
void
multiRead_oneObjectPerMaster()
{
    int dataLength = 100;
    uint16_t keyLength = 30;

    printf("# RAMCloud multiRead performance for %u B objects"
           " with %u byte keys\n", dataLength, keyLength);
    printf("# with one object located on each master.\n");
    printf("# Generated by 'clusterperf.py multiRead_oneObjectPerMaster'\n#\n");
    printf("# Num Objs    Num Masters    Objs/Master    "
           "Latency (us)    Latency/Obj (us)\n");
    printf("#--------------------------------------------------------"
            "--------------------\n");

    int objsPerMaster = 1;
    int maxNumMasters = numTables;

    for (int numMasters = 1; numMasters <= maxNumMasters; numMasters++) {
        double latency =
            doMultiRead(dataLength, keyLength, numMasters, objsPerMaster);
        printf("%10d %14d %14d %14.1f %18.2f\n",
            numMasters*objsPerMaster, numMasters, objsPerMaster,
            1e06*latency, 1e06*latency/numMasters/objsPerMaster);
    }
}

// This benchmark measures the multiread times for multiple
// 100B objects with 30B keys on a single master server.
void
multiRead_oneMaster()
{
    int dataLength = 100;
    uint16_t keyLength = 30;

    printf("# RAMCloud multiRead performance for %u B objects"
           " with %u byte keys\n", dataLength, keyLength);
    printf("# located on a single master.\n");
    printf("# Generated by 'clusterperf.py multiRead_oneMaster'\n#\n");
    printf("# Num Objs    Num Masters    Objs/Master    "
           "Latency (us)    Latency/Obj (us)\n");
    printf("#--------------------------------------------------------"
            "--------------------\n");

    int numMasters = 1;
    int maxObjsPerMaster = 5000;

    for (int objsPerMaster = 1; objsPerMaster <= maxObjsPerMaster;
         objsPerMaster = (objsPerMaster < 10) ?
            objsPerMaster + 1 : (objsPerMaster < 100) ?
            objsPerMaster + 10 : (objsPerMaster < 1000) ?
                objsPerMaster + 100 : objsPerMaster + 1000) {

        double latency =
            doMultiRead(dataLength, keyLength, numMasters, objsPerMaster);
        printf("%10d %14d %14d %14.1f %18.2f\n",
            numMasters*objsPerMaster, numMasters, objsPerMaster,
            1e06*latency, 1e06*latency/numMasters/objsPerMaster);
    }
}

// This benchmark measures the multiread times for an approximately
// fixed number of 100B objects with 30B keys distributed evenly
// across varying number of master servers.
void
multiRead_general()
{
    int dataLength = 100;
    uint16_t keyLength = 30;

    printf("# RAMCloud multiRead performance for "
           "an approximately fixed number\n");
    printf("# of %u B objects with %u byte keys\n", dataLength, keyLength);
    printf("# distributed evenly across varying number of masters.\n");
    printf("# Generated by 'clusterperf.py multiRead_general'\n#\n");
    printf("# Num Objs    Num Masters    Objs/Master    "
           "Latency (us)    Latency/Obj (us)\n");
    printf("#--------------------------------------------------------"
            "--------------------\n");

    int totalObjs = 5000;
    int maxNumMasters = numTables;

    for (int numMasters = 1; numMasters <= maxNumMasters; numMasters++) {
        int objsPerMaster = totalObjs / numMasters;
        double latency =
            doMultiRead(dataLength, keyLength, numMasters, objsPerMaster);
        printf("%10d %14d %14d %14.1f %18.2f\n",
            numMasters*objsPerMaster, numMasters, objsPerMaster,
            1e06*latency, 1e06*latency/numMasters/objsPerMaster);
    }
}

// This benchmark measures the multiread times for an approximately
// fixed number of 100B objects with 30B keys distributed evenly
// across varying number of master servers. Requests are issued
// in a random order.
void
multiRead_generalRandom()
{
    int dataLength = 100;
    uint16_t keyLength = 30;

    printf("# RAMCloud multiRead performance for "
           "an approximately fixed number\n");
    printf("# of %u B objects with %u byte keys\n", dataLength, keyLength);
    printf("# distributed evenly across varying number of masters.\n");
    printf("# Requests are issued in a random order.\n");
    printf("# Generated by 'clusterperf.py multiRead_generalRandom'\n#\n");
    printf("# Num Objs    Num Masters    Objs/Master    "
           "Latency (us)    Latency/Obj (us)\n");
    printf("#--------------------------------------------------------"
            "--------------------\n");

    int totalObjs = 5000;
    int maxNumMasters = numTables;

    for (int numMasters = 1; numMasters <= maxNumMasters; numMasters++) {
        int objsPerMaster = totalObjs / numMasters;
        double latency =
            doMultiRead(dataLength, keyLength, numMasters, objsPerMaster, true);
        printf("%10d %14d %14d %14.1f %18.2f\n",
            numMasters*objsPerMaster, numMasters, objsPerMaster,
            1e06*latency, 1e06*latency/numMasters/objsPerMaster);
    }
}

// This benchmark measures the multiwrite times for multiple
// 100B objects with 30B keys on a single master server.
void
multiWrite_oneMaster()
{
    int numMasters = 1;
    int dataLength = 100;
    uint16_t keyLength = 30;
    int maxObjsPerMaster = 5000;

    printf("# RAMCloud multiWrite performance for %u B objects"
           " with %u byte keys\n", dataLength, keyLength);
    printf("# located on a single master.\n");
    printf("# Generated by 'clusterperf.py multiWrite_oneMaster'\n#\n");
    printf("# Num Objs    Num Masters    Objs/Master    "
           "Latency (us)    Latency/Obj (us)\n");
    printf("#--------------------------------------------------------"
            "--------------------\n");

    for (int objsPerMaster = 1; objsPerMaster <= maxObjsPerMaster;
         objsPerMaster = (objsPerMaster < 10) ?
            objsPerMaster + 1 : (objsPerMaster < 100) ?
            objsPerMaster + 10 : (objsPerMaster < 1000) ?
                objsPerMaster + 100 : objsPerMaster + 1000) {

        double latency =
            doMultiWrite(dataLength, keyLength, numMasters, objsPerMaster);
        printf("%10d %14d %14d %14.1f %18.2f\n",
            numMasters*objsPerMaster, numMasters, objsPerMaster,
            1e06*latency, 1e06*latency/numMasters/objsPerMaster);
    }

}

// This benchmark measures overall network bandwidth using many clients, each
// reading repeatedly a single large object on a different server.  The goal
// is to stress the internal network switching fabric without overloading any
// particular client or server.
void
netBandwidth()
{
    const char* key = "123456789012345678901234567890";
    uint16_t keyLength = downCast<uint16_t>(strlen(key));

    // Duration of the test, in ms.
    int ms = 100;

    if (clientIndex > 0) {
        // Slaves execute the following code.  First, wait for the master
        // to set everything up, then open the table we will use.
        char command[20];
        getCommand(command, sizeof(command));
        char tableName[20];
        snprintf(tableName, sizeof(tableName), "table%d", clientIndex);
        uint64_t tableId = cluster->getTableId(tableName);
        RAMCLOUD_LOG(NOTICE, "Client %d reading from table %lu", clientIndex,
                tableId);
        setSlaveState("running");

        // Read a value from the table repeatedly, and compute bandwidth.
        Buffer value;
        double latency = timeRead(tableId, key, keyLength, ms, value);
        double bandwidth = value.getTotalLength()/latency;
        sendMetrics(bandwidth);
        setSlaveState("done");
        RAMCLOUD_LOG(NOTICE,
                "Bandwidth (%u-byte object with %u-byte key): %.1f MB/sec",
                value.getTotalLength(), keyLength, bandwidth/(1024*1024));
        return;
    }

    // The master executes the code below.  First, create a table for each
    // slave, with a single object.

    int size = objectSize;
    if (size < 0)
        size = 1024*1024;
    uint64_t* tableIds = createTables(numClients, objectSize, key, keyLength);

    // Start all the slaves running, and read our own local object.
    sendCommand("run", "running", 1, numClients-1);
    RAMCLOUD_LOG(DEBUG, "Master reading from table %lu", tableIds[0]);
    Buffer value;
    double latency = timeRead(tableIds[0], key, keyLength, 100, value);
    double bandwidth = value.getTotalLength()/latency;
    sendMetrics(bandwidth);

    // Collect statistics.
    ClientMetrics metrics;
    getMetrics(metrics, numClients);
    RAMCLOUD_LOG(DEBUG,
            "Bandwidth (%u-byte object with %u-byte key): %.1f MB/sec",
            value.getTotalLength(), keyLength, bandwidth/(1024*1024));

    printBandwidth("netBandwidth", sum(metrics[0]),
            "many clients reading from different servers");
    printBandwidth("netBandwidth.max", max(metrics[0]),
            "fastest client");
    printBandwidth("netBandwidth.min", min(metrics[0]),
            "slowest client");
}

// Each client reads a single object from each master.  Good for
// testing that each host in the cluster can send/receive RPCs
// from every other host.
void
readAllToAll()
{
    const char* key = "123456789012345678901234567890";
    uint16_t keyLength = downCast<uint16_t>(strlen(key));

    if (clientIndex > 0) {
        char command[20];
        do {
            getCommand(command, sizeof(command));
            usleep(10 * 1000);
        } while (strcmp(command, "run") != 0);
        setSlaveState("running");

        for (int tableNum = 0; tableNum < numTables; ++tableNum) {
            string tableName = format("table%d", tableNum);
            try {
                uint64_t tableId = cluster->getTableId(tableName.c_str());

                Buffer result;
                uint64_t startCycles = Cycles::rdtsc();
                ReadRpc read(cluster, tableId, key, keyLength, &result);
                while (!read.isReady()) {
                    context.dispatch->poll();
                    double secsWaiting =
                        Cycles::toSeconds(Cycles::rdtsc() - startCycles);
                    if (secsWaiting > 1.0) {
                        RAMCLOUD_LOG(ERROR,
                                    "Client %d couldn't read from table %s",
                                    clientIndex, tableName.c_str());
                        read.cancel();
                        continue;
                    }
                }
                read.wait();
            } catch (ClientException& e) {
                RAMCLOUD_LOG(ERROR,
                    "Client %d got exception reading from table %s: %s",
                    clientIndex, tableName.c_str(), e.what());
            } catch (...) {
                RAMCLOUD_LOG(ERROR,
                    "Client %d got unknown exception reading from table %s",
                    clientIndex, tableName.c_str());
            }
        }
        setSlaveState("done");
        return;
    }

    int size = objectSize;
    if (size < 0)
        size = 100;
    uint64_t* tableIds = createTables(numTables, size, key, keyLength);

    for (int i = 0; i < numTables; ++i) {
        uint64_t tableId = tableIds[i];
        Buffer result;
        uint64_t startCycles = Cycles::rdtsc();
        ReadRpc read(cluster, tableId, key, keyLength, &result);
        while (!read.isReady()) {
            context.dispatch->poll();
            if (Cycles::toSeconds(Cycles::rdtsc() - startCycles) > 1.0) {
                RAMCLOUD_LOG(ERROR,
                            "Master client %d couldn't read from tableId %lu",
                            clientIndex, tableId);
                return;
            }
        }
        read.wait();
    }

    for (int slaveIndex = 1; slaveIndex < numClients; ++slaveIndex) {
        sendCommand("run", "running", slaveIndex);
        // Give extra time if clients have to contact a lot of masters.
        waitSlave(slaveIndex, "done", 1.0 + 0.1 * numTables);
    }

    delete[] tableIds;
}
// Read a single object many times, and compute a cumulative distribution
// of read times.
void
readDist()
{
    if (clientIndex != 0)
        return;

    // Create an object to read, and verify its contents (this also
    // loads all caches along the way).
    const char* key = "123456789012345678901234567890";
    uint16_t keyLength = downCast<uint16_t>(strlen(key));
    Buffer input, value;
    fillBuffer(input, objectSize, dataTable, key, keyLength);
    cluster->write(dataTable, key, keyLength,
                input.getRange(0, objectSize), objectSize);
    cluster->read(dataTable, key, keyLength, &value);
    checkBuffer(&value, 0, objectSize, dataTable, key, keyLength);

    // Warmup, if desired
    for (int i = 0; i < warmupCount; i++) {
        cluster->read(dataTable, key, keyLength, &value);
    }

    // Issue the reads as quickly as possible, and save the times.
    std::vector<uint64_t> ticks;
    ticks.resize(count);
    uint64_t start = Cycles::rdtsc();
    for (int i = 0; i < count; i++) {
        cluster->read(dataTable, key, keyLength, &value);
        ticks[i] = Cycles::rdtsc();
    }

    // Output the times (several comma-separated values on each line).
    int valuesInLine = 0;
    for (int i = 0; i < count; i++) {
        if (valuesInLine >= 10) {
            valuesInLine = 0;
            printf("\n");
        }
        if (valuesInLine != 0) {
            printf(",");
        }
        double micros = Cycles::toSeconds(ticks[i] - start)*1.0e06;
        printf("%.2f", micros);
        valuesInLine++;
        start = ticks[i];
    }
    printf("\n");
}

// This benchmark measures the latency and server throughput for reads
// when several clients are simultaneously reading the same object.
void
readLoaded()
{
    const char* key = "123456789012345678901234567890";
    uint16_t keyLength = downCast<uint16_t>(strlen(key));

    if (clientIndex > 0) {
        // Slaves execute the following code, which creates load by
        // repeatedly reading a particular object.
        while (true) {
            char command[20];
            char doc[200];
            getCommand(command, sizeof(command));
            if (strcmp(command, "run") == 0) {
                MakeKey controlKey(keyVal(0, DOC));
                readObject(controlTable, controlKey.get(), controlKey.length(),
                        doc, sizeof(doc));
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
                    cluster->read(dataTable, key, keyLength, &buffer);
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
                RAMCLOUD_LOG(NOTICE, "Average latency (object size %d, "
                        "key size %u): %.1fus (%s)", size, keyLength,
                        Cycles::toSeconds(Cycles::rdtsc() - start)*1e06/count,
                        doc);
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
    int size = objectSize;
    if (size < 0)
        size = 100;
    printf("# RAMCloud read performance as a function of load (1 or more\n");
    printf("# clients all reading a single %d-byte object with %d-byte key\n"
           "# repeatedly).\n", size, keyLength);
    printf("# Generated by 'clusterperf.py readLoaded'\n");
    printf("#\n");
    printf("# numClients  readLatency(us)  throughput(total kreads/sec)\n");
    printf("#----------------------------------------------------------\n");
    for (int numSlaves = 0; numSlaves < numClients; numSlaves++) {
        char message[100];
        Buffer input, output;
        snprintf(message, sizeof(message), "%d active clients", numSlaves+1);
        MakeKey controlKey(keyVal(0, DOC));
        cluster->write(controlTable, controlKey.get(), controlKey.length(),
                message);
        cluster->write(dataTable, key, keyLength, "");
        sendCommand("run", "running", 1, numSlaves);
        fillBuffer(input, size, dataTable, key, keyLength);
        cluster->write(dataTable, key, keyLength,
                input.getRange(0, size), size);
        double t = timeRead(dataTable, key, keyLength, 100, output);
        cluster->write(dataTable, key, keyLength, "");
        checkBuffer(&output, 0, size, dataTable, key, keyLength);
        printf("%5d     %10.1f          %8.0f\n", numSlaves+1, t*1e06,
                (numSlaves+1)/(1e03*t));
        sendCommand(NULL, "idle", 1, numSlaves);
    }
    sendCommand("done", "done", 1, numClients-1);
}

// Read an object that doesn't exist. This excercises some exception paths that
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
                cluster->read(dataTable, "55", 2, &output);
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

/**
 * This method contains the core of the "readRandom" test; it is
 * shared by the master and slaves.
 *
 * \param tableIds
 *      Array of numTables identifiers for the tables available for
 *      the test.
 * \param docString
 *      Information provided by the master about this run; used
 *      in log messages.
 */
void readRandomCommon(uint64_t *tableIds, char *docString)
{
    // Duration of test.
    double ms = 100;
    uint64_t startTime = Cycles::rdtsc();
    uint64_t endTime = startTime + Cycles::fromSeconds(ms/1e03);
    uint64_t slowTicks = Cycles::fromSeconds(10e-06);
    uint64_t readStart, readEnd;
    uint64_t maxLatency = 0;
    int count = 0;
    int slowReads = 0;

    const char* key = "123456789012345678901234567890";
    uint16_t keyLength = downCast<uint16_t>(strlen(key));

    // Each iteration through this loop issues one read operation to a
    // randomly-selected table.
    while (true) {
        uint64_t tableId = tableIds[generateRandom() % numTables];
        readStart = Cycles::rdtsc();
        Buffer value;
        cluster->read(tableId, key, keyLength, &value);
        readEnd = Cycles::rdtsc();
        count++;
        uint64_t latency = readEnd - readStart;

        // When computing the slowest read, skip the first reads so that
        // everything has a chance to get fully warmed up.
        if ((latency > maxLatency) && (count > 100))
            maxLatency = latency;
        if (latency > slowTicks)
            slowReads++;
        if (readEnd > endTime)
            break;
    }
    double thruput = count/Cycles::toSeconds(readEnd - startTime);
    double slowPercent = 100.0 * slowReads / count;
    sendMetrics(thruput, Cycles::toSeconds(maxLatency), slowPercent);
    if (clientIndex != 0) {
        RAMCLOUD_LOG(NOTICE,
                "%s: throughput: %.1f reads/sec., max latency: %.1fus, "
                "reads > 20us: %.1f%%", docString,
                thruput, Cycles::toSeconds(maxLatency)*1e06, slowPercent);
    }
}

// In this test all of the clients repeatedly read objects from a collection
// of tables on different servers.  For each read a client chooses a table
// at random.
void
readRandom()
{
    uint64_t *tableIds = NULL;

    if (clientIndex > 0) {
        // This is a slave: execute commands coming from the master.
        while (true) {
            char command[20];
            char doc[200];
            getCommand(command, sizeof(command));
            if (strcmp(command, "run") == 0) {
                if (tableIds == NULL) {
                    // Open all the tables.
                    tableIds = new uint64_t[numTables];
                    for (int i = 0; i < numTables; i++) {
                        char tableName[20];
                        snprintf(tableName, sizeof(tableName), "table%d", i);
                        tableIds[i] = cluster->getTableId(tableName);
                    }
                }
                MakeKey controlKey(keyVal(0, DOC));
                readObject(controlTable, controlKey.get(), controlKey.length(),
                        doc, sizeof(doc));
                setSlaveState("running");
                readRandomCommon(tableIds, doc);
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

    // This is the master: first, create the tables.
    int size = objectSize;
    if (size < 0)
        size = 100;
    const char* key = "123456789012345678901234567890";
    uint16_t keyLength = downCast<uint16_t>(strlen(key));
    tableIds = createTables(numTables, size, key, keyLength);

    // Vary the number of clients and repeat the test for each number.
    printf("# RAMCloud read performance when 1 or more clients read\n");
    printf("# %d-byte objects with %u-byte keys chosen at random from\n"
           "# %d servers.\n", size, keyLength, numTables);
    printf("# Generated by 'clusterperf.py readRandom'\n");
    printf("#\n");
    printf("# numClients  throughput(total kreads/sec)  slowest(ms)  "
                "reads > 10us\n");
    printf("#--------------------------------------------------------"
                "------------\n");
    fflush(stdout);
    for (int numActive = 1; numActive <= numClients; numActive++) {
        char doc[100];
        snprintf(doc, sizeof(doc), "%d active clients", numActive);
        MakeKey key(keyVal(0, DOC));
        cluster->write(controlTable, key.get(), key.length(), doc);
        sendCommand("run", "running", 1, numActive-1);
        readRandomCommon(tableIds, doc);
        sendCommand(NULL, "idle", 1, numActive-1);
        ClientMetrics metrics;
        getMetrics(metrics, numActive);
        printf("%3d               %6.0f                    %6.2f"
                "          %.1f%%\n",
                numActive, sum(metrics[0])/1e03, max(metrics[1])*1e03,
                sum(metrics[2])/numActive);
        fflush(stdout);
    }
    sendCommand("done", "done", 1, numClients-1);
}

// Read times for 100B objects with string keys of different lengths.
void
readVaryingKeyLength()
{
    if (clientIndex != 0)
        return;
    Buffer input, output;
    uint16_t keyLengths[] = {
         1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50,
         55, 60, 65, 70, 75, 80, 85, 90, 95, 100,
         200, 300, 400, 500, 600, 700, 800, 900, 1000,
         2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000,
         20000, 30000, 40000, 50000, 60000
    };
    int dataLength = 100;

    printf("# RAMCloud read performance for %u B objects\n", dataLength);
    printf("# with keys of various lengths.\n");
    printf("# Generated by 'clusterperf.py readVaryingKeyLength'\n#\n");
    printf("# Key Length      Latency (us)     Bandwidth (MB/s)\n");
    printf("#--------------------------------------------------------"
            "--------------------\n");

    foreach (uint16_t keyLength, keyLengths) {
        char key[keyLength];
        genRandomString(key, keyLength);

        fillBuffer(input, dataLength, dataTable, key, keyLength);
        cluster->write(dataTable, key, keyLength,
                input.getRange(0, dataLength), dataLength);
        double t = timeRead(dataTable, key, keyLength, 100, output);
        checkBuffer(&output, 0, dataLength, dataTable, key, keyLength);

        printf("%12u %16.1f %19.1f\n", keyLength, 1e06*t,
               (keyLength / t)/(1024.0*1024.0));
    }
}

// Write times for 100B objects with string keys of different lengths.
void
writeVaryingKeyLength()
{
    if (clientIndex != 0)
        return;
    Buffer input, output;
    uint16_t keyLengths[] = {
         1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50,
         55, 60, 65, 70, 75, 80, 85, 90, 95, 100,
         200, 300, 400, 500, 600, 700, 800, 900, 1000,
         2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000,
         20000, 30000, 40000, 50000, 60000
    };
    int dataLength = 100;

    printf("# RAMCloud write performance for %u B objects\n", dataLength);
    printf("# with keys of various lengths.\n");
    printf("# Generated by 'clusterperf.py writeVaryingKeyLength'\n#\n");
    printf("# Key Length      Latency (us)     Bandwidth (MB/s)\n");
    printf("#--------------------------------------------------------"
            "--------------------\n");

    foreach (uint16_t keyLength, keyLengths) {
        char key[keyLength];
        genRandomString(key, keyLength);

        fillBuffer(input, dataLength, dataTable, key, keyLength);
        cluster->write(dataTable, key, keyLength,
                input.getRange(0, dataLength), dataLength);
        double t = timeWrite(dataTable, key, keyLength,
                input.getRange(0, dataLength), dataLength, 100);
        Buffer output;
        cluster->read(dataTable, key, keyLength, &output);
        checkBuffer(&output, 0, dataLength, dataTable, key, keyLength);

        printf("%12u %16.1f %19.1f\n", keyLength, 1e06*t,
               (keyLength / t)/(1024.0*1024.0));
    }
}

// This benchmark measures the latency and server throughput for write
// when some data is written asynchronously and then some smaller value
// is written synchronously.
void
writeAsyncSync()
{
    if (clientIndex > 0)
        return;

    const uint32_t count = 100;
    const uint32_t syncObjectSize = 100;
    const uint32_t asyncObjectSizes[] = { 100, 1000, 10000, 100000, 1000000 };
    const uint32_t arrayElts = static_cast<uint32_t>
                               (sizeof32(asyncObjectSizes) /
                               sizeof32(asyncObjectSizes[0]));

    uint32_t maxSize = syncObjectSize;
    for (uint32_t j = 0; j < arrayElts; ++j)
        maxSize = std::max(maxSize, asyncObjectSizes[j]);

    char* garbage = new char[maxSize];

    const char* key = "123456789012345678901234567890";
    uint16_t keyLength = downCast<uint16_t>(strlen(key));

    // prime
    cluster->write(dataTable, key, keyLength, &garbage[0], syncObjectSize);
    cluster->write(dataTable, key, keyLength, &garbage[0], syncObjectSize);

    printf("# Gauges impact of asynchronous writes on synchronous writes.\n"
           "# Write two values. The size of the first varies over trials\n"
           "# (its size is given as 'firstObjectSize'). The first write is\n"
           "# either synchronous (if firstWriteIsSync is 1) or asynchronous\n"
           "# (if firstWriteIsSync is 0). The response time of the first\n"
           "# write is given by 'firstWriteLatency'. The second write is\n"
           "# a %u B object which is always written synchronously (its \n"
           "# response time is given by 'syncWriteLatency'\n"
           "# Both writes use a %u B key.\n"
           "# Generated by 'clusterperf.py writeAsyncSync'\n#\n"
           "# firstWriteIsSync firstObjectSize firstWriteLatency(us) "
               "syncWriteLatency(us)\n"
           "#--------------------------------------------------------"
               "--------------------\n",
           syncObjectSize, keyLength);
    for (int sync = 0; sync < 2; ++sync) {
        for (uint32_t j = 0; j < arrayElts; ++j) {
            const uint32_t asyncObjectSize = asyncObjectSizes[j];
            uint64_t asyncTicks = 0;
            uint64_t syncTicks = 0;
            for (uint32_t i = 0; i < count; ++i) {
                {
                    CycleCounter<> _(&asyncTicks);
                    cluster->write(dataTable, key, keyLength, &garbage[0],
                                   asyncObjectSize, NULL, NULL, !sync);
                }
                {
                    CycleCounter<> _(&syncTicks);
                    cluster->write(dataTable, key, keyLength, &garbage[0],
                                   syncObjectSize);
                }
            }
            printf("%18d %15u %21.1f %20.1f\n", sync, asyncObjectSize,
                   Cycles::toSeconds(asyncTicks) * 1e6 / count,
                   Cycles::toSeconds(syncTicks) * 1e6 / count);
        }
    }

    delete garbage;
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
    {"indexBasic", indexBasic},
    {"indexMultiple", indexMultiple},
    {"indexScalability", indexScalability},
    {"multiWrite_oneMaster", multiWrite_oneMaster},
    {"multiRead_oneMaster", multiRead_oneMaster},
    {"multiRead_oneObjectPerMaster", multiRead_oneObjectPerMaster},
    {"multiRead_general", multiRead_general},
    {"multiRead_generalRandom", multiRead_generalRandom},
    {"netBandwidth", netBandwidth},
    {"readAllToAll", readAllToAll},
    {"readDist", readDist},
    {"readLoaded", readLoaded},
    {"readNotFound", readNotFound},
    {"readRandom", readRandom},
    {"readVaryingKeyLength", readVaryingKeyLength},
    {"writeVaryingKeyLength", writeVaryingKeyLength},
    {"writeAsyncSync", writeAsyncSync},
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
            "Usage: ClusterPerf [options] testName testName ...\n\n"
            "Runs one or more benchmarks on a RAMCloud cluster and outputs\n"
            "performance information.  This program is not normally invoked\n"
            "directly; it is invoked by the clusterperf script.\n\n"
            "Allowed options:");
    desc.add_options()
        ("clientIndex", po::value<int>(&clientIndex)->default_value(0),
                "Index of this client (first client is 0)")
        ("coordinator,C", po::value<string>(&coordinatorLocator),
                "Service locator for the cluster coordinator (required)")
        ("count,c", po::value<int>(&count)->default_value(100000),
                "Number of times to invoke operation for test")
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
        ("numTables", po::value<int>(&numTables)->default_value(10),
                "Number of tables to use for test")
        ("testName", po::value<vector<string>>(&testNames),
                "Name(s) of test(s) to run")
        ("warmup", po::value<int>(&warmupCount)->default_value(100),
                "Number of times to invoke operation before beginning "
                "measurements")
        ("numIndexlet", po::value<int>(&numIndexlet)->default_value(1),
                "number of Indexlets")
        ("numIndexes", po::value<int>(&numIndexes)->default_value(1),
                "number of secondary keys per object");
    po::positional_options_description desc2;
    desc2.add("testName", -1);
    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).
            options(desc).positional(desc2).run(), vm);
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
        std::cout << desc << '\n';
        exit(0);
    }
    if (coordinatorLocator.empty()) {
        RAMCLOUD_LOG(ERROR, "missing required option --coordinator");
        exit(1);
    }

    RamCloud r(&context, coordinatorLocator.c_str());
    cluster = &r;
    cluster->createTable("data");
    dataTable = cluster->getTableId("data");
    cluster->createTable("control");
    controlTable = cluster->getTableId("control");

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
    exit(1);
}
