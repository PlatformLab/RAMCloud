/* Copyright (c) 2014 Stanford University
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

#include <RamCloud.h>
#include <MultiRead.h>
#include <MultiWrite.h>
#include <MultiRemove.h>

#include <Util.h>
#include "edu_stanford_ramcloud_RAMCloud.h"
#include "JavaCommon.h"

using namespace RAMCloud;

static void printBuffer(ByteBuffer buffer, uint32_t len) {
    printf("\nBuffer %p, %u: \n%s\n",
           buffer.pointer,
           buffer.mark,
           Util::hexDump(
                   static_cast<void*>(buffer.pointer),
                   len).c_str());
}


/**
 * Create a RejectRules pointer from a byte array representation of a Java
 * RejectRules object.
 *
 * \param env
 *      The current JNI environment.
 * \param jRejectRules
 *      A Java byte array holding the data for a RejectRules struct.
 * \return A RejectRules object from the given byte array.
 */
static RejectRules
createRejectRules(JNIEnv* env, jbyteArray jRejectRules) {
    RejectRules out;
    void* rulesPointer = env->GetPrimitiveArrayCritical(jRejectRules, 0);
    out.givenVersion = static_cast<uint64_t*> (rulesPointer)[0];
    out.doesntExist = static_cast<char*> (rulesPointer)[8];
    out.exists = static_cast<char*> (rulesPointer)[9];
    out.versionLeGiven = static_cast<char*> (rulesPointer)[10];
    out.versionNeGiven = static_cast<char*> (rulesPointer)[11];
    env->ReleasePrimitiveArrayCritical(jRejectRules, rulesPointer, JNI_ABORT);
    /*
    printf("Created RejectRules:\n\tVersion: %u\n\tDE: %u\n\tE: %u\n\tVLG: %u\n\tVNG: %u\n",
           out.givenVersion,
           out.doesntExist,
           out.exists,
           out.versionLeGiven,
           out.versionNeGiven); */
    return out;
}

/**
 * Gets the pointer to the memory region the specified ByteBuffer points to.
 *
 * \param env
 *      The current JNI environment.
 * \param jRamCloud
 *      The calling class.
 * \param jByteBuffer
 *      The java.nio.ByteBuffer object, created with
 *      ByteBuffer.allocateDirect(), to retrieve a pointer for.
 * \return A pointer, as a Java long, to the memory region allocated for the
 *      specified ByteBuffer.
 */
JNIEXPORT jlong
JNICALL Java_edu_stanford_ramcloud_RAMCloud_cppGetByteBufferPointer(
        JNIEnv *env,
        jclass jRamCloud,
        jobject jByteBuffer) {
    jlong out = reinterpret_cast<jlong>(env->GetDirectBufferAddress(jByteBuffer));
    return out;
}

/**
 * Construct a RamCloud for a particular cluster.
 *
 * \param env
 *      The current JNI environment.
 * \param jRamCloud
 *      The calling class.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ will communicate.
 *      The format for the input buffer is:
 *          4 bytes for length of cluster locator string
 *          NULL-terminated string for cluster locator
 *          NULL-terminated string for cluster name
 *      The format for the output buffer is:
 *          4 bytes for status code of the RamCloud constructor
 *          8 bytes for a pointer to the created RamCloud object
 */
JNIEXPORT void
JNICALL Java_edu_stanford_ramcloud_RAMCloud_cppConnect(
        JNIEnv *env,
        jclass jRamCloud,
        jlong byteBufferPointer) {
    ByteBuffer buffer(byteBufferPointer);
    uint32_t locatorLength = buffer.read<uint32_t>();
    char* locator = buffer.pointer + buffer.mark;
    buffer.mark += 1 + locatorLength;
    char* name = buffer.pointer + buffer.mark;

    RamCloud* ramcloud = NULL;
    buffer.rewind();
    try {
        ramcloud = new RamCloud(locator, name);
    } EXCEPTION_CATCHER(buffer);
    buffer.write(reinterpret_cast<uint64_t>(ramcloud));
}

/**
 * Disconnect from the RAMCloud cluster. This causes the JNI code to destroy
 * the underlying RamCloud C++ object.
 * 
 * \param env
 *      The current JNI environment.
 * \param jRamCloud
 *      The calling class.
 * \param ramcloudClusterHandle
 *      A pointer to the C++ RamCloud object.
 */
JNIEXPORT void
JNICALL Java_edu_stanford_ramcloud_RAMCloud_cppDisconnect(JNIEnv *env,
        jclass jRamCloud,
        jlong ramcloudClusterHandle) {
    delete reinterpret_cast<RamCloud*> (ramcloudClusterHandle);
}

/**
 * Create a new table.
 * 
 * \param env
 *      The current JNI environment.
 * \param jRamCloud
 *      The calling class.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ will communicate.
 *      The format for the input buffer is:
 *          8 bytes for a pointer to a C++ RamCloud object
 *          4 bytes for the number of servers across which this table will be
 *              divided
 *          NULL-terminated string for the name of the table to create
 *      The format for the output buffer is:
 *          4 bytes for the status code of the createTable operation
 *          8 bytes for the ID of the table created
 */
JNIEXPORT void
JNICALL Java_edu_stanford_ramcloud_RAMCloud_cppCreateTable(JNIEnv *env,
        jclass jRamCloud,
        jlong byteBufferPointer) {
    ByteBuffer buffer(byteBufferPointer);
    RamCloud* ramcloud = buffer.readPointer<RamCloud>();
    uint32_t serverSpan = buffer.read<uint32_t>();
    char* tableName = buffer.pointer + buffer.mark;
    uint64_t tableId;
    buffer.rewind();
    try {
        tableId = ramcloud->createTable(tableName, serverSpan);
    } EXCEPTION_CATCHER(buffer);
    buffer.write(tableId);
}

/**
 * Delete a table.
 *
 * All objects in the table are implicitly deleted, along with any
 * other information associated with the table.  If the table does
 * not currently exist then the operation returns successfully without
 * actually doing anything.
 *
 * \param env
 *      The current JNI environment.
 * \param jRamCloud
 *      The calling class.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ will communicate.
 *      The format for the input buffer is:
 *          8 bytes for a pointer to a C++ RamCloud object
 *          NULL-terminated string for the name of the table to delete
 *      The format for the output buffer is:
 *          4 bytes for the status code of the dropTable operation
 */
JNIEXPORT void
JNICALL Java_edu_stanford_ramcloud_RAMCloud_cppDropTable(JNIEnv *env,
        jclass jRamCloud,
        jlong byteBufferPointer) {
    ByteBuffer buffer(byteBufferPointer);
    RamCloud* ramcloud = buffer.readPointer<RamCloud>();
    char* tableName = buffer.pointer + buffer.mark;
    buffer.rewind();
    try {
        ramcloud->dropTable(tableName);
    } EXCEPTION_CATCHER(buffer);
}

/**
 * Given the name of a table, return the table's unique identifier, which
 * is used to access the table.
 *
 * \param env
 *      The current JNI environment.
 * \param jRamCloud
 *      The calling class.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ will communicate.
 *      The format for the input buffer is:
 *          8 bytes for a pointer to a C++ RamCloud object
 *          NULL-terminated string for the name of the table to get the ID for
 *      The format for the output buffer is:
 *          4 bytes for the status code of the getTableId operation
 *          8 bytes for the ID of the table specified
 */
JNIEXPORT void
JNICALL Java_edu_stanford_ramcloud_RAMCloud_cppGetTableId(JNIEnv *env,
        jclass jRamCloud,
        jlong byteBufferPointer) {
    ByteBuffer buffer(byteBufferPointer);
    RamCloud* ramcloud = buffer.readPointer<RamCloud>();
    char* tableName = buffer.pointer + buffer.mark;
    uint64_t tableId;
    buffer.rewind();
    try {
        tableId = ramcloud->getTableId(tableName);
    } EXCEPTION_CATCHER(buffer);
    buffer.write(tableId);
}


#if TIME_CPP
uint32_t test_num_current = 0;
const uint32_t test_num_times = 100000;

uint64_t test_times[test_num_times];
#endif

/**
 * Read the current contents of an object.
 *
 * \param env
 *      The current JNI environment.
 * \param jRamCloud
 *      The calling class.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ will communicate.
 *      The format for the input buffer is:
 *          8 bytes for a pointer to a C++ RamCloud object
 *          8 bytes for the ID of the table to read from
 *          4 bytes for the length of the key to find
 *          byte array for the key to find
 *          12 bytes representing the RejectRules
 *      The format for the output buffer is:
 *          4 bytes for the status code of the read operation
 *          8 bytes for the version of the read object
 *          4 bytes for the size of the read value
 *          byte array for the read value
 */
JNIEXPORT void
JNICALL Java_edu_stanford_ramcloud_RAMCloud_cppRead(
        JNIEnv *env,
        jclass jRamCloud,
        jlong byteBufferPointer) {
    ByteBuffer byteBuffer(byteBufferPointer);
    RamCloud* ramcloud = byteBuffer.readPointer<RamCloud>();
    uint64_t tableId = byteBuffer.read<uint64_t>();
    uint32_t keyLength = byteBuffer.read<uint32_t>();
    void* key = byteBuffer.getVoidPointer(keyLength);
    RejectRules rejectRules = byteBuffer.read<RejectRules>();
    Buffer buffer;
    uint64_t version;
    byteBuffer.rewind();
#if TIME_CPP
    uint64_t start = Cycles::rdtsc();
#endif
    try {
        ramcloud->read(tableId,
                       key,
                       keyLength,
                       &buffer,
                       &rejectRules,
                       &version);
    } EXCEPTION_CATCHER(byteBuffer);
#if TIME_CPP
    test_times[test_num_current] = Cycles::rdtsc() - start;
    test_num_current++;
    if (test_num_current == test_num_times) {
        std::sort(boost::begin(test_times), boost::end(test_times));
        printf("Median C++ Read Time: %f\n", Cycles::toSeconds(test_times[test_num_times / 2]) * 1000000);
        test_num_current = 0;
    }
#endif
    byteBuffer.write(version);
    byteBuffer.write(buffer.size());
    buffer.copy(0, buffer.size(), byteBuffer.getVoidPointer());
}

/**
 * Delete an object from a table. If the object does not currently exist
 * then the operation succeeds without doing anything (unless rejectRules
 * causes the operation to be aborted).
 *
 * \param env
 *      The current JNI environment.
 * \param jRamCloud
 *      The calling class.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ will communicate.
 *      The format for the input buffer is:
 *          8 bytes for a pointer to a C++ RamCloud object
 *          8 bytes for the ID of the table to remove from
 *          4 bytes for the length of the key to find and remove
 *          byte array for the key to find and remove
 *          12 bytes representing the RejectRules
 *      The format for the output buffer is:
 *          4 bytes for the status code of the remove operation
 *          8 bytes for the version of the object just before deletion
 */
JNIEXPORT void
JNICALL Java_edu_stanford_ramcloud_RAMCloud_cppRemove(JNIEnv *env,
        jclass jRamCloud,
        jlong byteBufferPointer) {
    ByteBuffer buffer(byteBufferPointer);
    RamCloud* ramcloud = buffer.readPointer<RamCloud>();
    uint64_t tableId = buffer.read<uint64_t>();
    uint32_t keyLength = buffer.read<uint32_t>();
    void* key = buffer.getVoidPointer(keyLength);
    RejectRules rejectRules = buffer.read<RejectRules>();
    uint64_t version;
    buffer.rewind();
    try {
        ramcloud->remove(tableId, key, keyLength, &rejectRules, &version);
    } EXCEPTION_CATCHER(buffer);
    buffer.write(version);
}

/**
 * Replace the value of a given object, or create a new object if none
 * previously existed.
 *
 * \param env
 *      The current JNI environment.
 * \param jRamCloud
 *      The calling class.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ will communicate.
 *      The format for the input buffer is:
 *          8 bytes for a pointer to a C++ RamCloud object
 *          8 bytes for the ID of the table to write to
 *          4 bytes for the length of the key to write
 *          byte array for the key to write
 *          4 bytes for the length of the value to write
 *          byte array for the valule to write
 *          12 bytes representing the RejectRules
 *      The format for the output buffer is:
 *          4 bytes for the status code of the write operation
 *          8 bytes for the version of the object written
 */
JNIEXPORT void
JNICALL Java_edu_stanford_ramcloud_RAMCloud_cppWrite(JNIEnv *env,
        jclass jRamCloud,
        jlong byteBufferPointer) {
    ByteBuffer buffer(byteBufferPointer);
    RamCloud* ramcloud = buffer.readPointer<RamCloud>();
    uint64_t tableId = buffer.read<uint64_t>();
    uint32_t keyLength = buffer.read<uint32_t>();
    void* key = buffer.getVoidPointer(keyLength);
    uint32_t valueLength = buffer.read<uint32_t>();
    void* value = buffer.getVoidPointer(valueLength);
    RejectRules rules = buffer.read<RejectRules>();
    uint64_t version;
    buffer.rewind();
#if TIME_CPP
    uint64_t start = Cycles::rdtsc();
#endif
    try {
        ramcloud->write(tableId,
                        key, keyLength,
                        value, valueLength,
                        &rules,
                        &version);
    } EXCEPTION_CATCHER(buffer);
#if TIME_CPP
    test_times[test_num_current] = Cycles::rdtsc() - start;
    test_num_current++;
    if (test_num_current == test_num_times) {
        std::sort(boost::begin(test_times), boost::end(test_times));
        printf("Median C++ Write Time: %f\n", Cycles::toSeconds(test_times[test_num_times / 2]) * 1000000);
        test_num_current = 0;
    }
#endif
    buffer.write(version);
}

