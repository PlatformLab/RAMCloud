/* Copyright (c) 2013 Stanford University
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
#include "edu_stanford_ramcloud_RAMCloud.h"

using namespace RAMCloud;

// Time C++ blocks
#define TIME_CPP true

#define check_null(var, msg)                                                \
    if (var == NULL) {                                                      \
        throw Exception(HERE, "JRamCloud: NULL returned: " msg "\n");       \
    }

/**
 * This class provides a simple means of extracting C-style strings
 * from a jstring and cleans up when the destructor is called. This
 * avoids having to manually do the annoying GetStringUTFChars /
 * ReleaseStringUTFChars dance. 
 */
class JStringGetter {
public:

    JStringGetter(JNIEnv* env, jstring jString)
    : env(env)
    , jString(jString)
    , string(env->GetStringUTFChars(jString, 0)) {
        check_null(string, "GetStringUTFChars failed");
    }

    ~JStringGetter() {
        if (string != NULL)
            env->ReleaseStringUTFChars(jString, string);
    }

private:
    JNIEnv* env;
    jstring jString;

public:
    const char* const string;
};

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
 * This macro is used to catch C++ exceptions and convert them into Java
 * exceptions. Be sure to wrap the individual RamCloud:: calls in try blocks,
 * rather than the entire methods, since doing so with functions that return
 * non-void is a bad idea with undefined(?) behaviour. 
 *
 * _returnValue is the value that should be returned from the JNI function
 * when an exception is caught and generated in Java. As far as I can tell,
 * the exception fires immediately upon returning from the JNI method. I
 * don't think anything else would make sense, but the JNI docs kind of
 * suck.
 */
#define EXCEPTION_CATCHER(_returnValue)                                 \
    catch (ClientException& e) {                                        \
        env->SetIntArrayRegion(status, 0, 1, reinterpret_cast<jint*>(&e.status)); \
        return _returnValue;                                            \
    }

/**
 * Construct a RamCloud for a particular cluster.
 *
 * \param env
 *      The current JNI environment.
 * \param jRamCloud
 *      The calling class.
 * \param locator
 *      Describes how to locate the coordinator. It can have either of
 *      two forms. The preferred form is a locator for external storage
 *      that contains the cluster configuration information (such as a
 *      string starting with "zk:", which will be passed to the ZooStorage
 *      constructor). With this form, sessions can automatically be
 *      redirected to a new coordinator if the current one crashes.
 *      Typically the value for this argument will be the same as the
 *      value of the "-x" command-line option given to the coordinator
 *      when it started. The second form is deprecated, but is retained
 *      for testing. In this form, the location is specified as a RAMCloud
 *      service locator for a specific coordinator. With this form it is
 *      not possible to roll over to a different coordinator if a given
 *      one fails; we will have to wait for the specified coordinator to
 *      restart.
 * \param clusterName
 *      Name of the current cluster. Used to allow independent operation
 *      of several clusters sharing many of the same resources. This is
 *      typically the same as the value of the "--clusterName" command-line
 *      option given to the coordinator when it started.
 * \param status
 *      Java integer array of length 1 to put the status in if there
 *      are any exceptions.
 */
JNIEXPORT jlong
JNICALL Java_edu_stanford_ramcloud_RAMCloud_cppConnect(JNIEnv *env,
        jclass jRamCloud,
        jstring locator,
        jstring clusterName,
        jintArray status) {

    JStringGetter locatorString(env, locator);
    JStringGetter name(env, clusterName);
    RamCloud* ramcloud = NULL;
    try {
        ramcloud = new RamCloud(locatorString.string, name.string);
        // Not sure what this does, but Stephen wrote it so I'll keep
        // it for now
        ramcloud->clientContext->transportManager->setSessionTimeout(10000);
    }
    EXCEPTION_CATCHER(NULL);
    return reinterpret_cast<jlong> (ramcloud);
}

/**
 * Disconnect from the RAMCloud cluster. This causes the JNI code to destroy
 * the underlying RamCloud C++ object.
 * 
 * \param env
 *      The current JNI environment.
 * \param jRamCloud
 *      The calling class.
 * \param ramcloudObjectPointer
 *      A pointer to the C++ RamCloud object.
 */
JNIEXPORT void
JNICALL Java_edu_stanford_ramcloud_RAMCloud_cppDisconnect(JNIEnv *env,
        jclass jRamCloud,
        jlong ramcloudObjectPointer) {
    delete reinterpret_cast<RamCloud*> (ramcloudObjectPointer);
}

/**
 * Create a new table.
 * 
 * \param env
 *      The current JNI environment.
 * \param jRamCloud
 *      The calling class.
 * \param ramcloudObjectPointer
 *      A pointer to the C++ RamCloud object.
 * \param jTableName
 *      Name for the new table.
 * \param jServerSpan
 *      The number of servers across which this table will be divided
 *      (defaults to 1). Keys within the table will be evenly distributed
 *      to this number of servers according to their hash. This is a temporary
 *      work-around until tablet migration is complete; until then, we must
 *      place tablets on servers statically.
 * \param status
 *      Java integer array of length 1 to put the status in if there
 *      are any exceptions.
 * \return
 *      The return value is an identifier for the created table; this is
 *      used instead of the table's name for most RAMCloud operations
 *      involving the table.
 */
JNIEXPORT jlong
JNICALL Java_edu_stanford_ramcloud_RAMCloud_cppCreateTable(JNIEnv *env,
        jclass jRamCloud,
        jlong ramcloudObjectPointer,
        jstring jTableName,
        jint jServerSpan,
        jintArray status) {
    RamCloud* ramcloud = reinterpret_cast<RamCloud*> (ramcloudObjectPointer);
    JStringGetter tableName(env, jTableName);
    uint64_t tableId;
    try {
        tableId = ramcloud->createTable(tableName.string, jServerSpan);
    }
    EXCEPTION_CATCHER(-1);
    return static_cast<jlong> (tableId);
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
 * \param ramcloudObjectPointer
 *      A pointer to the C++ RamCloud object.
 * \param jTableName
 *      Name of the table to delete.
 * \param status
 *      Java integer array of length 1 to put the status in if there
 *      are any exceptions.
 */
JNIEXPORT void
JNICALL Java_edu_stanford_ramcloud_RAMCloud_cppDropTable(JNIEnv *env,
        jclass jRamCloud,
        jlong ramcloudObjectPointer,
        jstring jTableName,
        jintArray status) {
    RamCloud* ramcloud = reinterpret_cast<RamCloud*> (ramcloudObjectPointer);
    JStringGetter tableName(env, jTableName);
    try {
        ramcloud->dropTable(tableName.string);
    }
    EXCEPTION_CATCHER();
}

/**
 * Given the name of a table, return the table's unique identifier, which
 * is used to access the table.
 *
 * \param env
 *      The current JNI environment.
 * \param jRamCloud
 *      The calling class.
 * \param ramcloudObjectPointer
 *      A pointer to the C++ RamCloud object.
 * \param jTableName
 *      Name of the desired table.
 * \param status
 *      Java integer array of length 1 to put the status in if there
 *      are any exceptions.
 * \return
 *      The return value is an identifier for the table; this is used
 *      instead of the table's name for most RAMCloud operations
 *      involving the table.
 */
JNIEXPORT jlong
JNICALL Java_edu_stanford_ramcloud_RAMCloud_cppGetTableId(JNIEnv *env,
        jclass jRamCloud,
        jlong ramcloudObjectPointer,
        jstring jTableName,
        jintArray status) {
    RamCloud* ramcloud = reinterpret_cast<RamCloud*> (ramcloudObjectPointer);
    JStringGetter tableName(env, jTableName);
    uint64_t tableId;
    try {
        tableId = ramcloud->getTableId(tableName.string);
    }
    EXCEPTION_CATCHER(-1);
    return tableId;
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
 * \param ramcloudObjectPointer
 *      A pointer to the C++ RamCloud object
 * \param jTableId
 *      The table containing the desired object (return value from a
 *      previous call to getTableId).
 * \param jKey
 *      Variable length key that uniquely identifies the object within
 *      tableId. It does not necessarily have to be null terminated. The
 *      caller must ensure that the storage for this key is unchanged
 *      through the life of the RPC.
 * \param jRejectRules
 *      If non-NULL, specifies conditions under which the read should be
 *      aborted with an error.
 * \param versionBuffer
 *      A long array with a single value that will hold the version of the
 *      read object.
 * \param status
 *      Java integer array of length 1 to put the status in if there
 *      are any exceptions.
 * \return A byte array holding the value of the read object
 */
JNIEXPORT jbyteArray
JNICALL Java_edu_stanford_ramcloud_RAMCloud_cppRead(JNIEnv *env,
        jclass jRamCloud,
        jlong ramcloudObjectPointer,
        jlong jTableId,
        jbyteArray jKey,
        jbyteArray jRejectRules,
        jlongArray versionBuffer,
        jintArray status) {
    RamCloud* ramcloud = reinterpret_cast<RamCloud*> (ramcloudObjectPointer);

    RejectRules* rejectRules = NULL;
    if (jRejectRules != NULL) {
        RejectRules temp = createRejectRules(env, jRejectRules);
        rejectRules = &temp;
    }

    // Use of critical methods decreases likelihood of an array copy
    jsize keyLength = env->GetArrayLength(jKey);
    void* jKeyPointer = env->GetPrimitiveArrayCritical(jKey, 0);

    Buffer buffer;
    uint64_t version;
#if TIME_CPP
    uint64_t start = Cycles::rdtsc();
#endif
    try {
        ramcloud->read(jTableId,
                jKeyPointer,
                keyLength,
                &buffer,
                rejectRules,
                &version);
    }
    EXCEPTION_CATCHER(NULL);
#if TIME_CPP
    test_times[test_num_current] = Cycles::rdtsc() - start;

    test_num_current++;
    if (test_num_current == test_num_times) {
        std::sort(boost::begin(test_times), boost::end(test_times));
        printf("Median C++ Read Time: %f\n", Cycles::toSeconds(test_times[test_num_times / 2]) * 1000000);
        test_num_current = 0;
    }
#endif

    env->SetLongArrayRegion(versionBuffer, 0, 1, reinterpret_cast<jlong*> (&version));

    // Copy read value from C++ Buffer to Java byte array
    jbyteArray jValue = env->NewByteArray(buffer.getTotalLength());
    check_null(jValue, "NewByteArray failed");
    void* jValuePointer = env->GetPrimitiveArrayCritical(jValue, 0);
    buffer.copy(0, buffer.getTotalLength(), jValuePointer);
    env->ReleasePrimitiveArrayCritical(jValue, jValuePointer, 0);

    return jValue;
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
 * \param ramcloudObjectPointer
 *      A pointer to the C++ RamCloud object
 * \param jTableId
 *      The table containing the object to be deleted (return value from
 *      a previous call to getTableId).
 * \param jKey
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param jRejectRules
 *      If non-NULL, specifies conditions under which the remove should be
 *      aborted with an error.
 * \param status
 *      Java integer array of length 1 to put the status in if there
 *      are any exceptions.
 * \return The version number of the object (just before
 *      deletion).
 */
JNIEXPORT jlong
JNICALL Java_edu_stanford_ramcloud_RAMCloud_cppRemove(JNIEnv *env,
        jclass jRamCloud,
        jlong ramcloudObjectPointer,
        jlong jTableId,
        jbyteArray jKey,
        jbyteArray jRejectRules,
        jintArray status) {
    RamCloud* ramcloud = reinterpret_cast<RamCloud*> (ramcloudObjectPointer);
    RejectRules* rejectRules = NULL;
    if (jRejectRules != NULL) {
        RejectRules temp = createRejectRules(env, jRejectRules);
        rejectRules = &temp;
    }
    jsize keySize = env->GetArrayLength(jKey);
    void* jKeyPointer = env->GetPrimitiveArrayCritical(jKey, 0);
    uint64_t version;
    try {
        ramcloud->remove(jTableId, jKeyPointer, keySize, rejectRules, &version);
    }
    EXCEPTION_CATCHER(-1);
    env->ReleasePrimitiveArrayCritical(jKey, jKeyPointer, JNI_ABORT);
    return static_cast<jlong> (version);
}

/**
 * Replace the value of a given object, or create a new object if none
 * previously existed.
 *
 * \param env
 *      The current JNI environment.
 * \param jRamCloud
 *      The calling class.
 * \param ramcloudObjectPointer
 *      A pointer to the C++ RamCloud object.
 * \param jTableId
 *      The ID of the table to write to (return value from a previous call
 *      to getTableId).
 * \param jKey
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param jValue
 *      NULL-terminated string providing the new value for the object (the
 *      terminating NULL character will not be part of the object).
 * \param jRejectRules
 *      If non-NULL, specifies conditions under which the write
 *      should be aborted with an error.
 * \param status
 *      Java integer array of length 1 to put the status in if there
 *      are any exceptions.
 * \return The version number of the object is returned.
 *      If the operation was successful this will be the new version for
 *      the object. If the operation failed then the version number returned
 *      is the current version of the object, or 0 if the object does not
 *      exist.
 */
JNIEXPORT jlong
JNICALL Java_edu_stanford_ramcloud_RAMCloud_cppWrite(JNIEnv *env,
        jclass jRamCloud,
        jlong ramcloudObjectPointer,
        jlong jTableId,
        jbyteArray jKey,
        jbyteArray jValue,
        jbyteArray jRejectRules,
        jintArray status) {
    RamCloud* ramcloud = reinterpret_cast<RamCloud*> (ramcloudObjectPointer);
    RejectRules* rejectRules = NULL;
    if (jRejectRules != NULL) {
        RejectRules temp = createRejectRules(env, jRejectRules);
        rejectRules = &temp;
    }

    jsize keyLength = env->GetArrayLength(jKey);
    jsize valueLength = env->GetArrayLength(jValue);
    void* jKeyPointer = env->GetPrimitiveArrayCritical(jKey, 0);
    void* jValuePointer = env->GetPrimitiveArrayCritical(jValue, 0);
    uint64_t version;

#if TIME_CPP
    uint64_t start = Cycles::rdtsc();
#endif
    try {
        ramcloud->write(jTableId,
                jKeyPointer, keyLength,
                jValuePointer, valueLength,
                rejectRules,
                &version);
    }
    EXCEPTION_CATCHER(-1);
#if TIME_CPP
    test_times[test_num_current] = Cycles::rdtsc() - start;
    test_num_current++;
    if (test_num_current == test_num_times) {
        std::sort(boost::begin(test_times), boost::end(test_times));
        printf("Median C++ Write Time: %f\n", Cycles::toSeconds(test_times[test_num_times / 2]) * 1000000);
        test_num_current = 0;
    }
#endif

    env->ReleasePrimitiveArrayCritical(jValue, jValuePointer, JNI_ABORT);
    env->ReleasePrimitiveArrayCritical(jKey, jKeyPointer, JNI_ABORT);

    return static_cast<jlong> (version);
}
