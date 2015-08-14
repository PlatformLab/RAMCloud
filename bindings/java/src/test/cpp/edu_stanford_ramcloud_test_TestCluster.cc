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

#include <TestUtil.h>
#include <RamCloud.h>
#include <MockCluster.h>
#include "edu_stanford_ramcloud_test_TestCluster.h"

using namespace RAMCloud;

/**
 * Creates a mock cluster with one master.
 *
 * \param env
 *      The current JNI environment.
 * \param clazz
 *      The calling Java class.
 * \param pointers
 *      A java long array to store the resulting pointers of the
 *      RAMCloud and cluster objects.
 */
JNIEXPORT void JNICALL Java_edu_stanford_ramcloud_test_TestCluster_createMockCluster
(JNIEnv *env, jclass clazz, jlongArray pointers) {
    // Disable the verbose C++ log messages
    Logger::get().setLogLevels(1);

    Context* context = new Context();
    MockCluster* cluster = new MockCluster(context);
    ServerConfig config = ServerConfig::forTesting();
    config.services = {WireFormat::MASTER_SERVICE,
                       WireFormat::PING_SERVICE};
    config.localLocator = "mock:host=master1";
    config.maxObjectKeySize = 512;
    config.maxObjectDataSize = 1024;
    config.segmentSize = 128*1024;
    config.segletSize = 128*1024;
    cluster->addServer(config);
    RamCloud* ramcloud = new RamCloud(context, "mock:host=coordinator");

    uint64_t* pointerPointer = static_cast<uint64_t*>(env->GetPrimitiveArrayCritical(pointers, 0));
    pointerPointer[0] = reinterpret_cast<uint64_t>(cluster);
    pointerPointer[1] = reinterpret_cast<uint64_t>(ramcloud);
    env->ReleasePrimitiveArrayCritical(pointers, reinterpret_cast<void*>(pointerPointer), 0);
}

/**
 * Deletes a mock cluster and RAMCloud object.
 *
 * \param env
 *      The current JNI environment.
 * \param clazz
 *      The calling Java class.
 * \param pointers
 *      A java long array with the pointers to delete.
 */
JNIEXPORT void JNICALL Java_edu_stanford_ramcloud_test_TestCluster_destroy
(JNIEnv *env, jclass clazz, jlongArray pointers) {
    uint64_t* pointerPointer = static_cast<uint64_t*>(env->GetPrimitiveArrayCritical(pointers, 0));
    RamCloud* ramcloud = reinterpret_cast<RamCloud*>(pointerPointer[1]);
    delete ramcloud;
    /**
    MockCluster* cluster = reinterpret_cast<MockCluster*>(pointerPointer[0]);
    delete cluster;
    */
    env->ReleasePrimitiveArrayCritical(pointers, reinterpret_cast<void*>(pointerPointer), JNI_ABORT);
}
