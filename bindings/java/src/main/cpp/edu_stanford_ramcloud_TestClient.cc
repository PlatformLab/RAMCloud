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

#include <Cycles.h>
#include <RamCloud.h>
#include <TableEnumerator.h>
#include "edu_stanford_ramcloud_TestClient.h"

using namespace RAMCloud;

/**
 * A method for Java to call to test anything in C++.
 *
 * \param env
 *      The calling JNI environment
 * \param jRamCloud
 *      The calling class
 * \param ramcloudClusterHandle
 *      A pointer to the C++ RAMCloud object
 * \param arg
 *      An argument Java can pass in for C++ to do something with
 */
JNIEXPORT void
JNICALL Java_edu_stanford_ramcloud_TestClient_test(JNIEnv *env,
                                                   jclass jRamCloud,
                                                   jlong ramcloudClusterHandle,
                                                   jlong arg) {
    uint32_t count(1000000);
    RamCloud* ramcloud = reinterpret_cast<RamCloud*>(ramcloudClusterHandle);
    TableEnumerator enumerator(*ramcloud, arg, 0);
    uint64_t start = Cycles::rdtsc();
    uint32_t keyLength;
    const void* key;
    uint32_t valueLength;
    const void* value;
    for (uint32_t i = 0; i < count; i++) {
        enumerator.nextKeyAndData(&keyLength, &key, &valueLength, &value);
    }
    uint64_t time = Cycles::rdtsc() - start;
    printf("Time per enumerate in C++: %f\n", Cycles::toSeconds(time));
}
