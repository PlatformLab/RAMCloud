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
#include "edu_stanford_ramcloud_TestClient.h"

using namespace RAMCloud;

/**
 * A method for Java to call to test anything in C++.
 *
 * \param env
 *      The calling JNI environment
 * \param jRamCloud
 *      The calling class
 * \param string1
 *      An argument Java can pass in for C++ to do something with
 */
JNIEXPORT void
JNICALL Java_edu_stanford_ramcloud_TestClient_test(JNIEnv *env,
        jclass jRamCloud,
        jstring string1) {
    const char* key = env->GetStringUTFChars(string1, 0);
    jsize keyLength = env->GetStringUTFLength(string1);
    // uint64_t out[2];
    // MurmurHash3_x64_128(key, keyLength, 1, &out);
    // printf("%016lx\n", out[0] & 0x0000ffffffffffffUL);
    Key k(2, reinterpret_cast<const void*>(key), keyLength);
    uint64_t secondaryHash;
    uint64_t i = HashTable::findBucketIndex(524288, k.getHash(), &secondaryHash);
    printf("%s %u\n", key, i);
}
