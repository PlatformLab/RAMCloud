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

JNIEXPORT void
JNICALL Java_edu_stanford_ramcloud_RAMCloud_test(JNIEnv *env,
        jclass jRamCloud,
        jstring string1) {
    const char* key = env->GetStringUTFChars(string1, 0);
    printf("%s\n", key);
    jsize keyLength = env->GetStringUTFLength(string1);
    uint64_t out[2];
    MurmurHash3_x64_128(key, keyLength, 1, &out);
    //printf("%016lx\n", out[0] & 0x0000ffffffffffffUL);
}

JNIEXPORT void
JNICALL Java_edu_stanford_ramcloud_RAMCloud_testError(JNIEnv *env,
        jclass jRamCloud,
        jintArray array) {
    // test
    uint32_t num(32);
    uint64_t start = Cycles::rdtsc();
    env->SetIntArrayRegion(array, 0, 1, reinterpret_cast<jint*> (&num));
    // printf("%f\n", (double) Cycles::toSeconds(Cycles::rdtsc() - start) * 1000000.0);
}
