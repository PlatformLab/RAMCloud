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
#include <TableEnumerator.h>
#include "edu_stanford_ramcloud_TableIterator.h"

using namespace RAMCloud;

JNIEXPORT jlong
JNICALL Java_edu_stanford_ramcloud_TableIterator_createTableEnumerator
(JNIEnv *env, jclass tableIterator, jlong ramcloudObjectPointer, jlong tableId) {
    TableEnumerator* enumerator = new TableEnumerator(
        *reinterpret_cast<RamCloud*>(ramcloudObjectPointer),
        tableId,
        0);
    
    return reinterpret_cast<jlong>(enumerator);
}

JNIEXPORT jlong
JNICALL Java_edu_stanford_ramcloud_TableIterator_getNextObject
(JNIEnv *env, jclass tableIterator, jlong tableEnumeratorPointer, jobjectArray objectData) {
    const void* buffer = NULL;
    uint32_t size;
    TableEnumerator* enumerator = reinterpret_cast<TableEnumerator*>(tableEnumeratorPointer);
    enumerator->next(&size, &buffer);
    if (buffer == NULL) {
        return -1;
    }
    
    Object object(buffer, size);
    uint16_t keyLength;
    uint32_t valueLength;
    const jbyte* key = static_cast<const jbyte*>(object.getKey(0, &keyLength));
    const jbyte* value = static_cast<const jbyte*>(object.getValue(&valueLength));

    jbyteArray jKey = env->NewByteArray(keyLength);
    jbyteArray jValue = env->NewByteArray(valueLength);
    env->SetByteArrayRegion(jKey, 0, keyLength, key);
    env->SetByteArrayRegion(jValue, 0, valueLength, value);
    env->SetObjectArrayElement(objectData, 0, jKey);
    env->SetObjectArrayElement(objectData, 1, jValue);
    
    return object.getVersion();
}


JNIEXPORT jboolean
JNICALL Java_edu_stanford_ramcloud_TableIterator_hasNext
(JNIEnv *env, jclass tableIterator, jlong tableEnumeratorPointer) {
    TableEnumerator* enumerator = reinterpret_cast<TableEnumerator*>(tableEnumeratorPointer);
    
    return enumerator->hasNext();
}
