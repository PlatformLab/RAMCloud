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
#include "JavaCommon.h"

using namespace RAMCloud;

/**
 * Constructs a TableEnumerator object for the given tableId and returns a
 * pointer to it.
 *
 * \param env
 *      The calling JNI environment.
 * \param tableIterator
 *      The calling Java class.
 * \param ramcloudClusterHandle
 *      A pointer to the RAMCloud object to construct the enumerator with.
 * \param tableId
 *      The ID of the table for the TableEnumerator to enumerate.
 * \result A pointer to the constructed TableEnumerator object.
 */
JNIEXPORT jlong
JNICALL Java_edu_stanford_ramcloud_TableIterator_createTableEnumerator
(JNIEnv *env, jclass tableIterator, jlong ramcloudClusterHandle, jlong tableId) {
    TableEnumerator* enumerator = new TableEnumerator(
            *reinterpret_cast<RamCloud*>(ramcloudClusterHandle),
            tableId,
            0);
    
    return reinterpret_cast<jlong>(enumerator);
}

/**
 * Gets the next blob of objects from the TableEnumerator.
 *
 * \param env
 *      The calling JNI environment.
 * \param tableIterator
 *      The calling Java class.
 * \param tableEnumeratorPointer
 *      A pointer to the TableEnumerator object to get the blob from.
 * \param status
 *      Java integer array of length 1 to put the status in if there
 *      are any exceptions.
 * \result A Java NIO ByteBuffer wrapping the C++ buffer holding the blob of
 *         objects.
 */
JNIEXPORT jobject
JNICALL Java_edu_stanford_ramcloud_TableIterator_getNextBatch
(JNIEnv *env, jclass tableIterator, jlong tableEnumeratorPointer, jintArray status) {
    TableEnumerator* enumerator = reinterpret_cast<TableEnumerator*>(tableEnumeratorPointer);

    Buffer* buf = NULL;
    try {
        enumerator->nextObjectBlob(&buf);
    } catch (ClientException &ex) {
        
    }
    if (buf == NULL) {
        return NULL;
    }
    jobject out = env->NewDirectByteBuffer(buf->getRange(0, buf->size()), buf->size());
    return out;

}

/**
 * Deletes the C++ TableEnumerator object.
 *
 * \param env
 *      The calling JNI environment.
 * \param tableIterator
 *      The calling Java class.
 * \param tableEnumeratorPointer
 *      A pointer to the TableEnumerator object to delete.
 */
JNIEXPORT void
JNICALL Java_edu_stanford_ramcloud_TableIterator_delete
(JNIEnv *env, jclass tableIterator, jlong tableEnumeratorPointer) {
    TableEnumerator* enumerator = reinterpret_cast<TableEnumerator*>(tableEnumeratorPointer);
    delete enumerator;
}

