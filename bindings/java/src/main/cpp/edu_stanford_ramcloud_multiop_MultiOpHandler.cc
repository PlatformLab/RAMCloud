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

#include "edu_stanford_ramcloud_multiop_MultiOpHandler.h"
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
 * Calls Java to read the results in the ByteBuffer and store them on the Java
 * heap, so that C++ can resume filling the buffer from the beginning without
 * fear of overwriting data that has not yet been stored in Java.
 */
void flushBuffer(
        JNIEnv *env,
        const jobject &multiOpHandler,
        ByteBuffer &buffer,
        uint32_t numObjects,
        uint32_t currentIndex) {
    buffer.mark = 4;
    buffer.write(numObjects);
    static jmethodID unloadBufferId =
            env->GetMethodID(env->GetObjectClass(multiOpHandler),
                            "unloadBuffer",
                            "()V");
    env->CallVoidMethod(multiOpHandler, unloadBufferId);
    buffer.rewind();
    buffer.write(currentIndex);
    buffer.mark += 4;
}

/**
 * Performs a multi-read operation.
 *
 * \param env
 *      The current JNI environment.
 * \param multiOpHandler
 *      The calling class.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ will communicate.
 *      The format for the input buffer is:
 *          8 bytes for a pointer to a C++ RamCloud object
 *          4 bytes for the index of the first read operation
 *          4 bytes for the number of multiread operations
 *          For each read operation:
 *              8 bytes for the tableId to read from
 *              2 bytes for the length of the key to read
 *              byte array for the key to read
 *      The format for the output buffer is:
 *          4 bytes for index of the first read operation
 *          4 bytes for the number of results in the buffer
 *          For each result:
 *              4 bytes for the status of the operation.
 *              If the status is 0:
 *                  8 bytes for the version of the read object
 *                  4 bytes for the length of the read value
 *                  byte array for the read value
 */
JNIEXPORT void
JNICALL Java_edu_stanford_ramcloud_multiop_MultiOpHandler_cppMultiRead(
        JNIEnv *env,
        jobject multiOpHandler,
        jlong byteBufferPointer) {
    ByteBuffer buffer(byteBufferPointer);
    RamCloud* ramcloud = buffer.readPointer<RamCloud>();
    
    uint32_t currentIndex = buffer.read<uint32_t>();
    uint32_t numObjects = buffer.read<uint32_t>();
    
    // ObjectBuffers that the object values will be read into.
    Tub<ObjectBuffer> values[numObjects];
    
    // MultiReadObjects that specify information for the multi-read to use.
    MultiReadObject objects[numObjects];
    
    // Pointers to the objects in the above array.
    MultiReadObject* objectPointers[numObjects];
    
    for (int i = 0; i < numObjects; i++) {
        uint64_t tableId = buffer.read<uint64_t>();
        uint16_t keyLength = buffer.read<uint16_t>();
        void* key = buffer.getVoidPointer(keyLength);
        objects[i] = {
            tableId,
            key,
            keyLength,
            &values[i]
        };
        objectPointers[i] = &objects[i];
    }

#if TIME_CPP
    uint64_t start = Cycles::rdtsc();
#endif
    MultiRead request(ramcloud, objectPointers, numObjects);
    request.wait();
#if TIME_CPP
    start = Cycles::rdtsc() - start;
    printf("C++ MultiRead Time: %f\n", Cycles::toSeconds(start) * 1000000 / numObjects);
#endif

    buffer.rewind();
    buffer.write(currentIndex);
    buffer.mark += 4;
    uint32_t lastFlush(0);
    for (int i = 0; i < numObjects; i++) {
        if (buffer.mark + 4 >= bufferSize) {
            flushBuffer(env, multiOpHandler, buffer, i - lastFlush, currentIndex);
            lastFlush = i;
        }
        uint32_t status = static_cast<uint32_t>(objects[i].status);                               
        buffer.write(status);
        if (status == 0) {
            uint32_t valueLength;
            const void* value = values[i].get()->getValue(&valueLength);
            // If the next result can't fit into the buffer, call Java to read
            // the data currently in the buffer and resume filling it from the
            // beginning.
            if (buffer.mark + 12 + valueLength >= bufferSize) {
                flushBuffer(env, multiOpHandler, buffer, i - lastFlush, currentIndex);
                lastFlush = i;
                buffer.write(status);
            }
            buffer.write(objects[i].version);
            buffer.write(valueLength);
            memcpy(buffer.getVoidPointer(), value, valueLength);
            buffer.mark += valueLength;
        }
        currentIndex++;
    }
    flushBuffer(env, multiOpHandler, buffer, numObjects - lastFlush, currentIndex);
}

/**
 * Performs a multi-write operation.
 *
 * \param env
 *      The current JNI environment.
 * \param multiOpHandler
 *      The calling class.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ will communicate.
 *      The format for the input buffer is:
 *          8 bytes for a pointer to a C++ RamCloud object
 *          4 bytes for the index of the first write operation
 *          4 bytes for the number of multiwrite operations
 *          For each write operation:
 *              8 bytes for the tableId to write to
 *              2 bytes for the length of the key to write
 *              byte array for the key to write
 *              4 bytes for the length of the value to write
 *              byte array for the value to write
 *              12 bytes representing the RejectRules for this operation
 *      The format for the output buffer is:
 *          4 bytes for index of the first read operation
 *          4 bytes for the number of results in the buffer
 *          For each result:
 *              4 bytes for the status of the operation.
 *              If the status is 0:
 *                  8 bytes for the version of the written object
 */
JNIEXPORT void
JNICALL Java_edu_stanford_ramcloud_multiop_MultiOpHandler_cppMultiWrite(
        JNIEnv *env,
        jobject multiOpHandler,
        jlong byteBufferPointer) {
    ByteBuffer buffer(byteBufferPointer);
    RamCloud* ramcloud = buffer.readPointer<RamCloud>();
    
    uint32_t currentIndex = buffer.read<uint32_t>();
    uint32_t numObjects = buffer.read<uint32_t>();

    Tub<MultiWriteObject> objects[numObjects];
    MultiWriteObject* objectPointers[numObjects];
    
    for (int i = 0; i < numObjects; i++) {
        uint64_t tableId = buffer.read<uint64_t>();
        uint16_t keyLength = buffer.read<uint16_t>();
        void* key = buffer.getVoidPointer(keyLength);
        uint32_t valueLength = buffer.read<uint32_t>();
        void* value = buffer.getVoidPointer(valueLength);
        RejectRules* rule = buffer.getPointer<RejectRules>();

        objects[i].construct(tableId,
                             key,
                             keyLength,
                             value,
                             valueLength,
                             rule);
        objectPointers[i] = objects[i].get();
    }

#if TIME_CPP
    uint64_t start = Cycles::rdtsc();
#endif
    MultiWrite request(ramcloud, objectPointers, numObjects);
    request.wait();
#if TIME_CPP
    start = Cycles::rdtsc() - start;
    printf("C++ MultiWrite Time: %f\n", Cycles::toSeconds(start) * 1000000 / numObjects);
#endif

    buffer.rewind();
    buffer.write(currentIndex);
    buffer.mark += 4;
    uint32_t lastFlush(0);
    for (int i = 0; i < numObjects; i++) {
        if (buffer.mark + 4 >= bufferSize) {
            flushBuffer(env, multiOpHandler, buffer, i - lastFlush, currentIndex);
            lastFlush = i;
        }
        uint32_t status = static_cast<uint32_t>(objectPointers[i]->status);
        buffer.write(status);
        if (status == 0) {
            if (buffer.mark + 8 >= bufferSize) {
                flushBuffer(env, multiOpHandler, buffer, i - lastFlush, currentIndex);
                lastFlush = i;
                buffer.write(status);
            }
            buffer.write(objectPointers[i]->version);
        }
        currentIndex++;
    }
    flushBuffer(env, multiOpHandler, buffer, numObjects - lastFlush, currentIndex);
}

/**
 * Performs a multi-remove operation.
 *
 * \param env
 *      The current JNI environment.
 * \param multiOpHandler
 *      The calling class.
 * \param byteBufferPointer
 *      A pointer to the ByteBuffer through which Java and C++ will communicate.
 *      The format for the input buffer is:
 *          8 bytes for a pointer to a C++ RamCloud object
 *          4 bytes for the index of the first remove operation
 *          4 bytes for the number of multiremove operations
 *          For each remove operation:
 *              8 bytes for the tableId to remove to
 *              2 bytes for the length of the key to remove
 *              byte array for the key to remove
 *              12 bytes representing the RejectRules for this operation
 *      The format for the output buffer is:
 *          4 bytes for index of the first read operation
 *          4 bytes for the number of results in the buffer
 *          For each result:
 *              4 bytes for the status of the operation
 *              If the status is 0:
 *                  8 bytes for the version of the object just before removal
 */
JNIEXPORT void
JNICALL Java_edu_stanford_ramcloud_multiop_MultiOpHandler_cppMultiRemove(
        JNIEnv *env,
        jobject multiOpHandler,
        jlong byteBufferPointer) {
#if TIME_CPP
    uint64_t start = Cycles::rdtsc();
#endif
    ByteBuffer buffer(byteBufferPointer);
    RamCloud* ramcloud = buffer.readPointer<RamCloud>();
    
    uint32_t currentIndex = buffer.read<uint32_t>();
    uint32_t numObjects = buffer.read<uint32_t>();

    Tub<MultiRemoveObject> objects[numObjects];
    MultiRemoveObject* objectPointers[numObjects];
    
    for (int i = 0; i < numObjects; i++) {
        uint64_t tableId = buffer.read<uint64_t>();
        uint16_t keyLength = buffer.read<uint16_t>();
        void* key = buffer.getVoidPointer(keyLength);
        RejectRules* rule = buffer.getPointer<RejectRules>();

        objects[i].construct(tableId,
                             key,
                             keyLength,
                             rule);
        objectPointers[i] = objects[i].get();
    }

    MultiRemove request(ramcloud, objectPointers, numObjects);
    request.wait();
#if TIME_CPP
    start = Cycles::rdtsc() - start;
    printf("C++ MultiRemove Time: %f\n", Cycles::toSeconds(start) * 1000000 / numObjects);
#endif

    buffer.rewind();
    buffer.write(currentIndex);
    buffer.mark += 4;
    uint32_t lastFlush(0);
    for (int i = 0; i < numObjects; i++) {
        if (buffer.mark + 4 >= bufferSize) {
            flushBuffer(env, multiOpHandler, buffer, i - lastFlush, currentIndex);
            lastFlush = i;
        }
        uint32_t status = static_cast<uint32_t>(objectPointers[i]->status);
        buffer.write(status);
        if (status == 0) {
            if (buffer.mark + 8 >= bufferSize) {
                flushBuffer(env, multiOpHandler, buffer, i - lastFlush, currentIndex);
                lastFlush = i;
                buffer.write(status);
            }
            buffer.write(objectPointers[i]->version);
        }
        currentIndex++;
    }
    flushBuffer(env, multiOpHandler, buffer, numObjects - lastFlush, currentIndex);
}
