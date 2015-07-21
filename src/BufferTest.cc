/* Copyright (c) 2010-2015 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <string.h>
#include <strings.h>

#include "TestUtil.h"
#include "Buffer.h"
#include "Logger.h"
#include "MockSyscall.h"

namespace RAMCloud {

// Convenience for accessing the internalAllocation member as char*
#define INTERNAL_ALLOC (reinterpret_cast<char*>(buffer.internalAllocation))

class BufferTest: public ::testing::Test {
  public:
    char bigData[2000];
    TestLog::Enable logEnabler;
    FILE *f;
    char fileName[100];

    BufferTest()
        : bigData()
        , logEnabler()
        , f(NULL)
    {
        bigData[0] = 0;
    }

    ~BufferTest()
    {
        if (f != NULL) {
            fclose(f);
            unlink(fileName);
        }
    }

    /*
     * Fill a buffer with some predefined data.
     */
    void
    fillBuffer(Buffer* buffer)
    {
        buffer->appendExternal("ABCDEFGHIJ", 10);
        buffer->appendExternal("abcdefghij", 10);
        buffer->appendExternal("klmnopqrs\0", 10);
    }

    /**
     * Returns a pointer to a large chunk of static data useful for
     * putting in a buffer. The caller should not modify this.
     */
    const char*
    getBigData()
    {
        // Initialize bigData, if that hasn't already been done before,
        // and return it.
        if (bigData[0] == 0) {
            for (uint32_t i = 0; i < sizeof(bigData); i++) {
                bigData[i] = downCast<char>('a' + (i%20));
            }
        }
        return bigData;
    }

    void openFile() {
        strncpy(fileName, "/tmp/ramcloud-buffer-test-delete-this-XXXXXX",
                sizeof(fileName));
        int fd = mkstemp(fileName);
        f = fdopen(fd, "r+");
    }

    /**
     * Generate a string describing the contents of the buffer in a way
     * that displays its internal chunk structure.
     *
     * \param buffer
     *      Buffer whose chunks should be described.
     * \param alt
     *      True means display an alternate form where only the length
     *      and internal/external status of each chunk are shown.
     *
     * \return A string that describes the contents of the buffer. It
     *         consists of the contents of the various chunks separated
     *         by " | ", with long chunks abbreviated and non-printing
     *         characters converted to something printable.
     */
    string
    showChunks(Buffer* buffer, bool alt = false)
    {
        // The following declaration defines the maximum number of characters
        // to display from each chunk.
        static const uint32_t CHUNK_LIMIT = 20;
        const char *separator = "";
        string s;
        uint32_t actualLength = 0;

        for (Buffer::Chunk* chunk = buffer->firstChunk; chunk != NULL;
                chunk = chunk->next) {
            const char* data = static_cast<const char*>(chunk->data);
            actualLength += chunk->length;
            s.append(separator);
            separator = " | ";
            if (alt) {
                s.append(format("%u%s", chunk->length,
                        chunk->internal ? " (internal)" : ""));
            } else {
                for (uint32_t i = 0; i < chunk->length; i++) {
                    if (i >= CHUNK_LIMIT) {
                        // This chunk is too big to print in its entirety;
                        // just print a count of the remaining characters.
                        s.append(format("(+%d chars)", chunk->length-i));
                        break;
                    }
                    TestUtil::convertChar(data[i], &s);
                }
            }
        }
        if (actualLength != buffer->totalLength) {
            s.append(separator);
            s.append(format("totalLength is %u, but actual is %u",
                    buffer->totalLength, actualLength));
        }
        return s;
    }

    /*
     * This chunk class generates a log message when it is destroyed.
     */
    class TestChunk: public Buffer::Chunk {
      public:
        TestChunk(const void* data, uint32_t length)
            : Chunk(data, length)
        {}

        ~TestChunk()
        {
            RAMCLOUD_TEST_LOG("Destroyed chunk containing '%.*s'", length,
                    data);
            data = NULL;
            length = 0;
            next = NULL;
        }
    };

    // Simple class for use in testing methods such as alloc.
    struct TestObject
    {
        int a;
        int b;
        char c;
        TestObject(int a, int b, char c)
            : a(a)
            , b(b)
            , c(c)
        {}
    };

  private:
    DISALLOW_COPY_AND_ASSIGN(BufferTest);
};

TEST_F(BufferTest, alloc_basics) {
    Buffer buffer;
    char* p = static_cast<char*>(buffer.alloc(6));
    memcpy(p, "abcdef", 6);
    EXPECT_EQ(894u, buffer.extraAppendBytes + sizeof(Buffer::Chunk));
    buffer.emplaceAppend<int>(0x30313233);
    p = static_cast<char*>(buffer.alloc(4));
    memcpy(p, "0123456789", 4);
    EXPECT_EQ("abcdef32100123", showChunks(&buffer));
    EXPECT_EQ(14u, buffer.totalLength);
    EXPECT_EQ(886u, buffer.extraAppendBytes + sizeof(Buffer::Chunk));
}
TEST_F(BufferTest, alloc_useInternalSpace) {
    Buffer buffer;
    buffer.alloc(60);
    EXPECT_GE(buffer.firstChunk->data, INTERNAL_ALLOC);
    EXPECT_LT(buffer.firstChunk->data,
            INTERNAL_ALLOC+sizeof(buffer.internalAllocation));
}
TEST_F(BufferTest, alloc_allocateNewSpace) {
    Buffer buffer;
    Buffer::Chunk chunk("abcdef", 5);
    buffer.appendChunk(&chunk);
    buffer.availableLength = 200;
    buffer.alloc(400 - sizeof32(Buffer::Chunk));
    EXPECT_EQ(1000u, buffer.extraAppendBytes);
    EXPECT_TRUE(buffer.allocations);
    EXPECT_EQ(1u, buffer.allocations->size());
}
TEST_F(BufferTest, alloc_checkChunkLinks) {
    // Allocate three chunks: 1st and 3rd with new, 2nd with appendChunk.
    Buffer buffer;
    buffer.appendCopy("01234", 5);
    Buffer::Chunk chunk("abcdef", 5);
    buffer.appendChunk(&chunk);
    buffer.appendCopy("9876", 4);
    EXPECT_EQ("01234 | abcde | 9876", showChunks(&buffer));
    EXPECT_EQ('9', buffer.lastChunk->data[0]);
}

TEST_F(BufferTest, allocAux_alignLength) {
    Buffer buffer;
    uint64_t value = reinterpret_cast<uint64_t>(buffer.allocAux(3));
    EXPECT_EQ(0u, value & 0x7);
}
TEST_F(BufferTest, allocAux_useAvailableSpace) {
    Buffer buffer;
    char* p = static_cast<char*>(buffer.allocAux(16));
    EXPECT_EQ(INTERNAL_ALLOC + 984, p);
    EXPECT_EQ(884u, buffer.availableLength);
}
TEST_F(BufferTest, allocAux_useSpaceInChunk) {
    Buffer buffer;

    // Create an initial chunk that doesn't come from managed storage,
    // so we can make sure that allocAux is taking extra space from the
    // *last* chunk, not the first chunk.
    TestChunk chunk("abcdefg", 6);
    buffer.appendChunk(&chunk);

    // Now create a second chunk with a bunch of extra space.
    buffer.alloc(300 - sizeof32(Buffer::Chunk));

    // Finally, do the allocation we care about.
    char* p = static_cast<char*>(buffer.allocAux(16));
    EXPECT_EQ(INTERNAL_ALLOC + sizeof32(buffer.internalAllocation)
            - sizeof32(Buffer::Chunk) - 16, p);
    EXPECT_EQ(584u, buffer.extraAppendBytes);
}
TEST_F(BufferTest, allocAux_getNewAllocation) {
    Buffer buffer;
    buffer.availableLength = 0;
    char* p = static_cast<char*>(buffer.allocAux(400));
    EXPECT_EQ(1400u, buffer.totalAllocatedBytes);
    EXPECT_EQ(1000u, p - buffer.firstAvailable);
    EXPECT_EQ(1000u, buffer.availableLength);
}

TEST_F(BufferTest, allocPrepend_bufferEmpty) {
    Buffer buffer;
    char* p = static_cast<char*>(buffer.allocPrepend(6));
    EXPECT_EQ(p, buffer.firstChunk->data);
    EXPECT_EQ(6u, buffer.firstChunk->length);
    EXPECT_EQ(6u, buffer.totalLength);
}
TEST_F(BufferTest, allocPrepend_fastPath) {
    Buffer buffer;
    buffer.appendCopy("abcdef", 6);
    char* p = static_cast<char*>(buffer.allocPrepend(8));
    memcpy(p, "0123456789", 8);
    p = static_cast<char*>(buffer.allocPrepend(4));
    memcpy(p, "ABCD", 4);
    EXPECT_EQ("ABCD01234567abcdef", showChunks(&buffer));
}
TEST_F(BufferTest, allocPrepend_notEnoughSpaceForFastPath) {
    Buffer buffer;
    buffer.appendCopy("abcdef", 6);
    EXPECT_EQ(1u, buffer.getNumberChunks());
    buffer.allocPrepend(Buffer::PREPEND_SPACE);
    EXPECT_EQ(1u, buffer.getNumberChunks());
    buffer.allocPrepend(1);
    EXPECT_EQ(2u, buffer.getNumberChunks());
}
TEST_F(BufferTest, allocPrepend_createNewChunk) {
    Buffer buffer;
    buffer.appendExternal("abcdef", 6);
    char* p = static_cast<char*>(buffer.allocPrepend(8));
    memcpy(p, "0123456789", 8);
    p = static_cast<char*>(buffer.allocPrepend(4));
    memcpy(p, "ABCD", 4);
    EXPECT_EQ("ABCD | 01234567 | abcdef", showChunks(&buffer));
}
TEST_F(BufferTest, allocPrepend_invalidateCursor) {
    Buffer buffer;
    buffer.appendExternal("abcdef", 6);
    buffer.getRange(2, 1);
    EXPECT_EQ(0u, buffer.cursorOffset);
    buffer.allocPrepend(8);
    EXPECT_EQ(~0u, buffer.cursorOffset);
}

TEST_F(BufferTest, append_fromOtherBuffer) {
    Buffer buffer, buffer2;
    buffer.appendCopy("abc", 3);
    char external[1000];
    buffer.appendExternal(external, sizeof(external));
    buffer.appendCopy("def", 3);
    const char* external2 = "012345";
    buffer.appendExternal(external2, 6);
    EXPECT_EQ("3 (internal) | 1000 | 3 (internal) | 6",
            showChunks(&buffer, true));
    buffer2.appendCopy("01234", 5);
    buffer2.append(&buffer, 1, 1010);
    EXPECT_EQ("7 (internal) | 1000 | 8 (internal)",
            showChunks(&buffer2, true));
}

TEST_F(BufferTest, appendExternal_fromOtherBuffer) {
    Buffer buffer;
    buffer.appendCopy("abcdef", 6);
    Buffer buffer2;
    buffer2.appendExternal("01234", 5);
    buffer2.appendExternal("ABCDEFGH", 8);
    buffer.appendExternal(&buffer2, 3, 8);
    EXPECT_EQ("abcdef | 34 | ABCDEF", showChunks(&buffer));
}

TEST_F(BufferTest, appendChunk) {
    // First chunk in buffer.
    Buffer buffer;
    Buffer::Chunk chunk("abcdef", 5);
    Buffer::Chunk chunk2("01234567", 6);
    buffer.appendChunk(&chunk);
    EXPECT_EQ("abcde", showChunks(&buffer));
    EXPECT_EQ(&chunk, buffer.firstChunk);
    EXPECT_EQ(&chunk, buffer.lastChunk);

    // Second chunk.
    buffer.appendChunk(&chunk2);
    EXPECT_EQ("abcde | 012345", showChunks(&buffer));
    EXPECT_EQ(&chunk, buffer.firstChunk);
    EXPECT_EQ(&chunk2, buffer.lastChunk);
}
TEST_F(BufferTest, appendChunk_saveExtraSpace) {
    // Create a virtual chunk, then a copied chunk.
    Buffer buffer;
    Buffer::Chunk chunk("abcdef", 5);
    buffer.appendChunk(&chunk);
    buffer.appendCopy("012345", 6);
    EXPECT_EQ(0u, buffer.availableLength);
    uint32_t extra = buffer.extraAppendBytes;

    // Store a special character to mark what should be the first
    // byte of extra space.
    char *p = const_cast<char*>(static_cast<const char*>(
            buffer.lastChunk->data));
    p[buffer.lastChunk->length] = 'x';

    // Now create a virtual chunk and make sure that the extra space
    // was saved correctly.
    Buffer::Chunk chunk2("ABCDE", 4);
    buffer.appendChunk(&chunk2);
    EXPECT_EQ(0u, buffer.extraAppendBytes);
    EXPECT_EQ(extra, buffer.availableLength);
    EXPECT_EQ('x', *(static_cast<const char*>(buffer.firstAvailable)));
}
TEST_F(BufferTest, appendChunk_saveExtraSpace_tooSmall) {
    // In this test, the existing saved space is larger than the
    // extra bytes.
    Buffer buffer;
    buffer.extraAppendBytes = 100;
    Buffer::Chunk chunk("abcdef", 5);
    buffer.appendChunk(&chunk);
    EXPECT_EQ(sizeof(buffer.internalAllocation) - Buffer::PREPEND_SPACE,
            buffer.availableLength);
    EXPECT_EQ(INTERNAL_ALLOC + Buffer::PREPEND_SPACE,
            buffer.firstAvailable);
}

TEST_F(BufferTest, copy_outOfRange) {
    Buffer buffer;
    char copy[10];
    buffer.appendExternal("abcde", 5);
    copy[0] = 'x';
    EXPECT_EQ(0u, buffer.copy(5, 1, copy));
    EXPECT_EQ('x', copy[0]);
}
TEST_F(BufferTest, copy_clipLength) {
    Buffer buffer;
    char copy[10];
    buffer.appendExternal("abcde", 5);
    uint32_t length = buffer.copy(1, 5, copy);
    EXPECT_EQ("bcde", string(copy, length));
}
TEST_F(BufferTest, copy_zeroLength) {
    Buffer buffer;
    char copy[10];
    buffer.appendExternal("abcde", 5);
    copy[0] = 'x';
    EXPECT_EQ(0u, buffer.copy(2, 0, copy));
    EXPECT_EQ('x', copy[0]);
}
TEST_F(BufferTest, copy_resetCurrentChunk) {
    Buffer buffer;
    char copy[10];
    buffer.appendExternal("abcde", 5);
    buffer.appendExternal("0123", 4);
    buffer.cursorOffset = 5;
    buffer.cursorChunk = buffer.firstChunk->next;
    uint32_t length = buffer.copy(2, 2, copy);
    EXPECT_EQ("cd", string(copy, length));
}
TEST_F(BufferTest, copy_useCurrentChunk) {
    Buffer buffer;
    char copy[10];
    buffer.appendExternal("abcde", 5);
    buffer.appendExternal("0123", 4);
    buffer.appendExternal("ABCDEFG", 7);

    // This test intentionally puts a bogus value in cursorOffset:
    // that way the test results will be different if the method
    // resets cursorChunk rather than using that information.
    buffer.cursorOffset = 4;
    buffer.cursorChunk = buffer.firstChunk->next;
    uint32_t length = buffer.copy(8, 3, copy);
    EXPECT_EQ("ABC", string(copy, length));
}
TEST_F(BufferTest, copy_startAndEndInSameChunk) {
    Buffer buffer;
    char copy[10];
    buffer.appendExternal("abcde", 5);
    buffer.appendExternal("01234567", 8);
    uint32_t length = buffer.copy(7, 4, copy);
    EXPECT_EQ("2345", string(copy, length));
}
TEST_F(BufferTest, copy_blockSpansChunks) {
    Buffer buffer;
    char copy[10];
    buffer.appendExternal("abcde", 5);
    buffer.appendExternal("01234567", 8);
    buffer.appendExternal("ABC", 3);
    uint32_t length = buffer.copy(4, 10, copy);
    EXPECT_EQ("e01234567A", string(copy, length));
    EXPECT_EQ(13u, buffer.cursorOffset);
    EXPECT_EQ(buffer.lastChunk, buffer.cursorChunk);
}
TEST_F(BufferTest, copy_entireBuffer) {
    Buffer buffer;
    char copy[20];
    buffer.appendExternal("abcde", 5);
    buffer.appendExternal("01234567", 8);
    buffer.appendExternal("ABC", 3);
    uint32_t length = buffer.copy(0, 16, copy);
    EXPECT_EQ("abcde01234567ABC", string(copy, length));
}

TEST_F(BufferTest, fillFromString) {
    Buffer b;

    // Hexadecimal numbers
    b.fillFromString("0xAFaf0900 0xa");
    EXPECT_EQ("0xafaf0900 10", TestUtil::toString(&b));

    // Decimal numbers
    b.reset();
    b.fillFromString("123 -456");
    EXPECT_EQ("123 -456", TestUtil::toString(&b));

    // Strings
    b.reset();
    b.fillFromString("abc def");
    EXPECT_EQ("abc/0 def/0", TestUtil::toString(&b));
}

TEST_F(BufferTest, getNewAllocation) {
    Buffer::allocationLogThreshold = 4000;
    Buffer buffer;

    // First allocation: check proper rounding up of the allocation size.
    uint32_t actualLength;
    char* result = buffer.getNewAllocation(193, &actualLength);
    EXPECT_TRUE(result != NULL);
    EXPECT_EQ(1200u, actualLength);
    EXPECT_EQ(1200u, buffer.totalAllocatedBytes);
    EXPECT_TRUE(buffer.allocations);
    EXPECT_EQ(1u, buffer.allocations->size());
    EXPECT_EQ("", TestLog::get());

    // Second allocation: check for log message about threshold.
    result = buffer.getNewAllocation(600, &actualLength);
    EXPECT_TRUE(result != NULL);
    EXPECT_EQ(2800u, actualLength);
    EXPECT_EQ(4000u, buffer.totalAllocatedBytes);
    EXPECT_EQ(2u, buffer.allocations->size());
    EXPECT_EQ("getNewAllocation: buffer has consumed 4000 bytes of "
            "extra storage, current allocation: 2800 bytes",
            TestLog::get());
    EXPECT_EQ(8000u, Buffer::allocationLogThreshold);
}

TEST_F(BufferTest, getNumberChunks) {
    Buffer buffer;
    EXPECT_EQ(0u, buffer.getNumberChunks());
    buffer.appendExternal("abcde", 5);
    EXPECT_EQ(1u, buffer.getNumberChunks());
    buffer.appendExternal("012345678", 8);
    EXPECT_EQ(2u, buffer.getNumberChunks());
    buffer.reset();
    EXPECT_EQ(0u, buffer.getNumberChunks());
}

TEST_F(BufferTest, getRange_basics) {
    Buffer buffer;
    const char* chunk = "0123456789";
    buffer.appendExternal("abcde", 5);
    buffer.appendExternal(chunk, 10);
    char* result = static_cast<char*>(buffer.getRange(8, 3));
    EXPECT_EQ(chunk+3, result);
    EXPECT_EQ('3', *result);
    EXPECT_EQ(buffer.firstChunk->next, buffer.cursorChunk);
    EXPECT_EQ(5u, buffer.cursorOffset);
}
TEST_F(BufferTest, getRange_fastPath) {
    Buffer buffer;
    const char* chunk = "0123456789";
    buffer.appendExternal("abcde", 5);
    buffer.appendExternal(chunk, 10);

    // This test intentionally puts a bogus value in cursorOffset:
    // that way the test results will be different if the method
    // resets cursorChunk rather than using the fast path.
    buffer.cursorOffset = 4;
    buffer.cursorChunk = buffer.firstChunk->next;
    char* result = static_cast<char*>(buffer.getRange(8, 3));
    EXPECT_EQ(chunk+4, result);
    EXPECT_EQ('4', *result);
}
TEST_F(BufferTest, getRange_emptyBuffer) {
    Buffer buffer;
    void* nullPtr = NULL;
    void* result = buffer.getRange(0, 0);
    EXPECT_EQ(nullPtr, result);
}
TEST_F(BufferTest, getRange_rangeStartsPastBufferEnd) {
    Buffer buffer;
    buffer.appendExternal("1", 1);
    void* result = buffer.getRange(1, 0);
    void* nullPtr = NULL;
    EXPECT_EQ(nullPtr, result);
}
TEST_F(BufferTest, getRange_rangeExtendsOutsideBuffer) {
    Buffer buffer;
    buffer.appendExternal("abcde", 5);
    void* nullPtr = NULL;
    void* result = buffer.getRange(1, 5);
    EXPECT_EQ(nullPtr, result);
}
TEST_F(BufferTest, getRange_searchFromCurrent) {
    Buffer buffer;
    buffer.appendExternal("abcde", 5);
    buffer.appendExternal("0123456789", 10);
    buffer.appendExternal("ABCDEF", 6);
    buffer.appendExternal("!@#$%", 5);
    buffer.cursorOffset = 5;
    buffer.cursorChunk = buffer.firstChunk->next;
    char* result = static_cast<char*>(buffer.getRange(22, 4));
    EXPECT_EQ('@', *result);
    EXPECT_EQ(buffer.lastChunk, buffer.cursorChunk);
    EXPECT_EQ(21u, buffer.cursorOffset);
}
TEST_F(BufferTest, getRange_makeCopy) {
    Buffer buffer;
    buffer.appendExternal("abcde", 5);
    buffer.appendExternal("0123456789", 10);
    char* result = static_cast<char*>(buffer.getRange(2, 8));
    EXPECT_EQ("cde01234", string(result, 8));
}

TEST_F(BufferTest, peek_outOfRange) {
    Buffer buffer;
    buffer.appendExternal("abcde", 5);
    void* pointer;
    uint32_t length = buffer.peek(5, &pointer);
    char* nullPtr = NULL;
    EXPECT_EQ(nullPtr, pointer);
    EXPECT_EQ(0u, length);
}
TEST_F(BufferTest, peek_fastPath) {
    Buffer buffer;
    const char* chunk = "0123456789";
    buffer.appendExternal("abcde", 5);
    buffer.appendExternal(chunk, 10);

    // This test intentionally puts a bogus value in cursorOffset:
    // that way the test results will be different if the method
    // resets cursorChunk rather than using the fast path.
    buffer.cursorOffset = 4;
    buffer.cursorChunk = buffer.firstChunk->next;
    char* pointer;
    uint32_t length = buffer.peek(6, reinterpret_cast<void**>(&pointer));
    EXPECT_EQ(chunk+2, pointer);
    EXPECT_EQ(8u, length);
}
TEST_F(BufferTest, peek_searchFromStart) {
    Buffer buffer;
    buffer.appendExternal("abcde", 5);
    buffer.appendExternal("0123456789", 10);
    buffer.appendExternal("ABCDEF", 6);
    char* pointer;
    uint32_t length = buffer.peek(17, reinterpret_cast<void**>(&pointer));
    EXPECT_EQ('C', *pointer);
    EXPECT_EQ(4u, length);
    EXPECT_EQ(buffer.lastChunk, buffer.cursorChunk);
    EXPECT_EQ(15u, buffer.cursorOffset);
}

TEST_F(BufferTest, prependChunk) {
    // First chunk in buffer.
    Buffer buffer;
    Buffer::Chunk chunk("abcdef", 5);
    Buffer::Chunk chunk2("01234567", 6);
    buffer.prependChunk(&chunk);
    EXPECT_EQ("abcde", showChunks(&buffer));
    EXPECT_EQ(&chunk, buffer.firstChunk);
    EXPECT_EQ(&chunk, buffer.lastChunk);

    // Second chunk.
    buffer.prependChunk(&chunk2);
    EXPECT_EQ("012345 | abcde", showChunks(&buffer));
    EXPECT_EQ(&chunk2, buffer.firstChunk);
    EXPECT_EQ(&chunk, buffer.lastChunk);
}
TEST_F(BufferTest, prependChunk_invalidateCursor) {
    Buffer buffer;
    buffer.appendExternal("abcdef", 5);
    buffer.getRange(3, 1);
    EXPECT_EQ(0u, buffer.cursorOffset);
    Buffer::Chunk chunk("01234567", 6);
    buffer.prependChunk(&chunk);
    EXPECT_EQ(~0u, buffer.cursorOffset);
}

// Reset is tested by the resetInternal tests below.

TEST_F(BufferTest, truncate_alreadyTruncated) {
    Buffer buffer;
    buffer.appendExternal("abcdef", 6);
    buffer.truncate(7);
    EXPECT_EQ("abcdef", showChunks(&buffer));
}
TEST_F(BufferTest, truncate_newLengthZero) {
    Buffer buffer;
    buffer.appendCopy("abcdef", 6);
    buffer.truncate(0);
    EXPECT_EQ("", showChunks(&buffer));
    EXPECT_EQ(900u, buffer.availableLength);
    EXPECT_EQ(100u, buffer.firstAvailable
            - reinterpret_cast<char*>(buffer.internalAllocation));
}
TEST_F(BufferTest, truncate_cancelExtraAppendBytes) {
    Buffer buffer;
    buffer.appendCopy("abcdefghij", 10);
    EXPECT_EQ(890u, buffer.extraAppendBytes + sizeof(Buffer::Chunk));
    EXPECT_EQ(0u, buffer.availableLength);
    buffer.truncate(5);
    EXPECT_EQ(0u, buffer.extraAppendBytes);
    EXPECT_EQ(890u, buffer.availableLength + sizeof(Buffer::Chunk));
    EXPECT_EQ(110u, buffer.firstAvailable
            - reinterpret_cast<char*>(buffer.internalAllocation));
}
TEST_F(BufferTest, truncate_mustTruncate) {
    Buffer buffer;
    buffer.appendExternal("XYZ", 3);
    buffer.appendExternal("abcdef", 6);
    TestChunk chunk("12345", 5);
    buffer.appendChunk(&chunk);
    TestChunk chunk2("ABCDEFG", 7);
    buffer.appendChunk(&chunk2);
    buffer.truncate(5);
    EXPECT_EQ("XYZ | ab", showChunks(&buffer));
    EXPECT_EQ("~TestChunk: Destroyed chunk containing '12345' "
            "| ~TestChunk: Destroyed chunk containing 'ABCDEFG'",
            TestLog::get());
}
TEST_F(BufferTest, truncate_boundaryAtChunkEdge) {
    Buffer buffer;
    buffer.appendExternal("abcdef", 6);
    buffer.appendExternal("0123", 4);
    buffer.appendExternal("ABCDEFGHIJ", 10);
    buffer.truncate(6);
    EXPECT_EQ("abcdef", showChunks(&buffer));
}
TEST_F(BufferTest, truncate_resetCurrentChunk) {
    Buffer buffer;
    buffer.appendExternal("abcdef", 6);
    buffer.appendExternal("01234", 4);
    buffer.appendExternal("ABCDEFG", 7);
    buffer.getRange(12, 2);
    buffer.truncate(14);
    EXPECT_EQ(10u, buffer.cursorOffset);
    EXPECT_EQ("abcdef | 0123 | ABCD", showChunks(&buffer));
    buffer.truncate(9);
    EXPECT_EQ(~0u, buffer.cursorOffset);
    EXPECT_EQ("abcdef | 012", showChunks(&buffer));
}

TEST_F(BufferTest, truncateFront_newLengthZero) {
    Buffer buffer;
    buffer.appendCopy("abcdef", 6);
    buffer.truncateFront(6);
    EXPECT_EQ("", showChunks(&buffer));
    EXPECT_EQ(900u, buffer.availableLength);
    EXPECT_EQ(100u, buffer.firstAvailable
            - reinterpret_cast<char*>(buffer.internalAllocation));
}
TEST_F(BufferTest, truncateFront_resetCursor) {
    Buffer buffer;
    buffer.appendExternal("abcdef", 6);
    buffer.appendExternal("012345678", 8);
    buffer.getRange(10, 2);
    EXPECT_EQ(6u, buffer.cursorOffset);
    buffer.truncateFront(10);
    EXPECT_EQ("4567", showChunks(&buffer));
    EXPECT_EQ(~0u, buffer.cursorOffset);
    EXPECT_TRUE(buffer.cursorChunk == NULL);
    EXPECT_EQ('6', *(buffer.getOffset<char>(2)));
}
TEST_F(BufferTest, truncateFront_mustTruncate) {
    Buffer buffer;
    TestChunk chunk("abcdef", 6);
    buffer.appendChunk(&chunk);
    TestChunk chunk2("1234", 4);
    buffer.appendChunk(&chunk2);
    buffer.appendExternal("ABCDEFGH", 8);
    buffer.truncateFront(12);
    EXPECT_EQ("CDEFGH", showChunks(&buffer));
    EXPECT_EQ("~TestChunk: Destroyed chunk containing 'abcdef' "
            "| ~TestChunk: Destroyed chunk containing '1234'",
            TestLog::get());
    EXPECT_EQ(6u, buffer.totalLength);
}

TEST_F(BufferTest, write_basics) {
    Buffer buffer;
    fillBuffer(&buffer);
    openFile();
    EXPECT_EQ(20u, buffer.write(5, 20, f));
    fflush(f);
    EXPECT_EQ("FGHIJabcdefghijklmno", TestUtil::readFile(fileName));
}
TEST_F(BufferTest, write_offsetTooLarge) {
    Buffer buffer;
    fillBuffer(&buffer);
    openFile();
    EXPECT_EQ(0u, buffer.write(30, 5, f));
    fflush(f);
    EXPECT_EQ("", TestUtil::readFile(fileName));
}
TEST_F(BufferTest, write_lengthLongerThanBuffer) {
    Buffer buffer;
    fillBuffer(&buffer);
    openFile();
    buffer.truncate(buffer.size() -1);
    EXPECT_EQ(4u, buffer.write(25, 5, f));
    fflush(f);
    EXPECT_EQ("pqrs", TestUtil::readFile(fileName));
}
TEST_F(BufferTest, write_IoError) {
    Buffer buffer;
    fillBuffer(&buffer);
    MockSyscall sys;
    Syscall *savedSyscall = Buffer::sys;
    Buffer::sys = &sys;
    openFile();
    sys.fwriteResult = 3;
    EXPECT_EQ(3u, buffer.write(5, 20, f));
    Buffer::sys = savedSyscall;
}

TEST_F(BufferTest, Iterator_constructor_noArgs) {
    // Empty buffer.
    Buffer buffer;
    Buffer::Iterator it1(&buffer);
    char* nullData = NULL;
    EXPECT_EQ(nullData, it1.currentData);
    EXPECT_EQ(0u, it1.currentLength);

    // Contents.
    const char* data = "abcd";
    buffer.appendExternal(data, 4);
    buffer.appendExternal("012345", 6);
    Buffer::Iterator it2(&buffer);
    EXPECT_EQ(buffer.firstChunk, it2.current);
    EXPECT_EQ(data, it2.currentData);
    EXPECT_EQ(4u, it2.currentLength);
    EXPECT_EQ(10u, it2.bytesLeft);
}

TEST_F(BufferTest, Iterator_constructor_withRange) {
    Buffer buffer;
    buffer.appendExternal("abcd", 4);
    buffer.appendExternal("012345", 6);
    buffer.appendExternal("ABCDEFG", 7);

    // Iterator range doesn't overlap buffer's range.
    Buffer::Iterator it1(&buffer, 17, 4);
    char* nullData = NULL;
    EXPECT_EQ(nullData, it1.currentData);
    EXPECT_EQ(0u, it1.currentLength);

    // Clip the length.
    Buffer::Iterator it2(&buffer, 5, 10);
    EXPECT_EQ(10u, it2.bytesLeft);

    // Skip chunks, set iterator starting position.
    Buffer::Iterator it3(&buffer, 7, 6);
    EXPECT_EQ('0', *it3.current->data);
    EXPECT_EQ('3', *it3.currentData);
    EXPECT_EQ(3u, it3.currentLength);
    EXPECT_EQ(6u, it3.bytesLeft);

    // Truncate currentLength if the iterator ends before the end of
    // its first chunk.
    Buffer::Iterator it4(&buffer, 6, 2);
    EXPECT_EQ(2u, it4.currentLength);
    EXPECT_EQ(2u, it4.bytesLeft);
}

TEST_F(BufferTest, Iterator_getNumberChunks_none) {
    Buffer buffer;
    Buffer::Iterator it(&buffer);
    EXPECT_EQ(0u, it.getNumberChunks());
}
TEST_F(BufferTest, Iterator_getNumberChunks_oneChunk) {
    Buffer buffer;
    buffer.appendExternal("abcd", 4);
    buffer.appendExternal("012345", 6);
    Buffer::Iterator it(&buffer, 6, 4);
    EXPECT_EQ(1u, it.getNumberChunks());
}
TEST_F(BufferTest, Iterator_getNumberChunks_multipleChunks) {
    Buffer buffer;
    buffer.appendExternal("abcd", 4);
    buffer.appendExternal("012345", 6);
    buffer.appendExternal("ABCDEFG", 7);
    Buffer::Iterator it(&buffer, 2, 10);
    EXPECT_EQ(3u, it.getNumberChunks());
}

TEST_F(BufferTest, Iterator_next) {
    Buffer buffer;
    buffer.appendExternal("abcd", 4);
    buffer.appendExternal("012345", 6);
    buffer.appendExternal("ABCDEFG", 7);

    // More data left.
    Buffer::Iterator it(&buffer, 7, 6);
    it.next();
    EXPECT_EQ('A', *it.current->data);
    EXPECT_EQ('A', *it.currentData);
    EXPECT_EQ(3u, it.currentLength);
    EXPECT_EQ(3u, it.bytesLeft);

    // No more data left.
    it.next();
    char* nullData = NULL;
    EXPECT_EQ(buffer.lastChunk, it.current);
    EXPECT_EQ(nullData, it.currentData);
    EXPECT_EQ(0u, it.currentLength);
    EXPECT_EQ(0u, it.bytesLeft);

    // Iterator is already at the end.
    it.next();
    EXPECT_EQ(buffer.lastChunk, it.current);
    EXPECT_EQ(nullData, it.currentData);
    EXPECT_EQ(0u, it.currentLength);
    EXPECT_EQ(0u, it.bytesLeft);
}

//---------------------------------------------------
// Header file methods
//---------------------------------------------------

TEST_F(BufferTest, allocAux) {
    Buffer buffer;
    TestObject *t = buffer.allocAux<TestObject>(10, 20, 'x');
    EXPECT_EQ(10, t->a);
    EXPECT_EQ(20, t->b);
    EXPECT_EQ('x', t->c);
}

TEST_F(BufferTest, append) {
    Buffer buffer;
    buffer.append("abcde", 5);
    buffer.append("0123", 4);
    buffer.append("", 0);
    EXPECT_EQ("abcde0123", showChunks(&buffer));
    EXPECT_EQ(1u, buffer.getNumberChunks());
    char large[600];
    for (uint32_t i = 0; i < sizeof(large); i++) {
        large[i] = "qrstuvw"[i%6];
    }
    buffer.append(large, sizeof(large));
    EXPECT_EQ(2u, buffer.getNumberChunks());
    string extract(reinterpret_cast<const char*>(buffer.getRange(9, 10)), 10);
    EXPECT_EQ("qrstuvqrst", extract);
}

TEST_F(BufferTest, appendCopy) {
    Buffer buffer;
    buffer.appendCopy("abcde", 5);
    buffer.appendCopy("0123", 4);
    buffer.appendCopy("", 0);
    EXPECT_EQ("abcde0123", showChunks(&buffer));
}

TEST_F(BufferTest, appendExternal) {
    Buffer buffer;
    buffer.appendExternal("abcde", 5);
    buffer.appendExternal("0123", 4);
    buffer.appendExternal("", 0);
    EXPECT_EQ("abcde | 0123", showChunks(&buffer));
}

TEST_F(BufferTest, appendCopyAndGetStart) {
    Buffer buffer;
    TestObject t(10, 20, 'x');
    buffer.appendCopy(&t);
    TestObject* t2 = buffer.getStart<TestObject>();
    EXPECT_EQ(10, t2->a);
    EXPECT_EQ(20, t2->b);
    EXPECT_EQ('x', t2->c);
}

TEST_F(BufferTest, emplaceAppend) {
    Buffer buffer;
    buffer.emplaceAppend<TestObject>(10, 20, 'x');
    buffer.emplaceAppend<TestObject>(30, 40, 'y');
    TestObject* t1 = buffer.getStart<TestObject>();
    TestObject* t2 = buffer.getOffset<TestObject>(sizeof(TestObject));
    EXPECT_EQ(10, t1->a);
    EXPECT_EQ(20, t1->b);
    EXPECT_EQ('x', t1->c);
    EXPECT_EQ(30, t2->a);
    EXPECT_EQ(40, t2->b);
    EXPECT_EQ('y', t2->c);
    EXPECT_EQ(2*sizeof(TestObject), buffer.size());
}

TEST_F(BufferTest, emplacePrepend) {
    Buffer buffer;
    buffer.appendCopy("abcdefg", 6);
    buffer.emplacePrepend<TestObject>(10, 20, 'x');
    buffer.emplacePrepend<TestObject>(30, 40, 'y');
    EXPECT_EQ(1u, buffer.getNumberChunks());
    EXPECT_EQ(2*sizeof(TestObject) + 6, buffer.size());
    TestObject* t1 = buffer.getStart<TestObject>();
    TestObject* t2 = buffer.getOffset<TestObject>(sizeof(TestObject));
    EXPECT_EQ(30, t1->a);
    EXPECT_EQ(40, t1->b);
    EXPECT_EQ('y', t1->c);
    EXPECT_EQ(10, t2->a);
    EXPECT_EQ(20, t2->b);
    EXPECT_EQ('x', t2->c);
}

TEST_F(BufferTest, getOffset) {
    Buffer buffer;
    TestObject t(10, 20, 'x');
    buffer.appendCopy("abcdefg", 5);
    buffer.appendCopy(&t);
    TestObject* t2 = buffer.getOffset<TestObject>(5);
    EXPECT_EQ(10, t2->a);
    EXPECT_EQ(20, t2->b);
    EXPECT_EQ('x', t2->c);
}

TEST_F(BufferTest, resetInternal_partial) {
    // Construct a Buffer in a character array; this is needed so that
    // the Buffer constructor doesn't get called after we do a partial
    // reset (which leaves the buffer structure inconsistent).
    char storage[sizeof(Buffer)];
    Buffer* buffer = new(storage) Buffer;
    TestChunk chunk("abcdef", 6);
    TestChunk chunk2("0123", 4);
    buffer->appendChunk(&chunk);
    buffer->alloc(1500);
    buffer->alloc(3000);
    buffer->appendChunk(&chunk2);
    EXPECT_EQ(2u, buffer->allocations->size());
    buffer->cursorChunk = buffer->firstChunk;
    buffer->cursorOffset = 6;
    TestLog::reset();
    buffer->resetInternal(false);
    EXPECT_EQ("~TestChunk: Destroyed chunk containing 'abcdef' | "
            "~TestChunk: Destroyed chunk containing '0123'",
            TestLog::get());
    EXPECT_EQ(4510u, buffer->totalLength);
    EXPECT_EQ(2u, buffer->allocations->size());
}

TEST_F(BufferTest, resetInternal_full) {
    Buffer buffer;
    TestChunk chunk("abcdef", 6);
    TestChunk chunk2("0123", 4);
    buffer.appendChunk(&chunk);
    buffer.alloc(1500);
    buffer.alloc(3000);
    buffer.appendChunk(&chunk2);
    EXPECT_EQ(2u, buffer.allocations->size());
    buffer.cursorChunk = buffer.firstChunk;
    buffer.cursorOffset = 6;
    TestLog::reset();
    buffer.resetInternal(true);
    EXPECT_EQ("~TestChunk: Destroyed chunk containing 'abcdef' | "
            "~TestChunk: Destroyed chunk containing '0123'",
            TestLog::get());
    Buffer::Chunk* nullChunk = NULL;
    EXPECT_EQ(0u, buffer.totalLength);
    EXPECT_EQ(nullChunk, buffer.firstChunk);
    EXPECT_EQ(nullChunk, buffer.lastChunk);
    EXPECT_EQ(nullChunk, buffer.cursorChunk);
    EXPECT_EQ(~0u, buffer.cursorOffset);
    EXPECT_EQ(0u, buffer.extraAppendBytes);
    EXPECT_EQ(0u, buffer.allocations->size());
    EXPECT_EQ(900u, buffer.availableLength);
    EXPECT_EQ(100u, buffer.firstAvailable - INTERNAL_ALLOC);
    EXPECT_EQ(0u, buffer.totalAllocatedBytes);
}

TEST_F(BufferTest, Iterator_inlineMethods) {
    Buffer buffer;
    buffer.appendExternal("012345", 6);
    buffer.appendExternal("ABCDEFG", 7);

    Buffer::Iterator it(&buffer, 2, 6);
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ('2', *static_cast<const char*>(it.getData()));
    EXPECT_EQ(4u, it.getLength());

    // Last chunk.
    it.next();
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ('A', *static_cast<const char*>(it.getData()));
    EXPECT_EQ(2u, it.getLength());

    // No more data left.
    it.next();
    char* nullData = NULL;
    EXPECT_TRUE(it.isDone());
    EXPECT_EQ(nullData, static_cast<const char*>(it.getData()));
    EXPECT_EQ(0u, it.getLength());
}

}  // namespace RAMCloud
