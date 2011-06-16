/* Copyright (c) 2010 Stanford University
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

namespace RAMCloud {

class NotARawChunk : public Buffer::Chunk {
  public:
    static NotARawChunk* prependToBuffer(Buffer* buffer,
                                         void* data, uint32_t length) {
        NotARawChunk* chunk = new(buffer, CHUNK) NotARawChunk(data, length);
        Chunk::prependChunkToBuffer(buffer, chunk);
        return chunk;
    }
    static NotARawChunk* appendToBuffer(Buffer* buffer,
                                        void* data, uint32_t length) {
        NotARawChunk* chunk = new(buffer, CHUNK) NotARawChunk(data, length);
        Chunk::appendChunkToBuffer(buffer, chunk);
        return chunk;
    }
    bool destructed;
  private:
    NotARawChunk(void* data, uint32_t length)
        : Chunk(data, length), destructed(false) {}
  public:
    ~NotARawChunk() {
        destructed = true;
    }
};


class BufferAllocationTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(BufferAllocationTest);

    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_destructor);

    CPPUNIT_TEST(test_reset);

    CPPUNIT_TEST(test_allocateChunk);
    CPPUNIT_TEST(test_allocatePrepend);
    CPPUNIT_TEST(test_allocateAppend);

    CPPUNIT_TEST_SUITE_END();

    Buffer::Allocation* a;

  public:

    BufferAllocationTest() : a(NULL) {}

    void setUp() {
        a = Buffer::Allocation::newAllocation(256, 2048);
    }

    void tearDown() {
        if (a != NULL)
            free(a);
        a = NULL;
    }

    void test_constructor() {

        // make sure Allocation::padding is set correctly.
        CPPUNIT_ASSERT_EQUAL(0, reinterpret_cast<uint64_t>(a->data) & 0x7);

        CPPUNIT_ASSERT(a->next == NULL);
        CPPUNIT_ASSERT_EQUAL(256, a->prependTop);
        CPPUNIT_ASSERT_EQUAL(256, a->appendTop);
        CPPUNIT_ASSERT_EQUAL(2048, a->chunkTop);
    }

    void test_destructor() {
    }

    void test_reset() {
        a->allocateChunk(32);
        a->allocatePrepend(16);
        a->allocateAppend(64);
        a->next = a;
        a->reset(32, 256);

        CPPUNIT_ASSERT(a->next == NULL);
        CPPUNIT_ASSERT_EQUAL(32, a->prependTop);
        CPPUNIT_ASSERT_EQUAL(32, a->appendTop);
        CPPUNIT_ASSERT_EQUAL(256, a->chunkTop);
    }

    void test_allocateChunk() {
        uint32_t size = 2048 - 256;
        a->allocateChunk(0);
        CPPUNIT_ASSERT_EQUAL(&a->data[256 + 16],
                             a->allocateChunk(size - 16));
        CPPUNIT_ASSERT_EQUAL(&a->data[256],
                             a->allocateChunk(16));
        CPPUNIT_ASSERT_EQUAL(NULL, a->allocateChunk(1));
        CPPUNIT_ASSERT_EQUAL(NULL, a->allocateAppend(1));
    }

    void test_allocatePrepend() {
        uint32_t size = 256;
        a->allocatePrepend(0);
        CPPUNIT_ASSERT_EQUAL(&a->data[10], a->allocatePrepend(size - 10));
        CPPUNIT_ASSERT_EQUAL(&a->data[0], a->allocatePrepend(10));
        CPPUNIT_ASSERT_EQUAL(NULL, a->allocatePrepend(1));
    }

    void test_allocateAppend() {
        uint32_t size = 2048 - 256;
        a->allocateAppend(0);
        CPPUNIT_ASSERT_EQUAL(&a->data[256],
                       a->allocateAppend(size - 10));
        CPPUNIT_ASSERT_EQUAL(&a->data[2048 - 10],
                       a->allocateAppend(10));
        CPPUNIT_ASSERT_EQUAL(NULL, a->allocateAppend(1));
        CPPUNIT_ASSERT_EQUAL(NULL, a->allocateChunk(1));
    }
  private:
    DISALLOW_COPY_AND_ASSIGN(BufferAllocationTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(BufferAllocationTest);

class BufferChunkTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(BufferChunkTest);

    CPPUNIT_TEST(test_Chunk);
    CPPUNIT_TEST(test_ChunkDerivative);

    CPPUNIT_TEST_SUITE_END();


  public:
    void test_Chunk() {
        Buffer buf;
        char data;
        Buffer::Chunk* c;
        c = Buffer::Chunk::prependToBuffer(&buf, &data, sizeof(data));
        CPPUNIT_ASSERT_EQUAL(&data, c->data);
        CPPUNIT_ASSERT_EQUAL(sizeof(data), c->length);
        CPPUNIT_ASSERT_EQUAL(NULL, c->next);
        CPPUNIT_ASSERT(c->isRawChunk());
    }

    void test_ChunkDerivative() {
        Buffer buf;
        char data;
        NotARawChunk* c;
        c = NotARawChunk::prependToBuffer(&buf, &data, sizeof(data));
        CPPUNIT_ASSERT(!c->isRawChunk());
        ((Buffer::Chunk*) c)->~Chunk();
        CPPUNIT_ASSERT(c->destructed);
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(BufferChunkTest);

class BufferTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(BufferTest);

    CPPUNIT_TEST(test_constructor);
    CPPUNIT_TEST(test_destructor);

    CPPUNIT_TEST(test_reset);

    CPPUNIT_TEST(test_newAllocation);
    CPPUNIT_TEST(test_allocateChunk);
    CPPUNIT_TEST(test_allocatePrepend);
    CPPUNIT_TEST(test_allocateAppend);

    CPPUNIT_TEST(test_prepend);
    CPPUNIT_TEST(test_append);

    CPPUNIT_TEST(test_peek_normal);
    CPPUNIT_TEST(test_peek_offsetGreaterThanTotalLength);

    CPPUNIT_TEST(test_copyChunks);

    CPPUNIT_TEST(test_getRange_inputEdgeCases);
    CPPUNIT_TEST(test_getRange_peek);
    CPPUNIT_TEST(test_getRange_copy);

    CPPUNIT_TEST(test_copy_noop);
    CPPUNIT_TEST(test_copy_normal);

    CPPUNIT_TEST(test_fillFromString);

    CPPUNIT_TEST(test_truncateFront);
    CPPUNIT_TEST(test_truncateEnd);

    CPPUNIT_TEST(test_eq);

    CPPUNIT_TEST_SUITE_END();

    // I've inserted padding in between these arrays so that we don't get lucky
    // by going past end of testStr1 and hitting testStr2, etc.
    char testStr[30];
    char pad1[50];
    char testStr1[10];
    char pad2[50];
    char testStr2[10];
    char pad3[50];
    char testStr3[10];
    char pad4[50];
    char cmpBuf[30];    // To use for strcmp at the end of a test.
    Buffer *buf;

  public:
    BufferTest() : buf(NULL) {
        memcpy(testStr, "ABCDEFGHIJabcdefghijklmnopqrs\0", 30);
        memcpy(testStr1, "ABCDEFGHIJ", 10);
        memcpy(testStr2, "abcdefghij", 10);
        memcpy(testStr3, "klmnopqrs\0", 10);
        memset(pad1, 0xcc, sizeof(pad1));
        memset(pad2, 0xdd, sizeof(pad1));
        memset(pad3, 0xee, sizeof(pad1));
        memset(pad4, 0xff, sizeof(pad1));
    }

    void setUp() {
        // This uses prepend, so the tests for prepend
        // probably shouldn't use this.
        buf = new Buffer();
        Buffer::Chunk::prependToBuffer(buf, testStr3, 10);
        Buffer::Chunk::prependToBuffer(buf, testStr2, 10);
        Buffer::Chunk::prependToBuffer(buf, testStr1, 10);
    }

    void tearDown() { delete buf; }

    void test_constructor() {
        // Basic sanity checks for the constructor.
        Buffer b;
        CPPUNIT_ASSERT_EQUAL(0, b.totalLength);
        CPPUNIT_ASSERT_EQUAL(0, b.numberChunks);
        CPPUNIT_ASSERT_EQUAL(NULL, b.chunks);
        CPPUNIT_ASSERT_EQUAL(&b.initialAllocationContainer.allocation,
                             b.allocations);
        CPPUNIT_ASSERT_EQUAL(NULL, b.allocations->next);
    }

    void test_destructor() {
        // I don't know how I'd test this anymore.
    }

    void test_reset() {
        // Create a Chunk subclass that records when it is destructed.
        static int numChunkDeletes = 0;
        class TChunk : public Buffer::Chunk {
          public:
            TChunk(const void* data, uint32_t length)
                    : Chunk(data, length) {}
            virtual ~TChunk() {
                numChunkDeletes++;
            }
            static TChunk* appendToBuffer(Buffer* buffer, const char* s) {
                TChunk* chunk = new(buffer, CHUNK)
                                    TChunk(s, downCast<uint32_t>(strlen(s)));
                buffer->appendChunk(chunk);
                return chunk;
            }
        };
        Buffer b;
        TChunk::appendToBuffer(&b, "abcd");
        TChunk::appendToBuffer(&b, "12345");
        CPPUNIT_ASSERT_EQUAL("abcd12345", toString(&b));
        b.reset();
        CPPUNIT_ASSERT_EQUAL(2, numChunkDeletes);
        CPPUNIT_ASSERT_EQUAL(0, b.totalLength);
        CPPUNIT_ASSERT_EQUAL(0, b.numberChunks);
        CPPUNIT_ASSERT_EQUAL(NULL, b.chunks);
        CPPUNIT_ASSERT_EQUAL(NULL, b.chunksTail);
        CPPUNIT_ASSERT_EQUAL(&b.initialAllocationContainer.allocation,
                             b.allocations);
        CPPUNIT_ASSERT_EQUAL(2048, b.allocations->chunkTop);
    }

    void test_newAllocation() {
        Buffer b;
        uint32_t s;

        s = Buffer::INITIAL_ALLOCATION_SIZE * 2;
        Buffer::Allocation* a3 = b.newAllocation(s + 16, 0);
        CPPUNIT_ASSERT_EQUAL(s + 16, a3->prependTop);
        CPPUNIT_ASSERT_EQUAL(s + 16, a3->chunkTop);

        s *= 2;
        Buffer::Allocation* a2 = b.newAllocation(0, s + 16);
        CPPUNIT_ASSERT_EQUAL(0, a2->prependTop);
        CPPUNIT_ASSERT_EQUAL(s + 16, a2->chunkTop);

        s *= 2;
        Buffer::Allocation* a1 = b.newAllocation(0, 0);
        CPPUNIT_ASSERT_EQUAL(s / 8, a1->prependTop);
        CPPUNIT_ASSERT_EQUAL(s, a1->chunkTop);

        CPPUNIT_ASSERT_EQUAL(a1, b.allocations);
        CPPUNIT_ASSERT_EQUAL(a2, a1->next);
        CPPUNIT_ASSERT_EQUAL(a3, a2->next);
        CPPUNIT_ASSERT_EQUAL(&b.initialAllocationContainer.allocation,
                             a3->next);
    }

    void test_allocateChunk() {
        Buffer b;

        // fill up the initial allocation to get it out of the way
        b.allocations->chunkTop = b.allocations->appendTop;

        // allocates new Allocation
        b.allocateChunk(1);
        CPPUNIT_ASSERT_EQUAL(&b.initialAllocationContainer.allocation,
                             b.allocations->next);

        // uses existing Allocation
        b.allocateChunk(1);
        CPPUNIT_ASSERT_EQUAL(&b.initialAllocationContainer.allocation,
                             b.allocations->next);
    }

    void test_allocatePrepend() {
        Buffer b;

        // fill up the initial allocation to get it out of the way
        b.allocations->prependTop = 0;

        // allocates new Allocation
        b.allocatePrepend(1);
        CPPUNIT_ASSERT_EQUAL(&b.initialAllocationContainer.allocation,
                             b.allocations->next);

        // uses existing Allocation
        b.allocatePrepend(1);
        CPPUNIT_ASSERT_EQUAL(&b.initialAllocationContainer.allocation,
                             b.allocations->next);
    }

    void test_allocateAppend() {
        Buffer b;

        // fill up the initial allocation to get it out of the way
        b.allocations->appendTop = b.allocations->chunkTop;

        // allocates new Allocation
        b.allocateAppend(1);
        CPPUNIT_ASSERT_EQUAL(&b.initialAllocationContainer.allocation,
                             b.allocations->next);

        // uses existing Allocation
        b.allocateAppend(1);
        CPPUNIT_ASSERT_EQUAL(&b.initialAllocationContainer.allocation,
                             b.allocations->next);
    }

    void test_prepend() {
        Buffer b;
        Buffer::Chunk::prependToBuffer(&b, NULL, 0);
        CPPUNIT_ASSERT_EQUAL(NULL, b.chunksTail->data);
        Buffer::Chunk::prependToBuffer(&b, testStr3, 10);
        Buffer::Chunk::prependToBuffer(&b, testStr2, 10);
        Buffer::Chunk::prependToBuffer(&b, testStr1, 10);
        CPPUNIT_ASSERT_EQUAL(NULL, b.chunksTail->data);
        CPPUNIT_ASSERT_EQUAL("ABCDEFGHIJ | abcdefghij | klmnopqrs/0",
                bufferToDebugString(&b));
    }

    void test_append() {
        Buffer b;
        Buffer::Chunk::appendToBuffer(&b, NULL, 0);
        CPPUNIT_ASSERT_EQUAL(NULL, b.chunksTail->data);
        Buffer::Chunk::appendToBuffer(&b, testStr1, 10);
        Buffer::Chunk::appendToBuffer(&b, testStr2, 10);
        Buffer::Chunk::appendToBuffer(&b, testStr3, 10);
        CPPUNIT_ASSERT_EQUAL(testStr3, b.chunksTail->data);
        CPPUNIT_ASSERT_EQUAL("ABCDEFGHIJ | abcdefghij | klmnopqrs/0",
                bufferToDebugString(&b));
    }

    void test_peek_normal() {
        const void *retVal;
        CPPUNIT_ASSERT_EQUAL(10, buf->peek(0, &retVal));
        CPPUNIT_ASSERT_EQUAL(testStr1, retVal);
        CPPUNIT_ASSERT_EQUAL(1, buf->peek(9, &retVal));
        CPPUNIT_ASSERT_EQUAL(testStr1 + 9, retVal);
        CPPUNIT_ASSERT_EQUAL(10, buf->peek(10, &retVal));
        CPPUNIT_ASSERT_EQUAL(testStr2, retVal);
        CPPUNIT_ASSERT_EQUAL(5, buf->peek(25, &retVal));
        CPPUNIT_ASSERT_EQUAL(testStr3 + 5, retVal);
    }

    void test_peek_offsetGreaterThanTotalLength() {
        const void *retVal;
        CPPUNIT_ASSERT_EQUAL(0, buf->peek(30, &retVal));
        CPPUNIT_ASSERT_EQUAL(NULL, retVal);
        CPPUNIT_ASSERT_EQUAL(0, buf->peek(31, &retVal));
        CPPUNIT_ASSERT_EQUAL(NULL, retVal);
    }

    void test_copyChunks() {
        Buffer b;
        char scratch[50];

        // skip while loop
        strncpy(scratch, "0123456789", 11);
        buf->copyChunks(buf->chunks, 0, 0, scratch + 1);
        CPPUNIT_ASSERT_EQUAL("0123456789", scratch);

        // nonzero offset in first chunk, partial chunk
        strncpy(scratch, "01234567890123456789", 21);
        buf->copyChunks(buf->chunks, 5, 3, scratch + 1);
        CPPUNIT_ASSERT_EQUAL("0FGH4567890123456789", scratch);

        // spans chunks, ends at exactly the end of the buffer
        strncpy(scratch, "0123456789012345678901234567890123456789", 41);
        buf->copyChunks(buf->chunks, 0, 30, scratch + 1);
        // The data contains a null character, so check it in two
        // pieces (one up through the null, one after).
        CPPUNIT_ASSERT_EQUAL("0ABCDEFGHIJabcdefghijklmnopqrs", scratch);
        CPPUNIT_ASSERT_EQUAL("123456789", scratch+31);
    }

    void test_getRange_inputEdgeCases() {
        CPPUNIT_ASSERT_EQUAL(NULL, buf->getRange(0, 0));
        CPPUNIT_ASSERT_EQUAL(NULL, buf->getRange(30, 1));
        CPPUNIT_ASSERT_EQUAL(NULL, buf->getRange(29, 2));
    }

    void test_getRange_peek() {
        CPPUNIT_ASSERT_EQUAL(testStr1, buf->getRange(0, 10));
        CPPUNIT_ASSERT_EQUAL(testStr1 + 3, buf->getRange(3, 2));
        CPPUNIT_ASSERT_EQUAL(testStr2, buf->getRange(10, 10));
        CPPUNIT_ASSERT_EQUAL(testStr2 + 1,  buf->getRange(11, 5));
        CPPUNIT_ASSERT_EQUAL(testStr3, buf->getRange(20, 1));
        CPPUNIT_ASSERT_EQUAL(testStr3 + 9, buf->getRange(29, 1));
    }

    void test_getRange_copy() {
        char out[10];
        strncpy(out, static_cast<const char*>(buf->getRange(9, 2)), 2);
        out[2] = 0;
        CPPUNIT_ASSERT_EQUAL("Ja", out);
    }

    void test_copy_noop() {
        Buffer b;
        CPPUNIT_ASSERT_EQUAL(0, b.copy(0, 0, cmpBuf));
        CPPUNIT_ASSERT_EQUAL(0, b.copy(1, 0, cmpBuf));
        CPPUNIT_ASSERT_EQUAL(0, b.copy(1, 1, cmpBuf));
        CPPUNIT_ASSERT_EQUAL(0, buf->copy(30, 0, cmpBuf));
        CPPUNIT_ASSERT_EQUAL(0, buf->copy(30, 1, cmpBuf));
        CPPUNIT_ASSERT_EQUAL(0, buf->copy(31, 1, cmpBuf));
    }

    void test_copy_normal() {
        char scratch[50];

        // truncate transfer length
        CPPUNIT_ASSERT_EQUAL(5, buf->copy(25, 6, scratch + 1));

        // skip while loop (start in first chunk)
        strncpy(scratch, "01234567890123456789", 21);
        CPPUNIT_ASSERT_EQUAL(5, buf->copy(0, 5, scratch + 1));
        CPPUNIT_ASSERT_EQUAL("0ABCDE67890123456789", scratch);

        // starting point not in first chunk
        strncpy(scratch, "012345678901234567890123456789", 31);
        CPPUNIT_ASSERT_EQUAL(6, buf->copy(20, 6, scratch + 1));
        CPPUNIT_ASSERT_EQUAL("0klmnop78901234567890123456789", scratch);
    }

    void test_fillFromString() {
        Buffer b;

        // Hexadecimal numbers
        b.fillFromString("0xAFaf0900 0xa");
        CPPUNIT_ASSERT_EQUAL("0xafaf0900 10", toString(&b));

        // Decimal numbers
        b.fillFromString("123 -456");
        CPPUNIT_ASSERT_EQUAL("123 -456", toString(&b));

        // Strings
        b.fillFromString("abc def");
        CPPUNIT_ASSERT_EQUAL("abc/0 def/0", toString(&b));
    }

    void test_truncateFront() {
        Buffer b;
        b.truncateFront(0);
        b.truncateFront(10);
        Buffer::Chunk::appendToBuffer(&b, "abc", 3);
        Buffer::Chunk::appendToBuffer(&b, "def", 3);
        Buffer::Chunk::appendToBuffer(&b, "ghi", 3);
        b.truncateFront(0);
        CPPUNIT_ASSERT_EQUAL("abc | def | ghi", bufferToDebugString(&b));
        CPPUNIT_ASSERT_EQUAL(9, b.getTotalLength());
        b.truncateFront(4);
        CPPUNIT_ASSERT_EQUAL("ef | ghi", bufferToDebugString(&b));
        CPPUNIT_ASSERT_EQUAL(5, b.getTotalLength());
        b.truncateFront(2);
        CPPUNIT_ASSERT_EQUAL("ghi", bufferToDebugString(&b));
        CPPUNIT_ASSERT_EQUAL(3, b.getTotalLength());
        b.truncateFront(5);
        CPPUNIT_ASSERT_EQUAL("", bufferToDebugString(&b));
        CPPUNIT_ASSERT_EQUAL(0, b.getTotalLength());
    }

    void test_truncateEnd() {
        Buffer b;
        b.truncateEnd(0);
        b.truncateEnd(10);
        Buffer::Chunk::appendToBuffer(&b, "abc", 3);
        Buffer::Chunk::appendToBuffer(&b, "def", 3);
        Buffer::Chunk::appendToBuffer(&b, "ghi", 3);
        b.truncateEnd(0);
        CPPUNIT_ASSERT_EQUAL("abc | def | ghi", bufferToDebugString(&b));
        CPPUNIT_ASSERT_EQUAL(9, b.getTotalLength());
        b.truncateEnd(4);
        CPPUNIT_ASSERT_EQUAL("abc | de", bufferToDebugString(&b));
        CPPUNIT_ASSERT_EQUAL(5, b.getTotalLength());
        b.truncateEnd(2);
        CPPUNIT_ASSERT_EQUAL("abc", bufferToDebugString(&b));
        CPPUNIT_ASSERT_EQUAL(3, b.getTotalLength());
        b.truncateEnd(5);
        CPPUNIT_ASSERT_EQUAL("", bufferToDebugString(&b));
        CPPUNIT_ASSERT_EQUAL(0, b.getTotalLength());
    }

    void test_eq() {
        Buffer a;
        Buffer b;
        Buffer c;
        Buffer d;
        CPPUNIT_ASSERT(a == b);
        CPPUNIT_ASSERT(b == a);
        Buffer::Chunk::appendToBuffer(&a, "abc", 3);
        Buffer::Chunk::appendToBuffer(&a, "def", 3);
        CPPUNIT_ASSERT(a != b);
        CPPUNIT_ASSERT(b != a);
        Buffer::Chunk::appendToBuffer(&b, "a", 1);
        Buffer::Chunk::appendToBuffer(&b, "bcd", 3);
        Buffer::Chunk::appendToBuffer(&b, "", 0);
        Buffer::Chunk::appendToBuffer(&b, "ef", 2);
        Buffer::Chunk::appendToBuffer(&c, "x", 1);
        Buffer::Chunk::appendToBuffer(&c, "", 0);
        Buffer::Chunk::appendToBuffer(&c, "bcdef", 5);
        Buffer::Chunk::appendToBuffer(&d, "yz", 2);
        Buffer::Chunk::appendToBuffer(&d, "bcde", 4);
        CPPUNIT_ASSERT(a == a);
        CPPUNIT_ASSERT(b == b);
        CPPUNIT_ASSERT(a == b);
        CPPUNIT_ASSERT(b == a);
        CPPUNIT_ASSERT(b != c);
        CPPUNIT_ASSERT(c != b);
        CPPUNIT_ASSERT(c != d);
        CPPUNIT_ASSERT(d != c);
    }

    DISALLOW_COPY_AND_ASSIGN(BufferTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(BufferTest);


class BufferIteratorTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(BufferIteratorTest);
    CPPUNIT_TEST(test_constructor_subrange);
    CPPUNIT_TEST(test_constructor_lengthOutOfBounds);
    CPPUNIT_TEST(test_constructor_offsetOutOfBounds);
    CPPUNIT_TEST(test_isDone);
    CPPUNIT_TEST(test_next);
    CPPUNIT_TEST(test_getData);
    CPPUNIT_TEST(test_getData_onChunkBoundary);
    CPPUNIT_TEST(test_getData_offsetAdjustment);
    CPPUNIT_TEST(test_getLength);
    CPPUNIT_TEST(test_getLength_startAdjustment);
    CPPUNIT_TEST(test_getLength_endAdjustment);
    CPPUNIT_TEST(test_getTotalLength);
    CPPUNIT_TEST(test_getNumberChunks);
    CPPUNIT_TEST(test_getNumberChunks_offsetIntoBuffer);
    CPPUNIT_TEST(test_getNumberChunks_sanityBeyondTheEnd);
    // operator== tested by BufferTest::test_eq
    CPPUNIT_TEST_SUITE_END();

    Buffer* oneChunk;
    Buffer* twoChunks;
    Buffer::Iterator* oneIter;
    Buffer::Iterator* twoIter;
    char x[30];

  public:

    BufferIteratorTest()
        : oneChunk(NULL)
        , twoChunks(NULL)
        , oneIter(NULL)
        , twoIter(NULL)
    {
        memset(x, 'A', sizeof(x) - 1);
        x[sizeof(x) - 1] = '\0';
    }

    void
    setUp()
    {
        oneChunk = new Buffer();
        twoChunks = new Buffer();

        Buffer::Chunk::appendToBuffer(oneChunk, &x[0], 10);

        Buffer::Chunk::appendToBuffer(twoChunks, &x[0], 10);
        Buffer::Chunk::appendToBuffer(twoChunks, &x[10], 20);

        oneIter = new Buffer::Iterator(*oneChunk);
        twoIter = new Buffer::Iterator(*twoChunks);
    }

    void
    tearDown()
    {
        delete twoIter;
        delete oneIter;
        delete twoChunks;
        delete oneChunk;
    }

    void test_constructor_subrange()
    {
        Buffer::Iterator iter(*twoChunks, 12, 15);
        CPPUNIT_ASSERT_EQUAL(iter.current, twoChunks->chunks->next);
        CPPUNIT_ASSERT_EQUAL(12, iter.offset);
        CPPUNIT_ASSERT_EQUAL(15, iter.length);
    }

    void test_constructor_lengthOutOfBounds()
    {
        Buffer::Iterator iter(*twoChunks, 12, 100000);
        CPPUNIT_ASSERT_EQUAL(18, iter.length);
    }

    void test_constructor_offsetOutOfBounds()
    {
        Buffer::Iterator iter(*twoChunks, 100000, 1);
        CPPUNIT_ASSERT_EQUAL(30, iter.offset);
    }

    void test_isDone()
    {
        Buffer zeroChunks;
        Buffer::Iterator iter(zeroChunks);
        CPPUNIT_ASSERT(iter.isDone());

        CPPUNIT_ASSERT(!twoIter->isDone());
        twoIter->next();
        CPPUNIT_ASSERT(!twoIter->isDone());
        twoIter->next();
        CPPUNIT_ASSERT(twoIter->isDone());
    }

    void test_next()
    {
        // Pointing at the chunk.
        CPPUNIT_ASSERT_EQUAL(oneIter->current, oneChunk->chunks);

        oneIter->next();
        // Pointing beyond the chunk.
        CPPUNIT_ASSERT_EQUAL(oneIter->current, oneChunk->chunks->next);
        CPPUNIT_ASSERT(oneIter->isDone());

        oneIter->next();
        // Nothing should've changed since we were already done.
        CPPUNIT_ASSERT_EQUAL(oneIter->current, oneChunk->chunks->next);
    }

    void test_getData()
    {
        // Trivial case; no subrange adjustments.
        CPPUNIT_ASSERT_EQUAL(static_cast<const char *>(oneIter->getData()),
                             &x[0]);
    }

    void test_getData_onChunkBoundary()
    {
        // Start right on chunk boundary.
        Buffer::Iterator iter(*twoChunks, 10, 15);
        CPPUNIT_ASSERT_EQUAL(&x[10], iter.getData());
    }

    void test_getData_offsetAdjustment()
    {
        // Start some distance into second chunk; startOffset adjustment.
        Buffer::Iterator iter(*twoChunks, 12, 15);
        CPPUNIT_ASSERT_EQUAL(&x[12], iter.getData());
    }

    void test_getLength()
    {
        // straight-through, no branches taken
        CPPUNIT_ASSERT_EQUAL(oneIter->getLength(), 10);
    }

    void test_getLength_startAdjustment()
    {
        // adjust due to unused region at front of current
        Buffer::Iterator iter(*twoChunks, 2, 27);
        CPPUNIT_ASSERT_EQUAL(8, iter.getLength());
    }

    void test_getLength_endAdjustment()
    {
        // adjust due to unused region at end of current
        Buffer::Iterator iter(*twoChunks, 0, 7);
        CPPUNIT_ASSERT_EQUAL(7, iter.getLength());
    }

    void test_getTotalLength()
    {
        // Runs off the end
        Buffer::Iterator iter(*twoChunks, 29, 2);
        CPPUNIT_ASSERT_EQUAL(1, iter.getTotalLength());
    }

    void test_getNumberChunks()
    {
        CPPUNIT_ASSERT_EQUAL(2, twoIter->getNumberChunks());
    }

    void test_getNumberChunks_offsetIntoBuffer()
    {
        Buffer::Iterator iter(*twoChunks, 11, 10000);
        CPPUNIT_ASSERT(!iter.numberChunksIsValid);
        CPPUNIT_ASSERT_EQUAL(1, iter.getNumberChunks());
        CPPUNIT_ASSERT(iter.numberChunksIsValid);
    }

    void test_getNumberChunks_sanityBeyondTheEnd()
    {
        Buffer::Iterator iter(*twoChunks, 100000, 100000);
        CPPUNIT_ASSERT_EQUAL(0, iter.getNumberChunks());
    }

    DISALLOW_COPY_AND_ASSIGN(BufferIteratorTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(BufferIteratorTest);

class BufferAllocatorTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(BufferAllocatorTest);

    CPPUNIT_TEST(test_new_prepend);
    CPPUNIT_TEST(test_new_append);
    CPPUNIT_TEST(test_new_chunk);
    CPPUNIT_TEST(test_new_misc);

    CPPUNIT_TEST_SUITE_END();

  public:
    void test_new_prepend() {
        Buffer buf;

        operator new(0, &buf, PREPEND);
        CPPUNIT_ASSERT_EQUAL(0, buf.getTotalLength());

        *(new(&buf, PREPEND) char) = 'z';
        char* y = new(&buf, PREPEND) char;
        *y = 'y';
        NotARawChunk::prependToBuffer(&buf, y, sizeof(*y));
        *(new(&buf, PREPEND) char) = 'x';
        CPPUNIT_ASSERT_EQUAL("x | y | yz", bufferToDebugString(&buf));
    }

    void test_new_append() {
        Buffer buf;

        operator new(0, &buf, APPEND);
        CPPUNIT_ASSERT_EQUAL(0, buf.getTotalLength());

        *(new(&buf, APPEND) char) = 'z';
        char* y = new(&buf, APPEND) char;
        *y = 'y';
        NotARawChunk::appendToBuffer(&buf, y, sizeof(*y));
        *(new(&buf, APPEND) char) = 'x';
        CPPUNIT_ASSERT_EQUAL("zy | y | x", bufferToDebugString(&buf));
    }

    void test_new_chunk() {
        // tested enough by Chunk::prependToBuffer, Chunk::appendToBuffer
    }

    void test_new_misc() {
        // not sure what to test here...
        Buffer buf;
        operator new(0, &buf, MISC);
        new(&buf, MISC) char[10];
        CPPUNIT_ASSERT_EQUAL(0, buf.getTotalLength());
    }

};
CPPUNIT_TEST_SUITE_REGISTRATION(BufferAllocatorTest);

}  // namespace RAMCloud
