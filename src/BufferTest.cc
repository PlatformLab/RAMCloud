/* Copyright (c) 2010-2011 Stanford University
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
#include "MockSyscall.h"

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


class BufferAllocationTest : public ::testing::Test {
  public:
    Buffer::Allocation* a;

    BufferAllocationTest() : a(NULL)
    {
        a = Buffer::Allocation::newAllocation(256, 2048);
    }

    ~BufferAllocationTest() {
        if (a != NULL)
            free(a);
        a = NULL;
    }
  private:
    DISALLOW_COPY_AND_ASSIGN(BufferAllocationTest);
};

TEST_F(BufferAllocationTest, constructor) {

    // make sure Allocation::padding is set correctly.
    EXPECT_EQ(0U, reinterpret_cast<uint64_t>(a->data) & 0x7);

    EXPECT_TRUE(a->next == NULL);
    EXPECT_EQ(256U, a->prependTop);
    EXPECT_EQ(256U, a->appendTop);
    EXPECT_EQ(2048U, a->chunkTop);
}

TEST_F(BufferAllocationTest, destructor) {
}

TEST_F(BufferAllocationTest, reset) {
    a->allocateChunk(32);
    a->allocatePrepend(16);
    a->allocateAppend(64);
    a->next = a;
    a->reset(32, 256);

    EXPECT_TRUE(a->next == NULL);
    EXPECT_EQ(32U, a->prependTop);
    EXPECT_EQ(32U, a->appendTop);
    EXPECT_EQ(256U, a->chunkTop);
}

TEST_F(BufferAllocationTest, allocateChunk) {
    uint32_t size = 2048 - 256;
    a->allocateChunk(0);
    EXPECT_EQ(&a->data[256 + 16],
                            a->allocateChunk(size - 16));
    EXPECT_EQ(&a->data[256],
                            a->allocateChunk(16));
    EXPECT_TRUE(NULL == a->allocateChunk(1));
    EXPECT_TRUE(NULL == a->allocateAppend(1));
}

TEST_F(BufferAllocationTest, allocatePrepend) {
    uint32_t size = 256;
    a->allocatePrepend(0);
    EXPECT_EQ(&a->data[10], a->allocatePrepend(size - 10));
    EXPECT_EQ(&a->data[0], a->allocatePrepend(10));
    EXPECT_TRUE(NULL == a->allocatePrepend(1));
}

TEST_F(BufferAllocationTest, allocateAppend) {
    uint32_t size = 2048 - 256;
    a->allocateAppend(0);
    EXPECT_EQ(&a->data[256],
                    a->allocateAppend(size - 10));
    EXPECT_EQ(&a->data[2048 - 10],
                    a->allocateAppend(10));
    EXPECT_TRUE(NULL == a->allocateAppend(1));
    EXPECT_TRUE(NULL == a->allocateChunk(1));
}

class BufferChunkTest : public ::testing::Test {
  public:
      BufferChunkTest() {}
      ~BufferChunkTest() {}

    DISALLOW_COPY_AND_ASSIGN(BufferChunkTest);
};

TEST_F(BufferChunkTest, Chunk) {
    Buffer buf;
    char data;
    Buffer::Chunk* c;
    c = buf.prepend(&data, sizeof(data));
    EXPECT_EQ(&data, c->data);
    EXPECT_EQ(sizeof(data), c->length);
    EXPECT_TRUE(NULL == c->next);
    EXPECT_TRUE(c->isRawChunk());
}

TEST_F(BufferChunkTest, ChunkDerivative) {
    Buffer buf;
    char data;
    NotARawChunk* c;
    c = NotARawChunk::prependToBuffer(&buf, &data, sizeof(data));
    EXPECT_TRUE(!c->isRawChunk());
    ((Buffer::Chunk*) c)->~Chunk();
    EXPECT_TRUE(c->destructed);
}

TEST_F(BufferChunkTest, prependToBuffer_fromBuffer) {
    const char* firstChunk = "!!";
    const char* secondChunk = "@@@";
    Buffer source;
    source.append(firstChunk, 2);
    source.append(secondChunk, 3);

    const char* thirdChunk = "<<>>";
    Buffer dest;
    dest.append(thirdChunk, 4);
    Buffer::Chunk::prependToBuffer(&dest, &source, 0, 5);
    Buffer::Iterator it(dest);
    EXPECT_EQ(3U, it.getNumberChunks());
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(firstChunk, it.getData());
    it.next();
    EXPECT_EQ(secondChunk, it.getData());
    it.next();
    EXPECT_EQ(thirdChunk, it.getData());
    it.next();
    EXPECT_TRUE(it.isDone());

    // test a partial append
    const char* newFirstChunk = "!@#$%^&*()";
    source.prepend(newFirstChunk, 10);
    Buffer::Chunk::prependToBuffer(&dest, &source, 3, 6);
    Buffer::Iterator it2(dest);
    EXPECT_EQ(4U, it2.getNumberChunks());
    EXPECT_EQ(newFirstChunk + 3, it2.getData());
    EXPECT_EQ(6U, it2.getLength());
}

TEST_F(BufferChunkTest, appendToBuffer_fromBuffer) {
    const char* firstChunk = "<<>>";
    Buffer dest;
    dest.append(firstChunk, 4);

    const char* secondChunk = "!!";
    const char* thirdChunk = "@@@";
    Buffer source;
    source.append(secondChunk, 2);
    source.append(thirdChunk, 3);

    Buffer::Chunk::appendToBuffer(&dest, &source, 0, 5);
    Buffer::Iterator it(dest);
    EXPECT_EQ(3U, it.getNumberChunks());
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(firstChunk, it.getData());
    it.next();
    EXPECT_EQ(secondChunk, it.getData());
    it.next();
    EXPECT_EQ(thirdChunk, it.getData());
    it.next();
    EXPECT_TRUE(it.isDone());

    // test a partial append
    const char* newLastChunk = "!@#$%^&*()";
    source.prepend(newLastChunk, 10);
    Buffer::Chunk::appendToBuffer(&dest, &source, 3, 6);
    Buffer::Iterator it2(dest);
    EXPECT_EQ(4U, it2.getNumberChunks());
    it2.next();
    it2.next();
    it2.next();
    EXPECT_EQ(newLastChunk + 3, it2.getData());
    EXPECT_EQ(6U, it2.getLength());
}

class BufferTest : public ::testing::Test {
  public:

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
    FILE *f;
    char fileName[100];

    BufferTest()
        : buf(NULL)
        , f(NULL)
    {
        memcpy(testStr, "ABCDEFGHIJabcdefghijklmnopqrs\0", 30);
        memcpy(testStr1, "ABCDEFGHIJ", 10);
        memcpy(testStr2, "abcdefghij", 10);
        memcpy(testStr3, "klmnopqrs\0", 10);
        memset(pad1, 0xcc, sizeof(pad1));
        memset(pad2, 0xdd, sizeof(pad1));
        memset(pad3, 0xee, sizeof(pad1));
        memset(pad4, 0xff, sizeof(pad1));

        // This uses prepend, so the tests for prepend
        // probably shouldn't use this.
        buf = new Buffer();
        buf->prepend(testStr3, 10);
        buf->prepend(testStr2, 10);
        buf->prepend(testStr1, 10);
    }

    ~BufferTest()
    {
        delete buf;
        if (f != NULL) {
            fclose(f);
            unlink(fileName);
        }
    }

    void openFile() {
        strncpy(fileName, "/tmp/ramcloud-buffer-test-delete-this-XXXXXX",
                sizeof(fileName));
        int fd = mkstemp(fileName);
        f = fdopen(fd, "r+");
    }

    DISALLOW_COPY_AND_ASSIGN(BufferTest);
};

TEST_F(BufferTest, constructor) {
    // Basic sanity checks for the constructor.
    Buffer b;
    EXPECT_EQ(0U, b.totalLength);
    EXPECT_EQ(0U, b.numberChunks);
    EXPECT_TRUE(NULL == b.chunks);
    EXPECT_EQ(&b.initialAllocationContainer.allocation,
                            b.allocations);
    EXPECT_TRUE(NULL == b.allocations->next);
}

TEST_F(BufferTest, destructor) {
    // I don't know how I'd test this anymore.
}

TEST_F(BufferTest, reset) {
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
    EXPECT_EQ("abcd12345", TestUtil::toString(&b));
    b.reset();
    EXPECT_EQ(2, numChunkDeletes);
    EXPECT_EQ(0U, b.totalLength);
    EXPECT_EQ(0U, b.numberChunks);
    EXPECT_TRUE(NULL == b.chunks);
    EXPECT_TRUE(NULL == b.chunksTail);
    EXPECT_EQ(&b.initialAllocationContainer.allocation,
                            b.allocations);
    EXPECT_EQ(2048U, b.allocations->chunkTop);
}

TEST_F(BufferTest, newAllocation) {
    Buffer b;
    uint32_t s;

    s = Buffer::INITIAL_ALLOCATION_SIZE * 2;
    Buffer::Allocation* a3 = b.newAllocation(s + 16, 0);
    EXPECT_EQ(s + 16, a3->prependTop);
    EXPECT_EQ(s + 16, a3->chunkTop);

    s *= 2;
    Buffer::Allocation* a2 = b.newAllocation(0, s + 16);
    EXPECT_EQ(0U, a2->prependTop);
    EXPECT_EQ(s + 16, a2->chunkTop);

    s *= 2;
    Buffer::Allocation* a1 = b.newAllocation(0, 0);
    EXPECT_EQ(s / 8, a1->prependTop);
    EXPECT_EQ(s, a1->chunkTop);

    EXPECT_EQ(a1, b.allocations);
    EXPECT_EQ(a2, a1->next);
    EXPECT_EQ(a3, a2->next);
    EXPECT_EQ(&b.initialAllocationContainer.allocation,
                            a3->next);
}

TEST_F(BufferTest, allocateChunk) {
    Buffer b;

    // fill up the initial allocation to get it out of the way
    b.allocations->chunkTop = b.allocations->appendTop;

    // allocates new Allocation
    b.allocateChunk(1);
    EXPECT_EQ(&b.initialAllocationContainer.allocation,
                            b.allocations->next);

    // uses existing Allocation
    b.allocateChunk(1);
    EXPECT_EQ(&b.initialAllocationContainer.allocation,
                            b.allocations->next);
}

TEST_F(BufferTest, allocatePrepend) {
    Buffer b;

    // fill up the initial allocation to get it out of the way
    b.allocations->prependTop = 0;

    // allocates new Allocation
    b.allocatePrepend(1);
    EXPECT_EQ(&b.initialAllocationContainer.allocation,
                            b.allocations->next);

    // uses existing Allocation
    b.allocatePrepend(1);
    EXPECT_EQ(&b.initialAllocationContainer.allocation,
                            b.allocations->next);
}

TEST_F(BufferTest, allocateAppend) {
    Buffer b;

    // fill up the initial allocation to get it out of the way
    b.allocations->appendTop = b.allocations->chunkTop;

    // allocates new Allocation
    b.allocateAppend(1);
    EXPECT_EQ(&b.initialAllocationContainer.allocation,
                            b.allocations->next);

    // uses existing Allocation
    b.allocateAppend(1);
    EXPECT_EQ(&b.initialAllocationContainer.allocation,
                            b.allocations->next);
}

TEST_F(BufferTest, prepend) {
    Buffer b;
    b.prepend(NULL, 0);
    EXPECT_TRUE(NULL == b.chunksTail->data);
    b.prepend(testStr3, 10);
    b.prepend(testStr2, 10);
    b.prepend(testStr1, 10);
    EXPECT_TRUE(NULL == b.chunksTail->data);
    EXPECT_EQ("ABCDEFGHIJ | abcdefghij | klmnopqrs/0",
            TestUtil::bufferToDebugString(&b));
}

TEST_F(BufferTest, append) {
    Buffer b;
    b.append(NULL, 0);
    EXPECT_TRUE(NULL == b.chunksTail->data);
    b.append(testStr1, 10);
    b.append(testStr2, 10);
    b.append(testStr3, 10);
    EXPECT_EQ(testStr3, b.chunksTail->data);
    EXPECT_EQ("ABCDEFGHIJ | abcdefghij | klmnopqrs/0",
            TestUtil::bufferToDebugString(&b));
}

TEST_F(BufferTest, peek_normal) {
    const void *retVal;
    EXPECT_EQ(10U, buf->peek(0, &retVal));
    EXPECT_EQ(testStr1, retVal);
    EXPECT_EQ(1U, buf->peek(9, &retVal));
    EXPECT_EQ(testStr1 + 9, retVal);
    EXPECT_EQ(10U, buf->peek(10, &retVal));
    EXPECT_EQ(testStr2, retVal);
    EXPECT_EQ(5U, buf->peek(25, &retVal));
    EXPECT_EQ(testStr3 + 5, retVal);
}

TEST_F(BufferTest, peek_offsetGreaterThanTotalLength) {
    const void *retVal;
    EXPECT_EQ(0U, buf->peek(30, &retVal));
    EXPECT_TRUE(NULL == retVal);
    EXPECT_EQ(0U, buf->peek(31, &retVal));
    EXPECT_TRUE(NULL == retVal);
}

TEST_F(BufferTest, copyChunks) {
    Buffer b;
    char scratch[50];

    // skip while loop
    strncpy(scratch, "0123456789", 11);
    buf->copyChunks(buf->chunks, 0, 0, scratch + 1);
    EXPECT_STREQ("0123456789", scratch);

    // nonzero offset in first chunk, partial chunk
    strncpy(scratch, "01234567890123456789", 21);
    buf->copyChunks(buf->chunks, 5, 3, scratch + 1);
    EXPECT_STREQ("0FGH4567890123456789", scratch);

    // spans chunks, ends at exactly the end of the buffer
    strncpy(scratch, "0123456789012345678901234567890123456789", 41);
    buf->copyChunks(buf->chunks, 0, 30, scratch + 1);
    // The data contains a null character, so check it in two
    // pieces (one up through the null, one after).
    EXPECT_STREQ("0ABCDEFGHIJabcdefghijklmnopqrs", scratch);
    EXPECT_STREQ("123456789", scratch+31);
}

TEST_F(BufferTest, getRange_inputEdgeCases) {
    EXPECT_TRUE(NULL == buf->getRange(0, 0));
    EXPECT_TRUE(NULL == buf->getRange(30, 1));
    EXPECT_TRUE(NULL == buf->getRange(29, 2));
}

TEST_F(BufferTest, getRange_peek) {
    EXPECT_STREQ(testStr1, static_cast<const char*>(buf->getRange(0, 10)));
    EXPECT_STREQ(testStr1 + 3, static_cast<const char*>(buf->getRange(3, 2)));
    EXPECT_STREQ(testStr2, static_cast<const char*>(buf->getRange(10, 10)));
    EXPECT_STREQ(testStr2 + 1,
                 static_cast<const char*>(buf->getRange(11, 5)));
    EXPECT_STREQ(testStr3, static_cast<const char*>(buf->getRange(20, 1)));
    EXPECT_STREQ(testStr3 + 9, static_cast<const char*>(buf->getRange(29, 1)));
}

TEST_F(BufferTest, getRange_copy) {
    char out[10];
    strncpy(out, static_cast<const char*>(buf->getRange(9, 2)), 2);
    out[2] = 0;
    EXPECT_STREQ("Ja", out);
}

TEST_F(BufferTest, copy_noop) {
    Buffer b;
    EXPECT_EQ(0U, b.copy(0, 0, cmpBuf));
    EXPECT_EQ(0U, b.copy(1, 0, cmpBuf));
    EXPECT_EQ(0U, b.copy(1, 1, cmpBuf));
    EXPECT_EQ(0U, buf->copy(30, 0, cmpBuf));
    EXPECT_EQ(0U, buf->copy(30, 1, cmpBuf));
    EXPECT_EQ(0U, buf->copy(31, 1, cmpBuf));
}

TEST_F(BufferTest, copy_normal) {
    char scratch[50];

    // truncate transfer length
    EXPECT_EQ(5U, buf->copy(25U, 6, scratch + 1));

    // skip while loop (start in first chunk)
    strncpy(scratch, "01234567890123456789", 21);
    EXPECT_EQ(5U, buf->copy(0, 5, scratch + 1));
    EXPECT_STREQ("0ABCDE67890123456789", scratch);

    // starting point not in first chunk
    strncpy(scratch, "012345678901234567890123456789", 31);
    EXPECT_EQ(6U, buf->copy(20, 6, scratch + 1));
    EXPECT_STREQ("0klmnop78901234567890123456789", scratch);
}

TEST_F(BufferTest, write_basics) {
    openFile();
    EXPECT_EQ(20u, buf->write(5, 20, f));
    fflush(f);
    EXPECT_EQ("FGHIJabcdefghijklmno", TestUtil::readFile(fileName));
}

TEST_F(BufferTest, write_offsetTooLarge) {
    openFile();
    EXPECT_EQ(0u, buf->write(30, 5, f));
    fflush(f);
    EXPECT_EQ("", TestUtil::readFile(fileName));
}

TEST_F(BufferTest, write_lengthLongerThanBuffer) {
    openFile();
    buf->truncateEnd(1);
    EXPECT_EQ(4u, buf->write(25, 5, f));
    fflush(f);
    EXPECT_EQ("pqrs", TestUtil::readFile(fileName));
}

TEST_F(BufferTest, write_IoError) {
    MockSyscall sys;
    Syscall *savedSyscall = Buffer::sys;
    Buffer::sys = &sys;
    openFile();
    sys.fwriteResult = 3;
    EXPECT_EQ(3u, buf->write(5, 20, f));
    Buffer::sys = savedSyscall;
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

TEST_F(BufferTest, truncateFront) {
    Buffer b;
    b.truncateFront(0);
    b.truncateFront(10);
    b.append("abc", 3);
    b.append("def", 3);
    b.append("ghi", 3);
    b.truncateFront(0);
    EXPECT_EQ("abc | def | ghi", TestUtil::bufferToDebugString(&b));
    EXPECT_EQ(9U, b.getTotalLength());
    b.truncateFront(4);
    EXPECT_EQ("ef | ghi", TestUtil::bufferToDebugString(&b));
    EXPECT_EQ(5U, b.getTotalLength());
    b.truncateFront(2);
    EXPECT_EQ("ghi", TestUtil::bufferToDebugString(&b));
    EXPECT_EQ(3U, b.getTotalLength());
    b.truncateFront(5);
    EXPECT_EQ("", TestUtil::bufferToDebugString(&b));
    EXPECT_EQ(0U, b.getTotalLength());
}

TEST_F(BufferTest, truncateEnd) {
    Buffer b;
    b.truncateEnd(0);
    b.truncateEnd(10);
    b.append("abc", 3);
    b.append("def", 3);
    b.append("ghi", 3);
    b.truncateEnd(0);
    EXPECT_EQ("abc | def | ghi", TestUtil::bufferToDebugString(&b));
    EXPECT_EQ(9U, b.getTotalLength());
    b.truncateEnd(4);
    EXPECT_EQ("abc | de", TestUtil::bufferToDebugString(&b));
    EXPECT_EQ(5U, b.getTotalLength());
    b.truncateEnd(2);
    EXPECT_EQ("abc", TestUtil::bufferToDebugString(&b));
    EXPECT_EQ(3U, b.getTotalLength());
    b.truncateEnd(5);
    EXPECT_EQ("", TestUtil::bufferToDebugString(&b));
    EXPECT_EQ(0U, b.getTotalLength());
}

TEST_F(BufferTest, eq) {
    Buffer a;
    Buffer b;
    Buffer c;
    Buffer d;
    EXPECT_TRUE(a == b);
    EXPECT_TRUE(b == a);
    a.append("abc", 3);
    a.append("def", 3);
    EXPECT_TRUE(a != b);
    EXPECT_TRUE(b != a);
    b.append("a", 1);
    b.append("bcd", 3);
    b.append("", 0);
    b.append("ef", 2);
    c.append("x", 1);
    c.append("", 0);
    c.append("bcdef", 5);
    d.append("yz", 2);
    d.append("bcde", 4);
    EXPECT_TRUE(a == a);
    EXPECT_TRUE(b == b);
    EXPECT_TRUE(a == b);
    EXPECT_TRUE(b == a);
    EXPECT_TRUE(b != c);
    EXPECT_TRUE(c != b);
    EXPECT_TRUE(c != d);
    EXPECT_TRUE(d != c);
}

class BufferIteratorTest : public ::testing::Test {
  public:
    Buffer* oneChunk;
    Buffer* twoChunks;
    Buffer::Iterator* oneIter;
    Buffer::Iterator* twoIter;
    char x[30];

    BufferIteratorTest()
        : oneChunk(NULL)
        , twoChunks(NULL)
        , oneIter(NULL)
        , twoIter(NULL)
    {
        memset(x, 'A', sizeof(x) - 1);
        x[sizeof(x) - 1] = '\0';
        oneChunk = new Buffer();
        twoChunks = new Buffer();

        oneChunk->append(&x[0], 10);

        twoChunks->append(&x[0], 10);
        twoChunks->append(&x[10], 20);

        oneIter = new Buffer::Iterator(*oneChunk);
        twoIter = new Buffer::Iterator(*twoChunks);
    }

    ~BufferIteratorTest()
    {
        delete twoIter;
        delete oneIter;
        delete twoChunks;
        delete oneChunk;
    }

    DISALLOW_COPY_AND_ASSIGN(BufferIteratorTest);
};

TEST_F(BufferIteratorTest, constructor_subrange) {
    Buffer::Iterator iter(*twoChunks, 12, 15);
    EXPECT_EQ(iter.current, twoChunks->chunks->next);
    EXPECT_EQ(12U, iter.offset);
    EXPECT_EQ(15U, iter.length);
}

TEST_F(BufferIteratorTest, constructor_lengthOutOfBounds) {
    Buffer::Iterator iter(*twoChunks, 12, 100000);
    EXPECT_EQ(18U, iter.length);
}

TEST_F(BufferIteratorTest, constructor_offsetOutOfBounds) {
    Buffer::Iterator iter(*twoChunks, 100000, 1);
    EXPECT_EQ(30U, iter.offset);
}

TEST_F(BufferIteratorTest, isDone) {
    Buffer zeroChunks;
    Buffer::Iterator iter(zeroChunks);
    EXPECT_TRUE(iter.isDone());

    EXPECT_TRUE(!twoIter->isDone());
    twoIter->next();
    EXPECT_TRUE(!twoIter->isDone());
    twoIter->next();
    EXPECT_TRUE(twoIter->isDone());
}

TEST_F(BufferIteratorTest, next) {
    // Pointing at the chunk.
    EXPECT_EQ(oneIter->current, oneChunk->chunks);

    oneIter->next();
    // Pointing beyond the chunk.
    EXPECT_EQ(oneIter->current, oneChunk->chunks->next);
    EXPECT_TRUE(oneIter->isDone());

    oneIter->next();
    // Nothing should've changed since we were already done.
    EXPECT_EQ(oneIter->current, oneChunk->chunks->next);
}

TEST_F(BufferIteratorTest, getData) {
    // Trivial case; no subrange adjustments.
    EXPECT_EQ(static_cast<const char *>(oneIter->getData()),
                            &x[0]);
}

TEST_F(BufferIteratorTest, getData_onChunkBoundary) {
    // Start right on chunk boundary.
    Buffer::Iterator iter(*twoChunks, 10, 15);
    EXPECT_EQ(&x[10], iter.getData());
}

TEST_F(BufferIteratorTest, getData_offsetAdjustment) {
    // Start some distance into second chunk; startOffset adjustment.
    Buffer::Iterator iter(*twoChunks, 12, 15);
    EXPECT_EQ(&x[12], iter.getData());
}

TEST_F(BufferIteratorTest, getLength) {
    // straight-through, no branches taken
    EXPECT_EQ(oneIter->getLength(), 10U);
}

TEST_F(BufferIteratorTest, getLength_startAdjustment) {
    // adjust due to unused region at front of current
    Buffer::Iterator iter(*twoChunks, 2, 27);
    EXPECT_EQ(8U, iter.getLength());
}

TEST_F(BufferIteratorTest, getLength_endAdjustment) {
    // adjust due to unused region at end of current
    Buffer::Iterator iter(*twoChunks, 0, 7);
    EXPECT_EQ(7U, iter.getLength());
}

TEST_F(BufferIteratorTest, getTotalLength) {
    // Runs off the end
    Buffer::Iterator iter(*twoChunks, 29, 2);
    EXPECT_EQ(1U, iter.getTotalLength());
}

TEST_F(BufferIteratorTest, getNumberChunks) {
    EXPECT_EQ(2U, twoIter->getNumberChunks());
}

TEST_F(BufferIteratorTest, getNumberChunks_offsetIntoBuffer) {
    Buffer::Iterator iter(*twoChunks, 11, 10000);
    EXPECT_TRUE(!iter.numberChunksIsValid);
    EXPECT_EQ(1U, iter.getNumberChunks());
    EXPECT_TRUE(iter.numberChunksIsValid);
}

TEST_F(BufferIteratorTest, getNumberChunks_sanityBeyondTheEnd) {
    Buffer::Iterator iter(*twoChunks, 100000, 100000);
    EXPECT_EQ(0U, iter.getNumberChunks());
}

class BufferAllocatorTest : public ::testing::Test {
  public:
      BufferAllocatorTest() {}
      ~BufferAllocatorTest() {}

    DISALLOW_COPY_AND_ASSIGN(BufferAllocatorTest);
};

TEST_F(BufferAllocatorTest, new_prepend) {
    Buffer buf;

    operator new(0, &buf, PREPEND);
    EXPECT_EQ(0U, buf.getTotalLength());

    *(new(&buf, PREPEND) char) = 'z';
    char* y = new(&buf, PREPEND) char;
    *y = 'y';
    NotARawChunk::prependToBuffer(&buf, y, sizeof(*y));
    *(new(&buf, PREPEND) char) = 'x';
    EXPECT_EQ("x | y | yz", TestUtil::bufferToDebugString(&buf));
}

TEST_F(BufferAllocatorTest, new_append) {
    Buffer buf;

    operator new(0, &buf, APPEND);
    EXPECT_EQ(0U, buf.getTotalLength());

    *(new(&buf, APPEND) char) = 'z';
    char* y = new(&buf, APPEND) char;
    *y = 'y';
    NotARawChunk::appendToBuffer(&buf, y, sizeof(*y));
    *(new(&buf, APPEND) char) = 'x';
    EXPECT_EQ("zy | y | x", TestUtil::bufferToDebugString(&buf));
}

TEST_F(BufferAllocatorTest, new_chunk) {
    // tested enough by Chunk::prependToBuffer, Chunk::appendToBuffer
}

TEST_F(BufferAllocatorTest, new_misc) {
    // not sure what to test here...
    Buffer buf;
    operator new(0, &buf, MISC);
    new(&buf, MISC) char[10];
    EXPECT_EQ(0U, buf.getTotalLength());
}

}  // namespace RAMCloud
