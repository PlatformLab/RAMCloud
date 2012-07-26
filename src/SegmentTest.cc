/* Copyright (c) 2010-2012 Stanford University
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

#include "TestUtil.h"

#include "Segment.h"
#include "StringUtil.h"
#include "Log.h"
#include "LogEntryTypes.h"

namespace RAMCloud {

/**
 * Unit tests for Segment.
 */
class SegmentTest : public ::testing::TestWithParam<Segment::Allocator*> {
  public:
    class HorriblyFragmentedAllocator : public Segment::Allocator {
      public:
        HorriblyFragmentedAllocator()
            : currentBuffer(NULL), offset(0), buffers()
        {
        }

        ~HorriblyFragmentedAllocator()
        {
            foreach (uint8_t *b, buffers) {
                // Each buffer alternates between space we allocate and do not
                // allocate. Ensure any unallocated space was untouched.
                bool dirty = false;
                for (uint32_t off = getSegletSize();
                     off < 2 * getSegmentSize();
                     off += (2 * getSegletSize())) {
                    for (uint32_t i = 0; i < getSegletSize(); i++)
                        dirty |= (b[off + i] != 0xaa);
                }

                EXPECT_FALSE(dirty);
            
                delete[] b;
            }
        }

        uint32_t getSegletsPerSegment() { return 10000; }
        uint32_t getSegletSize() { return 7; }

        void*
        alloc()
        {
            // Not doing a malloc per tiny seglet saves a ton of test time.
            if (currentBuffer == NULL) {
                currentBuffer = new uint8_t[2 * getSegmentSize()];
                memset(currentBuffer, 0xaa, 2 * getSegmentSize());
                buffers.push_back(currentBuffer);
                offset = 0;
            }

            uint8_t* allocation = &currentBuffer[offset]; 
            offset += (2 * getSegletSize());

            if (offset == (2 * getSegmentSize()))
                currentBuffer = NULL;
            
            return allocation;
        }

        void free(void* seglet) { }

      private:
        uint8_t* currentBuffer;
        uint32_t offset;
        vector<uint8_t*> buffers;

        DISALLOW_COPY_AND_ASSIGN(HorriblyFragmentedAllocator);
    };

    SegmentTest()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(SegmentTest);
};

// Run tests with various different backing allocators, to stress the
// code with different fragmentation in the backing segment memory.
Segment::DefaultHeapAllocator boringDefaultAllocator;
SegmentTest::HorriblyFragmentedAllocator horriblyFragmentedAllocator;
INSTANTIATE_TEST_CASE_P(SegmentTestAllocators,
                        SegmentTest,
                        ::testing::Values(&boringDefaultAllocator,
                                          &horriblyFragmentedAllocator));

TEST_P(SegmentTest, constructor) {
    Segment s(*GetParam());
    EXPECT_FALSE(s.closed);
    EXPECT_EQ(0U, s.tail);

    // Footer should always exist.
    Buffer buffer;
    s.appendToBuffer(buffer);
    const Segment::EntryHeader* entryHeader = reinterpret_cast<
        const Segment::EntryHeader*>(buffer.getStart<Segment::EntryHeader>());
    EXPECT_EQ(LOG_ENTRY_TYPE_SEGFOOTER, entryHeader->getType());
    const Segment::Footer* footer = reinterpret_cast<const Segment::Footer*>(
        buffer.getRange(2, sizeof(*footer)));
    EXPECT_FALSE(footer->closed);
    EXPECT_EQ(0x722308dcU, footer->checksum);
}

TEST_F(SegmentTest, constructor_priorSegmentBuffer) {
    Segment previous;
    previous.append(LOG_ENTRY_TYPE_OBJ, "hi", 3);
    Buffer buffer;
    previous.appendToBuffer(buffer);

    const void* p = buffer.getRange(0, buffer.getTotalLength());
    Segment s(p, buffer.getTotalLength());

    EXPECT_TRUE(s.fakeAllocator);
    EXPECT_EQ(&*s.fakeAllocator, &s.allocator);
    EXPECT_EQ(1U, s.seglets.size());
    EXPECT_TRUE(s.closed);
    EXPECT_EQ(s.tail, buffer.getTotalLength());
    EXPECT_EQ(p, s.seglets[0]);
}

TEST_P(SegmentTest, append_blackBox) {
    Segment s(*GetParam());

    char buf[1000];
    for (uint32_t i = 0; i < 1000; i += 100) {
        uint32_t offset;
        EXPECT_TRUE(s.append(LOG_ENTRY_TYPE_OBJ, buf, i, offset));
        
        Buffer buffer;
        EXPECT_EQ(i, s.appendEntryToBuffer(offset, buffer));
        EXPECT_EQ(0, memcmp(buf, buffer.getRange(0, i), i));
    }
}

TEST_P(SegmentTest, append_outOfSpace) {
    Segment::Allocator* allocator = GetParam();
    Segment s(*allocator);

    // How many N-length writes can we make to this segment?
    char buf[107];
    uint32_t bytesPerAppend = s.bytesNeeded(sizeof(buf));
    uint32_t expectedAppends =
        (allocator->getSegmentSize() - s.bytesNeeded(sizeof32(Segment::Footer)))
            / bytesPerAppend;

    uint32_t actualAppends = 0;
    while (s.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf)))
        actualAppends++;

    EXPECT_EQ(expectedAppends, actualAppends);
    EXPECT_EQ(allocator->getSegletsPerSegment(), s.getSegletsAllocated());
    EXPECT_GE(allocator->getSegmentSize() - s.getTailOffset(),
        s.bytesNeeded(sizeof(Segment::Footer)));
}

TEST_P(SegmentTest, append_whiteBox) {
    Segment s(*GetParam());

    uint32_t offset;
    s.append(LOG_ENTRY_TYPE_OBJ, "hi", 2, offset);

    EXPECT_EQ(0U, offset);
    EXPECT_EQ(4U, s.getTailOffset());

    Buffer buffer;
    s.appendToBuffer(buffer);
    EXPECT_EQ(0, memcmp("hi", buffer.getRange(2, 2), 2));

    const Segment::EntryHeader* entryHeader = reinterpret_cast<
        const Segment::EntryHeader*>(buffer.getRange(4, sizeof(*entryHeader)));
    EXPECT_EQ(LOG_ENTRY_TYPE_SEGFOOTER, entryHeader->getType());
    EXPECT_EQ(1U, entryHeader->getLengthBytes());
    const Segment::Footer* footer = reinterpret_cast<const Segment::Footer*>(
        buffer.getRange(6, sizeof(*footer)));
    EXPECT_FALSE(footer->closed);
    EXPECT_EQ(0xa0614ee6, footer->checksum);
}

TEST_P(SegmentTest, append_differentLengthBytes) {
    uint32_t oneByteLengths[] = { 0, 255 }; 
    uint32_t twoByteLengths[] = { 256, 65535 };
    uint32_t threeByteLengths[] = { 65536 };
    struct {
        uint32_t expectedLengthBytes;
        uint32_t* bytesToAppend;
        uint32_t bytesToAppendLength;
    } tests[] = {
        { 1, oneByteLengths, arrayLength(oneByteLengths) },
        { 2, twoByteLengths, arrayLength(twoByteLengths) },
        { 3, threeByteLengths, arrayLength(threeByteLengths) }
        // 4-byte lengths? Fuhgeddaboudit!
    };

    for (uint32_t i = 0; i < unsafeArrayLength(tests); i++) {
        for (uint32_t j = 0; j < tests[i].bytesToAppendLength; j++) {
            uint32_t length = tests[i].bytesToAppend[j];

            char buf[length];
            Segment s(*GetParam());
            s.append(LOG_ENTRY_TYPE_OBJ, buf, length);
            EXPECT_EQ(sizeof(Segment::EntryHeader) +
                        tests[i].expectedLengthBytes + length,
                      s.getTailOffset());

            const Segment::EntryHeader* entryHeader = NULL;
            Buffer buffer;
            s.appendToBuffer(buffer, 0, sizeof(*entryHeader));
            entryHeader = buffer.getStart<Segment::EntryHeader>();

            EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, entryHeader->getType());
            EXPECT_EQ(tests[i].expectedLengthBytes,
                      entryHeader->getLengthBytes());
        }
    }
}

TEST_P(SegmentTest, close) {
    Segment s(*GetParam());
    EXPECT_FALSE(s.closed);
    s.close();
    EXPECT_TRUE(s.closed);

    Buffer buffer;
    s.appendToBuffer(buffer);
    const Segment::Footer* footer = reinterpret_cast<const Segment::Footer*>(
        buffer.getRange(buffer.getTotalLength() - sizeof32(Segment::Footer),
                        sizeof32(Segment::Footer)));
    EXPECT_TRUE(footer->closed);
    EXPECT_EQ(0x80488bdfU, footer->checksum);
}

TEST_P(SegmentTest, appendToBuffer_partial) {
    Segment s(*GetParam());
    uint32_t offset;
    s.append(LOG_ENTRY_TYPE_OBJ, "this is only a test!", 21, offset);

    Buffer buffer;
    s.appendToBuffer(buffer, 2, 21);
    EXPECT_EQ(21U, buffer.getTotalLength());
    EXPECT_STREQ("this is only a test!",
        reinterpret_cast<const char*>(buffer.getRange(0, 21)));
}

TEST_P(SegmentTest, appendToBuffer_all) {
    // Should always include the footer, even if nothing has been appended.
    Segment s(*GetParam());
    Buffer buffer;
    s.appendToBuffer(buffer);
    EXPECT_EQ(7U, buffer.getTotalLength());

    buffer.reset();
    s.append(LOG_ENTRY_TYPE_OBJ, "yo!", 3);
    s.appendToBuffer(buffer);
    EXPECT_EQ(7U + 5U, buffer.getTotalLength());
}

TEST_P(SegmentTest, appendEntryToBuffer) {
    Segment s(*GetParam());
    uint32_t offset;
    s.append(LOG_ENTRY_TYPE_OBJ, "this is only a test!", 21, offset);

    Buffer buffer;
    s.appendEntryToBuffer(offset, buffer);
    EXPECT_EQ(21U, buffer.getTotalLength());
    EXPECT_STREQ("this is only a test!",
        reinterpret_cast<const char*>(buffer.getRange(0, 21)));
}

TEST_P(SegmentTest, getEntryTypeAt_and_getEntryLengthAt) {
    Segment s(*GetParam());

    for (int i = 0; i < 50; i++) {
        uint32_t length = downCast<uint32_t>(generateRandom() % 100);
        char data[length];
        uint32_t offset;
        s.append(LOG_ENTRY_TYPE_OBJTOMB, data, length, offset);
        EXPECT_EQ(LOG_ENTRY_TYPE_OBJTOMB, s.getEntryTypeAt(offset));
        EXPECT_EQ(length, s.getEntryLengthAt(offset));
    }
}

TEST_P(SegmentTest, getSegletsAllocated) {
    Segment::Allocator* allocator = GetParam();
    Segment s(*allocator);
    EXPECT_EQ(allocator->getSegletsPerSegment(), s.getSegletsAllocated());
}

TEST_P(SegmentTest, getSegletsNeeded) {
    Segment::Allocator* allocator = GetParam();
    Segment s(*allocator);
    EXPECT_EQ(1U, s.getSegletsNeeded());

    char buf[allocator->getSegletSize()];
    bool ok = s.append(LOG_ENTRY_TYPE_OBJ, buf, allocator->getSegletSize());
    if (allocator->getSegletsPerSegment() > 1) {
        EXPECT_TRUE(ok);
        EXPECT_GE(s.getSegletsNeeded(), 2U);
        EXPECT_LE(s.getSegletsNeeded(), 3U);
    } else {
        EXPECT_FALSE(ok);
        EXPECT_EQ(1U, s.getSegletsNeeded());
    }
}

TEST_P(SegmentTest, appendFooter) {
    Segment s(*GetParam());

    // Appending the footer shouldn't alter the tail or the checksum
    // we've accumulated thus far.
    s.append(LOG_ENTRY_TYPE_OBJ, "blah", 4);
    uint32_t tail = s.getTailOffset();
    Crc32C checksum = s.checksum;
    s.appendFooter();
    EXPECT_EQ(tail, s.getTailOffset());
    EXPECT_EQ(checksum.getResult(), s.checksum.getResult());
}

TEST_P(SegmentTest, getEntryDataOffset_and_getEntryDataLength) {
    {
        Segment s(*GetParam());
        EXPECT_EQ(2U, s.getEntryDataOffset(0));
        char buf[200];
        s.append(LOG_ENTRY_TYPE_OBJ, buf, 200);
        EXPECT_EQ(2U, s.getEntryDataOffset(0));
        EXPECT_EQ(200U, s.getEntryDataLength(0));
    }

    {
        Segment s(*GetParam());
        EXPECT_EQ(2U, s.getEntryDataOffset(0));
        char buf[2000];
        s.append(LOG_ENTRY_TYPE_OBJ, buf, 2000);
        EXPECT_EQ(3U, s.getEntryDataOffset(0));
        EXPECT_EQ(2000U, s.getEntryDataLength(0));
    }

    {
        Segment s(*GetParam());
        s.append(LOG_ENTRY_TYPE_OBJ, NULL, 0);  // EntryHeader at 0
        s.append(LOG_ENTRY_TYPE_OBJ, NULL, 0);  // EntryHeader at 2
        s.append(LOG_ENTRY_TYPE_OBJ, "hi", 2);  // EntryHeader at 4
        s.append(LOG_ENTRY_TYPE_OBJ, NULL, 0);  // EntryHeader at 8

        EXPECT_EQ(2U, s.getEntryDataOffset(0));
        EXPECT_EQ(4U, s.getEntryDataOffset(2));
        EXPECT_EQ(6U, s.getEntryDataOffset(4));
        EXPECT_EQ(10U, s.getEntryDataOffset(8));

        EXPECT_EQ(0U, s.getEntryDataLength(0));
        EXPECT_EQ(0U, s.getEntryDataLength(2));
        EXPECT_EQ(2U, s.getEntryDataLength(4));
        EXPECT_EQ(0U, s.getEntryDataLength(8));
    }
}

TEST_P(SegmentTest, getAddressAt) {
    Segment s(*GetParam());
    void* nullPtr = NULL;
    EXPECT_NE(nullPtr, s.getAddressAt(s.allocator.getSegmentSize() - 1));
    EXPECT_EQ(nullPtr, s.getAddressAt(s.allocator.getSegmentSize()));
    EXPECT_EQ(nullPtr, s.getAddressAt(s.allocator.getSegmentSize() + 1));
    EXPECT_EQ(s.seglets[0], s.getAddressAt(0));
}

TEST_P(SegmentTest, getContiguousBytesAt) {
    Segment s(*GetParam());
    EXPECT_EQ(1U, s.getContiguousBytesAt(s.allocator.getSegmentSize() - 1));
    EXPECT_EQ(0U, s.getContiguousBytesAt(s.allocator.getSegmentSize()));
    EXPECT_EQ(0U, s.getContiguousBytesAt(s.allocator.getSegmentSize() + 1));
    EXPECT_EQ(s.allocator.getSegletSize(), s.getContiguousBytesAt(0));
    EXPECT_EQ(s.allocator.getSegletSize() - 1, s.getContiguousBytesAt(1));
}

TEST_P(SegmentTest, offsetToSeglet) {
    Segment s(*GetParam());
    EXPECT_EQ(s.seglets[0], s.offsetToSeglet(0));
    EXPECT_EQ(s.seglets[0], s.offsetToSeglet(1));
    EXPECT_EQ(s.seglets[0], s.offsetToSeglet(s.allocator.getSegletSize() - 1));
    if (s.seglets.size() > 1)
        EXPECT_EQ(s.seglets[1], s.offsetToSeglet(s.allocator.getSegletSize()));
    void* nullPtr = NULL;
    EXPECT_EQ(nullPtr, s.offsetToSeglet(downCast<uint32_t>(s.seglets.size()) *
                       s.allocator.getSegletSize()));
}

TEST_P(SegmentTest, bytesLeft) {
    Segment s(*GetParam());
    EXPECT_EQ(s.allocator.getSegmentSize(), s.bytesLeft());
    s.append(LOG_ENTRY_TYPE_OBJ, "blah", 5);
    EXPECT_EQ(s.allocator.getSegmentSize() - 7, s.bytesLeft());
    s.close();
    EXPECT_EQ(0U, s.bytesLeft());
}

TEST_P(SegmentTest, bytesNeeded) {
    Segment s(*GetParam());
    EXPECT_EQ(2U, s.bytesNeeded(0));
    EXPECT_EQ(257U, s.bytesNeeded(255));
    EXPECT_EQ(259U, s.bytesNeeded(256));
}

TEST_P(SegmentTest, copyOut) {
    Segment s(*GetParam());
    uint32_t segmentSize = s.allocator.getSegmentSize();

    char buf[1024];
    EXPECT_EQ(0U, s.copyOut(segmentSize, buf, sizeof(buf)));
    EXPECT_EQ(5U, s.copyOut(segmentSize - 5, buf, sizeof(buf)));
    EXPECT_EQ(sizeof32(buf),
              s.copyOut(segmentSize - sizeof32(buf), buf, sizeof(buf)));

    char src[100];
    s.copyIn(5, src, sizeof(src));
    s.copyOut(5, buf, sizeof(src));
    EXPECT_EQ(0, memcmp(src, buf, sizeof(src)));
}

TEST_P(SegmentTest, copyIn) {
    Segment s(*GetParam());
    uint32_t segmentSize = s.allocator.getSegmentSize();

    char buf[1024];
    EXPECT_EQ(0U, s.copyIn(segmentSize, buf, sizeof(buf)));
    EXPECT_EQ(5U, s.copyIn(segmentSize - 5, buf, sizeof(buf)));
    EXPECT_EQ(sizeof32(buf),
              s.copyIn(segmentSize - sizeof32(buf), buf, sizeof(buf)));

    // SegmentTest_copyOut tests that correct data is copied in and out.
}

TEST_P(SegmentTest, copyInFromBuffer) {
    Segment s(*GetParam());
    uint32_t segmentSize = s.allocator.getSegmentSize();

    char buf[1024];
    Buffer buffer;
    buffer.appendTo(buf, sizeof(buf));

    EXPECT_EQ(0U, s.copyInFromBuffer(segmentSize, buffer, 0, sizeof(buf))); 
    EXPECT_EQ(5U, s.copyInFromBuffer(segmentSize - 5, buffer, 0, sizeof(buf)));
    EXPECT_EQ(sizeof32(buf),
       s.copyInFromBuffer(segmentSize - sizeof32(buf), buffer, 0, sizeof(buf)));

    char buf2[1024];

    s.copyInFromBuffer(6, buffer, 0, sizeof(buf));
    s.copyOut(6, buf2, sizeof(buf));
    EXPECT_EQ(0, memcmp(buf, buf2, sizeof(buf)));

    EXPECT_EQ(83U, s.copyInFromBuffer(12, buffer, 0, 83));
    s.copyOut(12, buf2, 83);
    EXPECT_EQ(0, memcmp(&buf[0], buf2, 83));

    EXPECT_EQ(28U, s.copyInFromBuffer(19, buffer, 2, 28));
    s.copyOut(19, buf2, 28);
    EXPECT_EQ(0, memcmp(&buf[2], buf2, 28));
}

TEST_P(SegmentTest, checkMetadataIntegrity_simple) {
    TestLog::Enable _;
    Segment s(*GetParam());
    EXPECT_TRUE(s.checkMetadataIntegrity());
    s.append(LOG_ENTRY_TYPE_OBJ, "asdfhasdf", 10);
    EXPECT_TRUE(s.checkMetadataIntegrity());

    // scribbling on an entry's data won't harm anything
    s.copyIn(2, "ASDFHASDF", 10); 
    EXPECT_TRUE(s.checkMetadataIntegrity());

    // scribbling on metadata should result in a checksum error
    Segment::EntryHeader newHeader(LOG_ENTRY_TYPE_OBJTOMB, 10);
    s.copyIn(0, &newHeader, sizeof(newHeader));
    EXPECT_FALSE(s.checkMetadataIntegrity());
    EXPECT_TRUE(StringUtil::startsWith(TestLog::get(),
        "checkMetadataIntegrity: segment corrupt: bad checksum"));
}

TEST_P(SegmentTest, checkMetadataIntegrity_noFooter) {
    TestLog::Enable _;
    Segment s(*GetParam());
    uint32_t segmentSize = s.allocator.getSegmentSize();
    char buf[segmentSize];
    memset(buf, 0, segmentSize);
    s.copyIn(0, buf, segmentSize);
    EXPECT_FALSE(s.checkMetadataIntegrity());
    EXPECT_TRUE(StringUtil::startsWith(TestLog::get(),
        "checkMetadataIntegrity: segment corrupt: no footer by offset "));
}

TEST_P(SegmentTest, checkMetadataIntegrity_badLength) {
    TestLog::Enable _;
    Segment s(*GetParam());
    uint32_t segmentSize = s.allocator.getSegmentSize();
    Segment::EntryHeader header(LOG_ENTRY_TYPE_OBJ, 1024*1024*1024);
    s.copyIn(0, &header, sizeof(header));
    s.copyIn(sizeof(header), &segmentSize, sizeof(segmentSize));
    EXPECT_FALSE(s.checkMetadataIntegrity());
    EXPECT_TRUE(StringUtil::startsWith(TestLog::get(),
        "checkMetadataIntegrity: segment corrupt: no footer by offset "));
}

} // namespace RAMCloud
