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

struct SegmentAndSegletSize {
    uint32_t segmentSize;
    uint32_t segletSize;
    uint32_t getSegletsPerSegment() { return segmentSize / segletSize; }
};

class SegmentAndAllocator {
  public:
    SegmentAndAllocator(SegmentAndSegletSize* segmentAndSegletSize)
        : segmentSize(segmentAndSegletSize->segmentSize),
          segletSize(segmentAndSegletSize->segletSize),
          allocator(segmentSize, segletSize),
          segment()
    {
        vector<Seglet*> seglets;
        EXPECT_TRUE(allocator.alloc(SegletAllocator::DEFAULT,
                    segmentSize / segletSize,
                    seglets));
        segment.construct(seglets, segletSize);
    }

    const uint32_t segmentSize;
    const uint32_t segletSize;
    SegletAllocator allocator;
    Tub<Segment> segment;

    DISALLOW_COPY_AND_ASSIGN(SegmentAndAllocator);
};

/**
 * Unit tests for Segment.
 */
class SegmentTest : public ::testing::TestWithParam<SegmentAndSegletSize*> {
  public:
    SegmentTest()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(SegmentTest);
};

// Run tests with various different seglet sizes to stress the code with
// different fragmentation in the backing segment memory.

SegmentAndSegletSize boringDefault = {
    Segment::DEFAULT_SEGMENT_SIZE,
    Seglet::DEFAULT_SEGLET_SIZE
};

SegmentAndSegletSize horriblyFragmented = {
    98304,
    16
};

INSTANTIATE_TEST_CASE_P(SegmentTestAllocations,
                        SegmentTest,
                        ::testing::Values(&boringDefault,
                                          &horriblyFragmented));

TEST_P(SegmentTest, constructor) {
    SegmentAndAllocator segAndAlloc(GetParam());
    Segment& s = *segAndAlloc.segment;

    EXPECT_FALSE(s.closed);
    EXPECT_EQ(0U, s.head);

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
    EXPECT_FALSE(s.mustFreeBlocks);
}

TEST_F(SegmentTest, constructor_priorSegmentBuffer) {
    Segment previous;
    previous.append(LOG_ENTRY_TYPE_OBJ, "hi", 3);
    Buffer buffer;
    previous.appendToBuffer(buffer);

    const void* p = buffer.getRange(0, buffer.getTotalLength());
    Segment s(p, buffer.getTotalLength());

    EXPECT_EQ(0U, s.seglets.size());
    EXPECT_EQ(1U, s.segletBlocks.size());
    EXPECT_TRUE(s.closed);
    EXPECT_EQ(s.head, buffer.getTotalLength());
    EXPECT_EQ(p, s.segletBlocks[0]);
    EXPECT_FALSE(s.mustFreeBlocks);
}

TEST_P(SegmentTest, append_blackBox) {
    SegmentAndAllocator segAndAlloc(GetParam());
    Segment& s = *segAndAlloc.segment;

    char buf[1000];
    for (uint32_t i = 0; i < 1000; i += 100) {
        uint32_t offset;
        EXPECT_TRUE(s.append(LOG_ENTRY_TYPE_OBJ, buf, i, offset));

        Buffer buffer;
        s.getEntry(offset, buffer);
        EXPECT_EQ(i, buffer.getTotalLength());
        EXPECT_EQ(0, memcmp(buf, buffer.getRange(0, i), i));
    }
}

TEST_P(SegmentTest, append_outOfSpace) {
    SegmentAndAllocator segAndAlloc(GetParam());
    Segment& s = *segAndAlloc.segment;
    Segment::OpaqueFooterEntry unused;

    // How many N-length writes can we make to this segment?
    char buf[107];
    uint32_t bytesPerAppend = s.bytesNeeded(sizeof(buf));
    uint32_t expectedAppends =
        (GetParam()->segmentSize - s.bytesNeeded(sizeof32(Segment::Footer)))
            / bytesPerAppend;

    uint32_t actualAppends = 0;
    while (s.append(LOG_ENTRY_TYPE_OBJ, buf, sizeof(buf)))
        actualAppends++;

    EXPECT_EQ(expectedAppends, actualAppends);
    EXPECT_EQ(GetParam()->getSegletsPerSegment(), s.getSegletsAllocated());
    EXPECT_GE(GetParam()->segmentSize - s.getAppendedLength(unused),
        s.bytesNeeded(sizeof(Segment::Footer)));
}

TEST_P(SegmentTest, append_whiteBox) {
    SegmentAndAllocator segAndAlloc(GetParam());
    Segment& s = *segAndAlloc.segment;
    Segment::OpaqueFooterEntry unused;

    uint32_t offset;
    s.append(LOG_ENTRY_TYPE_OBJ, "hi", 2, offset);

    EXPECT_EQ(0U, offset);
    EXPECT_EQ(4U, s.getAppendedLength(unused));

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
    Segment::OpaqueFooterEntry unused;

    for (uint32_t i = 0; i < unsafeArrayLength(tests); i++) {
        for (uint32_t j = 0; j < tests[i].bytesToAppendLength; j++) {
            uint32_t length = tests[i].bytesToAppend[j];

            char buf[length];
            SegmentAndAllocator segAndAlloc(GetParam());
            Segment& s = *segAndAlloc.segment;
            s.append(LOG_ENTRY_TYPE_OBJ, buf, length);
            EXPECT_EQ(sizeof(Segment::EntryHeader) +
                        tests[i].expectedLengthBytes + length,
                      s.getAppendedLength(unused));

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
    SegmentAndAllocator segAndAlloc(GetParam());
    Segment& s = *segAndAlloc.segment;
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
    SegmentAndAllocator segAndAlloc(GetParam());
    Segment& s = *segAndAlloc.segment;
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
    SegmentAndAllocator segAndAlloc(GetParam());
    Segment& s = *segAndAlloc.segment;
    Buffer buffer;
    s.appendToBuffer(buffer);
    EXPECT_EQ(7U, buffer.getTotalLength());

    buffer.reset();
    s.append(LOG_ENTRY_TYPE_OBJ, "yo!", 3);
    s.appendToBuffer(buffer);
    EXPECT_EQ(7U + 5U, buffer.getTotalLength());
}

TEST_P(SegmentTest, getEntry) {
    SegmentAndAllocator segAndAlloc(GetParam());
    Segment& s = *segAndAlloc.segment;
    uint32_t offset;
    s.append(LOG_ENTRY_TYPE_OBJ, "this is only a test!", 21, offset);

    Buffer buffer;
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, s.getEntry(offset, buffer));
    EXPECT_EQ(21U, buffer.getTotalLength());
    EXPECT_STREQ("this is only a test!",
        reinterpret_cast<const char*>(buffer.getRange(0, 21)));
}

TEST_P(SegmentTest, getAppendedLength) {
    SegmentAndAllocator segAndAlloc(GetParam());
    Segment& s = *segAndAlloc.segment;
    // TODO(steve): write me.
    (void)s;
}

TEST_P(SegmentTest, getSegletsAllocated) {
    SegmentAndAllocator segAndAlloc(GetParam());
    Segment& s = *segAndAlloc.segment;
    EXPECT_EQ(GetParam()->getSegletsPerSegment(), s.getSegletsAllocated());
}

TEST_P(SegmentTest, appendFooter) {
    SegmentAndAllocator segAndAlloc(GetParam());
    Segment& s = *segAndAlloc.segment;
    Segment::OpaqueFooterEntry unused;

    // Appending the footer shouldn't alter the head or the checksum
    // we've accumulated thus far.
    s.append(LOG_ENTRY_TYPE_OBJ, "blah", 4);
    uint32_t head = s.getAppendedLength(unused);
    Crc32C checksum = s.checksum;
    s.appendFooter();
    EXPECT_EQ(head, s.getAppendedLength(unused));
    EXPECT_EQ(checksum.getResult(), s.checksum.getResult());
}

TEST_P(SegmentTest, getEntryInfo) {
    LogEntryType type;
    uint32_t dataOffset;
    uint32_t dataLength;

    {
        SegmentAndAllocator segAndAlloc(GetParam());
        Segment& s = *segAndAlloc.segment;
        s.getEntryInfo(0, type, dataOffset, dataLength);
        EXPECT_EQ(2U, dataOffset);
        char buf[200];
        s.append(LOG_ENTRY_TYPE_OBJ, buf, 200);

        s.getEntryInfo(0, type, dataOffset, dataLength);
        EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, type);
        EXPECT_EQ(2U, dataOffset);
        EXPECT_EQ(200U, dataLength);
    }

    {
        SegmentAndAllocator segAndAlloc(GetParam());
        Segment& s = *segAndAlloc.segment;
        s.getEntryInfo(0, type, dataOffset, dataLength);
        EXPECT_EQ(2U, dataOffset);
        char buf[2000];
        s.append(LOG_ENTRY_TYPE_OBJ, buf, 2000);
        s.getEntryInfo(0, type, dataOffset, dataLength);
        EXPECT_EQ(3U, dataOffset);
        EXPECT_EQ(2000U, dataLength);
    }

    {
        SegmentAndAllocator segAndAlloc(GetParam());
        Segment& s = *segAndAlloc.segment;
        s.append(LOG_ENTRY_TYPE_OBJ, NULL, 0);  // EntryHeader at 0
        s.append(LOG_ENTRY_TYPE_OBJ, NULL, 0);  // EntryHeader at 2
        s.append(LOG_ENTRY_TYPE_OBJ, "hi", 2);  // EntryHeader at 4
        s.append(LOG_ENTRY_TYPE_OBJ, NULL, 0);  // EntryHeader at 8

        s.getEntryInfo(0, type, dataOffset, dataLength);
        EXPECT_EQ(2U, dataOffset);

        s.getEntryInfo(2, type, dataOffset, dataLength);
        EXPECT_EQ(4U, dataOffset);

        s.getEntryInfo(4, type, dataOffset, dataLength);
        EXPECT_EQ(6U, dataOffset);

        s.getEntryInfo(8, type, dataOffset, dataLength);
        EXPECT_EQ(10U, dataOffset);

        s.getEntryInfo(0, type, dataOffset, dataLength);
        EXPECT_EQ(0U, dataLength);

        s.getEntryInfo(2, type, dataOffset, dataLength);
        EXPECT_EQ(0U, dataLength);

        s.getEntryInfo(4, type, dataOffset, dataLength);
        EXPECT_EQ(2U, dataLength);

        s.getEntryInfo(8, type, dataOffset, dataLength);
        EXPECT_EQ(0U, dataLength);
    }
}

TEST_P(SegmentTest, peek) {
    SegmentAndAllocator segAndAlloc(GetParam());
    Segment& s = *segAndAlloc.segment;
    const void* pointer = NULL;
    void* const nullPtr = NULL;

    EXPECT_EQ(1U, s.peek(GetParam()->segmentSize - 1, &pointer));
    EXPECT_EQ(0U, s.peek(GetParam()->segmentSize, &pointer));
    EXPECT_EQ(0U, s.peek(GetParam()->segmentSize + 1, &pointer));
    EXPECT_EQ(GetParam()->segletSize, s.peek(0, &pointer));
    EXPECT_EQ(GetParam()->segletSize - 1, s.peek(1, &pointer));

    pointer = NULL;
    EXPECT_NE(0U, s.peek(GetParam()->segmentSize - 1, &pointer));
    EXPECT_NE(nullPtr, pointer);

    pointer = NULL;
    EXPECT_EQ(0U, s.peek(GetParam()->segmentSize, &pointer));
    EXPECT_EQ(nullPtr, pointer);

    pointer = NULL;
    EXPECT_EQ(0U, s.peek(GetParam()->segmentSize + 1, &pointer));
    EXPECT_EQ(nullPtr, pointer);

    pointer = NULL;
    EXPECT_EQ(GetParam()->segletSize, s.peek(0, &pointer));
    EXPECT_EQ(s.segletBlocks[0], pointer);
}

TEST_P(SegmentTest, bytesLeft) {
    SegmentAndAllocator segAndAlloc(GetParam());
    Segment& s = *segAndAlloc.segment;
    EXPECT_EQ(GetParam()->segmentSize, s.bytesLeft() + 7);
    s.append(LOG_ENTRY_TYPE_OBJ, "blah", 5);
    EXPECT_EQ(GetParam()->segmentSize - 14, s.bytesLeft());
    s.close();
    EXPECT_EQ(0U, s.bytesLeft());
}

TEST_P(SegmentTest, bytesNeeded) {
    SegmentAndAllocator segAndAlloc(GetParam());
    Segment& s = *segAndAlloc.segment;
    EXPECT_EQ(2U, s.bytesNeeded(0));
    EXPECT_EQ(257U, s.bytesNeeded(255));
    EXPECT_EQ(259U, s.bytesNeeded(256));
}

TEST_P(SegmentTest, copyOut) {
    SegmentAndAllocator segAndAlloc(GetParam());
    Segment& s = *segAndAlloc.segment;
    uint32_t segmentSize = GetParam()->segmentSize;

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
    SegmentAndAllocator segAndAlloc(GetParam());
    Segment& s = *segAndAlloc.segment;
    uint32_t segmentSize = GetParam()->segmentSize;

    char buf[1024];
    EXPECT_EQ(0U, s.copyIn(segmentSize, buf, sizeof(buf)));
    EXPECT_EQ(5U, s.copyIn(segmentSize - 5, buf, sizeof(buf)));
    EXPECT_EQ(sizeof32(buf),
              s.copyIn(segmentSize - sizeof32(buf), buf, sizeof(buf)));

    // SegmentTest_copyOut tests that correct data is copied in and out.
}

TEST_P(SegmentTest, copyInFromBuffer) {
    SegmentAndAllocator segAndAlloc(GetParam());
    Segment& s = *segAndAlloc.segment;
    uint32_t segmentSize = GetParam()->segmentSize;

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
    SegmentAndAllocator segAndAlloc(GetParam());
    Segment& s = *segAndAlloc.segment;
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
    SegmentAndAllocator segAndAlloc(GetParam());
    Segment& s = *segAndAlloc.segment;
    uint32_t segmentSize = GetParam()->segmentSize;
    char buf[segmentSize];
    memset(buf, 0, segmentSize);
    s.copyIn(0, buf, segmentSize);
    EXPECT_FALSE(s.checkMetadataIntegrity());
    EXPECT_TRUE(StringUtil::startsWith(TestLog::get(),
        "checkMetadataIntegrity: segment corrupt: no footer by offset "));
}

TEST_P(SegmentTest, checkMetadataIntegrity_badLength) {
    TestLog::Enable _;
    SegmentAndAllocator segAndAlloc(GetParam());
    Segment& s = *segAndAlloc.segment;
    uint32_t segmentSize = GetParam()->segmentSize;
    Segment::EntryHeader header(LOG_ENTRY_TYPE_OBJ, 1024*1024*1024);
    s.copyIn(0, &header, sizeof(header));
    s.copyIn(sizeof(header), &segmentSize, sizeof(segmentSize));
    EXPECT_FALSE(s.checkMetadataIntegrity());
    EXPECT_TRUE(StringUtil::startsWith(TestLog::get(),
        "checkMetadataIntegrity: segment corrupt: no footer by offset "));
}

} // namespace RAMCloud
