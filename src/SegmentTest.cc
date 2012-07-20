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
#include "Log.h"
#include "LogEntryTypes.h"

namespace RAMCloud {

/**
 * Unit tests for Segment.
 */
class SegmentTest : public ::testing::TestWithParam<Segment::Allocator*> {
  public:
    class HorriblyFragmentedAllocator : public Segment::Allocator {
        uint32_t getSegletsPerSegment() { return 10000; }
        uint32_t getSegletSize() { return 3; }
        void* alloc() { return Memory::xmalloc(HERE, getSegmentSize()); };
        void free(void* seglet) { std::free(seglet); }
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
}

TEST_P(SegmentTest, append_simpleWhiteBox) {
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

    // How many 0-length writes can we make to this segment?
    uint32_t bytesPerAppend = s.bytesNeeded(0);
    uint32_t expectedAppends =
        (allocator->getSegmentSize() - s.bytesNeeded(sizeof32(Segment::Footer)))
            / bytesPerAppend;

    uint32_t actualAppends = 0;
    while (s.append(LOG_ENTRY_TYPE_OBJ, NULL, 0))
        actualAppends++;

    EXPECT_EQ(expectedAppends, actualAppends);
    EXPECT_EQ(s.getSegletsAllocated(), s.getSegletsNeeded());
    EXPECT_EQ(allocator->getSegletsPerSegment(), s.getSegletsAllocated());
}

} // namespace RAMCloud
