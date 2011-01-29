/* Copyright (c) 2010 Stanford University
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

#include "BenchUtil.h"
#include "ClientException.h"
#include "Logging.h"
#include "MasterServer.h"

namespace RAMCloud {

class SegmentIteratorBenchmark {
  public:
    SegmentIteratorBenchmark() {}

    void
    run(int numSegments, int minObjectBytes, int maxObjectBytes)
    {
        /*
         * Allocate numSegments Segments and fill them up objects of
         * objectBytes bytes. These will be the Segment we recover.
         */
        uint64_t numObjects = 0;
        uint64_t nextObjId = 0;
        Segment *segments[numSegments];
        for (int i = 0; i < numSegments; i++) {
            void *p = xmalloc(Segment::SEGMENT_SIZE);
            segments[i] = new Segment(0, 0, p, Segment::SEGMENT_SIZE, NULL);
            while (1) {
                uint64_t size = minObjectBytes;
                if (minObjectBytes != maxObjectBytes) {
                    uint64_t rnd = generateRandom();
                    size += rnd % (maxObjectBytes - minObjectBytes);
                }
                DECLARE_OBJECT(o, size);
                o->id = nextObjId++;
                o->table = 0;
                o->version = 0;
                const void *so = segments[i]->append(LOG_ENTRY_TYPE_OBJ,
                    o, o->size());
                if (so == NULL)
                    break;
                numObjects++;
            }
            segments[i]->close();
        }

        // scan through the segments
        uint64_t totalBytes = 0;
        uint64_t totalObjects = 0;
        uint64_t b = rdtsc();
        for (int i = 0; i < numSegments; i++) {
            Segment *s = segments[i];
            SegmentIterator si(s);
            while (!si.isDone()) {
                totalBytes += si.getLength();
                totalObjects++;
                si.next();
            }
        }
        uint64_t total = rdtsc() - b;

        printf("Minimum Object %d bytes, Maximum Object %d bytes, "
            "Total Objects %lu bytes\n", minObjectBytes, maxObjectBytes,
            totalBytes);
        printf("Scanned %lu objects in %lu usec (%lu usec/Segment)\n",
            totalObjects, cyclesToNanoseconds(total) / 1000,
            cyclesToNanoseconds(total) / 1000 / numSegments);

        // clean up
        for (int i = 0; i < numSegments; i++) {
            free(const_cast<void *>(segments[i]->getBaseAddress()));
            delete segments[i];
        }
    }

    DISALLOW_COPY_AND_ASSIGN(SegmentIteratorBenchmark);
};

}  // namespace RAMCloud

int
main()
{
    int numSegments = 1300;
    int objectBytes[][2] = {
        { 64, 64 },
        { 128, 128 },
        { 256, 256 },
        { 512, 512 },
        { 1024, 1024 },
        { 2048, 2048 },
        { 8192, 8192 },
        { 64, 128 },
        { 64, 256 },
        { 64, 512 },
        { 64, 1024 },
        { 64, 2048 },
        { 64, 8192 },
        { 0, 0 }
    };

    for (int i = 0; objectBytes[i][0] != 0; i++) {
        printf("==========================\n");
        RAMCloud::SegmentIteratorBenchmark sib;
        sib.run(numSegments, objectBytes[i][0], objectBytes[i][1]);
    }

    return 0;
}
