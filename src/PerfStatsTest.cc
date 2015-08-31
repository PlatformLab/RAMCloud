/* Copyright (c) 2014-2015 Stanford University
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

#include <stdarg.h>

#include "TestUtil.h"
#include "PerfStats.h"
#include "ServerId.h"
#include "WireFormat.h"

namespace RAMCloud {

class PerfStatsTest : public ::testing::Test {
  public:
    PerfStats stats;

    PerfStatsTest()
        : stats()
    {
        PerfStats::registeredStats.clear();
    }

    ~PerfStatsTest()
    {
    }

    // Fills a PerfStats with sample data.
    void
    fill(PerfStats* stats, uint32_t value)
    {
        stats->collectionTime = 10*value;
        stats->cyclesPerSecond = 1e6;
        stats->readCount = value;
        stats->writeCount = 2*value;
        stats->readObjectBytes = 3*value;
        stats->writeObjectBytes = 4*value;
        stats->temp5 = 5*value;
    }

    // Fills a PerfStats::Diff with sample data.
    void
    fillDiff(PerfStats::Diff* diff)
    {
        PerfStats stats1, stats2, stats3;
        fill(&stats1, 1000);
        fill(&stats2, 2000);
        fill(&stats3, 4000);

        Buffer before, after;
        combineStats(&before, &stats1, ServerId(1, 0).getId(),
                &stats1, ServerId(2, 0).getId(),
                &stats2, ServerId(3, 0).getId(), NULL);
        combineStats(&after, &stats2, ServerId(1, 0).getId(),
                &stats3, ServerId(2, 0).getId(),
                &stats3, ServerId(3, 0).getId(), NULL);
        PerfStats::clusterDiff(&before, &after, diff);
    }

    /**
     * Given a collection of PerfStats structures, combine them
     * into a Buffer with the format of serverControlAll(GET_PERF_STATS).
     * Arguments after buffer come in pairs: a PerfStats* first, then a
     * ServerId.getId(). The argument list must be terminated by a NULL
     * PerfStats*.
     */
    void
    combineStats(Buffer* buffer, ...)
    {
        buffer->reset();
        WireFormat::ServerControlAll::Response* header =
                buffer->emplaceAppend<WireFormat::ServerControlAll::Response>();
        header->common.status = STATUS_OK;
        header->serverCount = 0;
        header->respCount = 0;

        va_list args;
        va_start(args, buffer);
        while (1) {
            PerfStats* stats = va_arg(args, PerfStats*);
            if (stats == NULL) {
                break;
            }
            uint64_t id = va_arg(args, uint64_t);
            header->serverCount++;
            header->respCount++;
            WireFormat::ServerControl::Response* subHead = buffer->
                    emplaceAppend<WireFormat::ServerControl::Response>();
            subHead->common.status = STATUS_OK;
            subHead->serverId = id;
            subHead->outputLength = sizeof32(*stats);
            buffer->appendCopy<PerfStats>(stats);
        }
        header->totalRespLength = buffer->size() - sizeof32(*header);
        va_end(args);
    }

    DISALLOW_COPY_AND_ASSIGN(PerfStatsTest);
};

// Helper function for the following test.
static void testThreadLocalStats() {
    PerfStats::threadStats.readCount = 10;
}

TEST_F(PerfStatsTest, threadStatsVariable) {
    // This test ensures that different threads have different PerfStats.
    PerfStats::threadStats.readCount = 99;
    std::thread thread(testThreadLocalStats);
    thread.join();
    EXPECT_EQ(99lu, PerfStats::threadStats.readCount);
}

TEST_F(PerfStatsTest, registerStats_alreadyRegistered) {
    PerfStats::registerStats(&stats);
    EXPECT_EQ(1u, PerfStats::registeredStats.size());
    PerfStats stats2;
    PerfStats::registerStats(&stats2);
    EXPECT_EQ(2u, PerfStats::registeredStats.size());
    PerfStats::registerStats(&stats);
    EXPECT_EQ(2u, PerfStats::registeredStats.size());
    PerfStats::registerStats(&stats2);
    EXPECT_EQ(2u, PerfStats::registeredStats.size());
}

TEST_F(PerfStatsTest, registerStats_initialize) {
    PerfStats::nextThreadId = 44;
    EXPECT_EQ(0u, PerfStats::registeredStats.size());
    stats.collectionTime = 99;
    stats.temp5 = 100;
    PerfStats::registerStats(&stats);
    EXPECT_EQ(1u, PerfStats::registeredStats.size());
    EXPECT_EQ(44, stats.threadId);
    EXPECT_EQ(0u, stats.collectionTime);
    EXPECT_EQ(0u, stats.temp5);
    EXPECT_EQ(45, PerfStats::nextThreadId);
}

TEST_F(PerfStatsTest, collectStats) {
    PerfStats::registerStats(&stats);
    stats.readCount = 10;
    stats.writeCount = 20;
    PerfStats stats2;
    PerfStats::registerStats(&stats2);
    stats2.readCount = 100;
    stats2.writeCount = 200;
    PerfStats total;
    PerfStats::collectStats(&total);
    EXPECT_EQ(110u, total.readCount);
    EXPECT_EQ(220u, total.writeCount);
}

TEST_F(PerfStatsTest, clusterDiff_findMatchingData) {
    // Test code that skips entries where either before or after
    // data is missing.
    PerfStats stats1, stats2;
    fill(&stats1, 1000);
    fill(&stats2, 2000);

    Buffer before, after;
    combineStats(&before, &stats1, ServerId(1, 0).getId(),
            &stats1, ServerId(2, 0).getId(),
            &stats1, ServerId(3, 0).getId(),
            &stats1, ServerId(5, 0).getId(), NULL);
    combineStats(&after, &stats2, ServerId(1, 0).getId(),
            &stats2, ServerId(3, 0).getId(),
            &stats2, ServerId(4, 0).getId(), NULL);
    PerfStats::Diff diff;
    PerfStats::clusterDiff(&before, &after, &diff);
    ASSERT_EQ(2u, diff["serverId"].size());
    EXPECT_EQ(1.0, diff["serverId"][0]);
    EXPECT_EQ(3.0, diff["serverId"][1]);
}
TEST_F(PerfStatsTest, clusterDiff_collectMetrics) {
    PerfStats::Diff diff;
    fillDiff(&diff);
    ASSERT_EQ(3u, diff["cyclesPerSecond"].size());
    EXPECT_EQ(1e6, diff["cyclesPerSecond"][0]);
    EXPECT_EQ(1000.0, diff["readCount"][0]);
    EXPECT_EQ(3000.0, diff["readCount"][1]);
    EXPECT_EQ(2000.0, diff["readCount"][2]);
    EXPECT_EQ(5000.0, diff["temp5"][0]);
    EXPECT_EQ(15000.0, diff["temp5"][1]);
    EXPECT_EQ(10000.0, diff["temp5"][2]);
}

TEST_F(PerfStatsTest, parseStats_basics) {
    PerfStats stats1, stats2, stats3;
    fill(&stats1, 1000);
    fill(&stats2, 2000);
    fill(&stats3, 3000);

    Buffer buffer;
    combineStats(&buffer, &stats1, ServerId(3, 2).getId(),
            &stats2, ServerId(2, 1).getId(),
            &stats3, ServerId(5, 0).getId(), NULL);
    std::vector<PerfStats> parsed;
    PerfStats::parseStats(&buffer, &parsed);
    ASSERT_EQ(6u, parsed.size());
    EXPECT_EQ(0u, parsed[0].collectionTime);
    EXPECT_EQ(0u, parsed[1].collectionTime);
    EXPECT_EQ(20000u, parsed[2].collectionTime);
    EXPECT_EQ(10000u, parsed[3].collectionTime);
    EXPECT_EQ(0u, parsed[4].collectionTime);
    EXPECT_EQ(30000u, parsed[5].collectionTime);
    EXPECT_EQ(2000u, parsed[2].readCount);
    EXPECT_EQ(15000u, parsed[5].temp5);
}
TEST_F(PerfStatsTest, parseStats_shortBuffer) {
    PerfStats stats;
    fill(&stats, 1000);

    Buffer buffer;
    combineStats(&buffer, &stats, ServerId(3, 2).getId(), NULL);

    // First, try with not quite enough space for the PerfStats.
    buffer.truncate(buffer.size() - 1);
    std::vector<PerfStats> parsed;
    PerfStats::parseStats(&buffer, &parsed);
    ASSERT_EQ(0u, parsed.size());

    // Now, try without even enough space for the header.
    buffer.truncate(5);
    PerfStats::parseStats(&buffer, &parsed);
    ASSERT_EQ(0u, parsed.size());
}

TEST_F(PerfStatsTest, clusterDiff_formatMetric) {
    PerfStats::Diff diff;
    fillDiff(&diff);
    EXPECT_EQ("no metric xyzzy",
            PerfStats::formatMetric(&diff, "xyzzy", " %.3f"));
    EXPECT_EQ(" 1000.000 3000.000 2000.000",
            PerfStats::formatMetric(&diff, "readCount", " %.3f"));
    EXPECT_EQ("        1        3        2",
            PerfStats::formatMetric(&diff, "readCount", " %8.0f", 1e-3));
}

TEST_F(PerfStatsTest, clusterDiff_formatMetricRate) {
    PerfStats::Diff diff;
    fillDiff(&diff);
    EXPECT_EQ(" 100000.000 100000.000 100000.000",
            PerfStats::formatMetricRate(&diff, "readCount", " %.3f"));
    EXPECT_EQ("no metric xyzzy",
            PerfStats::formatMetricRate(&diff, "xyzzy", " %.3f"));
    diff.erase("cyclesPerSecond");
    EXPECT_EQ("no metric cyclesPerSecond",
            PerfStats::formatMetricRate(&diff, "readCount", " %.3f"));
    diff.erase("collectionTime");
    EXPECT_EQ("no metric collectionTime",
            PerfStats::formatMetricRate(&diff, "readCount", " %.3f"));
}

TEST_F(PerfStatsTest, clusterDiff_formatMetricRatio) {
    PerfStats::Diff diff;
    fillDiff(&diff);
    EXPECT_EQ("no metric xyzzy",
            PerfStats::formatMetricRatio(&diff, "xyzzy",
            "cyclesPerSecond", " %.3f"));
    EXPECT_EQ("no metric abcde",
            PerfStats::formatMetricRatio(&diff, "collectionTime",
            "abcde", " %.3f"));
    EXPECT_EQ(" 0.010 0.030 0.020",
            PerfStats::formatMetricRatio(&diff, "collectionTime",
            "cyclesPerSecond", " %.3f"));
    EXPECT_EQ(" 0.020 0.060 0.040",
            PerfStats::formatMetricRatio(&diff, "collectionTime",
            "cyclesPerSecond", " %.3f", 2.0));
}

}  // namespace RAMCloud
