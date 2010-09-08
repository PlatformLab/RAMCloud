/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for
 * any purpose with or without fee is hereby granted, provided that
 * the above copyright notice and this permission notice appear in all
 * copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL
 * WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL
 * AUTHORS BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR
 * CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS
 * OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
 * NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/**
 * \file
 * Unit tests for RAMCloud::Metrics and RAMCloud::BlockMetric.
 */

#include <cppunit/extensions/HelperMacros.h>

#include "Metrics.h"

namespace RAMCloud {

/**
 * Unit tests for Metrics.
 */
class MetricsTest : public CppUnit::TestFixture {
    DISALLOW_COPY_AND_ASSIGN(MetricsTest); // NOLINT

    CPPUNIT_TEST_SUITE(MetricsTest);
    CPPUNIT_TEST(test_read_noMarks);
    CPPUNIT_TEST(test_read_beginMarkOnly);
    CPPUNIT_TEST(test_read_normal);
    CPPUNIT_TEST(test_setup_normal);
    CPPUNIT_TEST(test_setup_fromRPC);
    CPPUNIT_TEST(test_mark_inc);
    CPPUNIT_TEST(test_mark_endBeforeBegin);
    CPPUNIT_TEST(test_mark_normal);
    CPPUNIT_TEST_SUITE_END();

  public:
    MetricsTest() {}

    void
    test_read_noMarks()
    {
        Metrics::setup(PERF_COUNTER_TSC,
                       MARK_RPC_PROCESSING_BEGIN, MARK_RPC_PROCESSING_END);
        CPPUNIT_ASSERT_EQUAL(0lu, Metrics::read());
    }

    void
    test_read_beginMarkOnly()
    {
        Metrics::setup(PERF_COUNTER_TSC,
                       MARK_RPC_PROCESSING_BEGIN, MARK_RPC_PROCESSING_END);
        Metrics::mark(MARK_RPC_PROCESSING_BEGIN);
        CPPUNIT_ASSERT_EQUAL(0lu, Metrics::read());
    }

    void
    test_read_normal()
    {
        Metrics::setup(PERF_COUNTER_TEST,
                       MARK_RPC_PROCESSING_BEGIN, MARK_RPC_PROCESSING_END);
        Metrics::MockCounter::set(100);
        Metrics::mark(MARK_RPC_PROCESSING_BEGIN);
        Metrics::MockCounter::set(1000);
        Metrics::mark(MARK_RPC_PROCESSING_END);
        CPPUNIT_ASSERT(Metrics::read() == 900);
    }

    void
    test_setup_normal()
    {
        Metrics::setup(PERF_COUNTER_PMC,
                       MARK_RPC_PROCESSING_BEGIN, MARK_RPC_PROCESSING_END);
        CPPUNIT_ASSERT_EQUAL(0lu, Metrics::beginCount);
        CPPUNIT_ASSERT_EQUAL(0lu, Metrics::count);
        CPPUNIT_ASSERT_EQUAL(PERF_COUNTER_PMC, Metrics::counterType);
        CPPUNIT_ASSERT_EQUAL(MARK_RPC_PROCESSING_BEGIN, Metrics::activeMark);
        CPPUNIT_ASSERT_EQUAL(MARK_RPC_PROCESSING_BEGIN, Metrics::beginMark);
        CPPUNIT_ASSERT_EQUAL(MARK_RPC_PROCESSING_END, Metrics::endMark);
        CPPUNIT_ASSERT_EQUAL(Metrics::NO_DATA, Metrics::state);
    }

    void
    test_setup_fromRPC()
    {
        static_assert(sizeof(RpcPerfCounter) == sizeof(uint32_t));
        RpcPerfCounter perfCounter;
        perfCounter.beginMark = MARK_RPC_PROCESSING_BEGIN;
        perfCounter.endMark = MARK_RPC_PROCESSING_END;
        perfCounter.counterType = PERF_COUNTER_TSC;
        Metrics::setup(perfCounter);
        CPPUNIT_ASSERT_EQUAL(PERF_COUNTER_TSC, Metrics::counterType);
        CPPUNIT_ASSERT_EQUAL(MARK_RPC_PROCESSING_BEGIN, Metrics::beginMark);
        CPPUNIT_ASSERT_EQUAL(MARK_RPC_PROCESSING_END, Metrics::endMark);
    }

    void
    test_mark_inc()
    {
        Metrics::setup(PERF_COUNTER_INC,
                       MARK_RPC_PROCESSING_BEGIN, MARK_RPC_PROCESSING_END);
        CPPUNIT_ASSERT_EQUAL(0lu, Metrics::read());
        Metrics::mark(MARK_RPC_PROCESSING_BEGIN);
        CPPUNIT_ASSERT_EQUAL(1lu, Metrics::read());
    }

    void
    test_mark_endBeforeBegin()
    {
        Metrics::setup(PERF_COUNTER_TSC,
                       MARK_RPC_PROCESSING_BEGIN, MARK_RPC_PROCESSING_END);
        Metrics::mark(MARK_RPC_PROCESSING_END);
        Metrics::mark(MARK_RPC_PROCESSING_BEGIN);
        CPPUNIT_ASSERT_EQUAL(0lu, Metrics::read());
    }

    void
    test_mark_normal()
    {
        Metrics::setup(PERF_COUNTER_TSC,
                       MARK_RPC_PROCESSING_BEGIN, MARK_RPC_PROCESSING_END);
        Metrics::mark(MARK_RPC_PROCESSING_BEGIN);
        CPPUNIT_ASSERT_EQUAL(Metrics::STARTED, Metrics::state);
        CPPUNIT_ASSERT_EQUAL(MARK_RPC_PROCESSING_END, Metrics::activeMark);
        CPPUNIT_ASSERT_EQUAL(0lu, Metrics::read());
        Metrics::mark(MARK_RPC_PROCESSING_END);
        CPPUNIT_ASSERT_EQUAL(Metrics::VALID, Metrics::state);
        CPPUNIT_ASSERT(Metrics::read() > 0);
    }

};
CPPUNIT_TEST_SUITE_REGISTRATION(MetricsTest);

/**
 * Unit tests for BlockMetric.
 */
class BlockMetricTest : public CppUnit::TestFixture {
    DISALLOW_COPY_AND_ASSIGN(BlockMetricTest); // NOLINT

    CPPUNIT_TEST_SUITE(BlockMetricTest);
    CPPUNIT_TEST(test_normal);
    CPPUNIT_TEST_SUITE_END();

  public:
    BlockMetricTest() {}

    void
    test_normal()
    {
        Metrics::setup(PERF_COUNTER_TSC,
                       MARK_RPC_PROCESSING_BEGIN, MARK_RPC_PROCESSING_END);
        {
            // Create an instance on the stack and let it get
            // destructed at the end of this block
            BlockMetric nameDoesNotMatter(MARK_RPC_PROCESSING_BEGIN,
                                          MARK_RPC_PROCESSING_END);
        }
        CPPUNIT_ASSERT_EQUAL(Metrics::VALID, Metrics::state);
        CPPUNIT_ASSERT(Metrics::read() > 0);
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(BlockMetricTest);

}
