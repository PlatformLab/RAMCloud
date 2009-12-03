/* Copyright (c) 2009 Stanford University
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

#include <string.h>

#include <config.h>

#include <server/index.h>

#include <cppunit/TestAssert.h>
#include <cppunit/extensions/HelperMacros.h>

#define D 0.0001

/* helper for RANGE QUERY ASSERT */
static bool
multimaps_equal(std::multimap<int, double> *expected,
                std::multimap<int, double> *actual)
{
    std::multimap<int, double>::iterator i;
    std::multimap<int, double>::iterator e;
    std::pair<std::multimap<int, double>::iterator,
              std::multimap<int, double>::iterator> ret;

    if (expected->size() != actual->size()) {
        return false;
    }

    for (i = actual->begin(); i != actual->end(); ++i) {
        ret = expected->equal_range(i->first);
        for (e = ret.first; e != ret.second; ++e) {
            if (e->second == i->second) {
                goto next;
            }
        }
        return false;
  next:
        continue;
    }

    return true;
}

/* helper for MULTI LOOKUP ASSERT */
static bool
sets_equal(std::set<double> *expected,
           std::set<double> *actual)
{
    std::set<double>::iterator i;
    std::set<double>::iterator e;

    if (expected->size() != actual->size()) {
        return false;
    }

    for (i = actual->begin(); i != actual->end(); ++i) {
        if (expected->find(*i) == expected->end()) {
            return false;
        }
    }

    return true;
}

class UniqueIndexTest : public CppUnit::TestFixture {
  public:
    virtual void setUp() = 0;
    virtual void tearDown() = 0;
    void TestInsert();
    void TestRemove();
    void TestLookup();
  protected:
    virtual RAMCloud::UniqueIndex<int, double> * getIndex() = 0;
  private:
    CPPUNIT_TEST_SUITE(UniqueIndexTest);
    CPPUNIT_TEST(TestInsert);
    CPPUNIT_TEST(TestRemove);
    CPPUNIT_TEST(TestLookup);
    CPPUNIT_TEST_SUITE_END_ABSTRACT();
};


class UniqueRangeIndexTest : public UniqueIndexTest {
  public:
    void TestRangeQuery();
    void TestRangeQueryNoKeys();
    void TestRangeQueryLimit();
  protected:
    virtual RAMCloud::UniqueRangeIndex<int, double> * getIndex() = 0;
  private:
    CPPUNIT_TEST_SUB_SUITE(UniqueRangeIndexTest, UniqueIndexTest);
    CPPUNIT_TEST(TestRangeQuery);
    CPPUNIT_TEST(TestRangeQueryNoKeys);
    CPPUNIT_TEST(TestRangeQueryLimit);
    CPPUNIT_TEST_SUITE_END_ABSTRACT();
};

class MultiIndexTest : public CppUnit::TestFixture {
  public:
    virtual void setUp() = 0;
    virtual void tearDown() = 0;
    void TestInsert();
    void TestRemove();
    void TestLookup();
    void TestLookupLimit();
  protected:
    virtual RAMCloud::MultiIndex<int, double> * getIndex() = 0;
  private:
    CPPUNIT_TEST_SUITE(MultiIndexTest);
    CPPUNIT_TEST(TestInsert);
    CPPUNIT_TEST(TestRemove);
    CPPUNIT_TEST(TestLookup);
    CPPUNIT_TEST(TestLookupLimit);
    CPPUNIT_TEST_SUITE_END_ABSTRACT();
};

class MultiRangeIndexTest : public MultiIndexTest {
  public:
    void TestRangeQuery();
    void TestRangeQueryNoKeys();
    void TestRangeQueryLimit();
  protected:
    virtual RAMCloud::MultiRangeIndex<int, double> * getIndex() = 0;
  private:
    CPPUNIT_TEST_SUB_SUITE(MultiRangeIndexTest, MultiIndexTest);
    CPPUNIT_TEST(TestRangeQuery);
    CPPUNIT_TEST(TestRangeQueryNoKeys);
    CPPUNIT_TEST(TestRangeQueryLimit);
    CPPUNIT_TEST_SUITE_END_ABSTRACT();
};

void
UniqueIndexTest::TestInsert()
{
    RAMCloud::UniqueIndex<int, double> *index = getIndex();
    index->Insert(1, 4.3);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(4.3, index->Lookup(1), D);
    index->Insert(0, 0.9);
    index->Insert(2, 4.7);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.9, index->Lookup(0), D);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(4.3, index->Lookup(1), D);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(4.7, index->Lookup(2), D);
    CPPUNIT_ASSERT_THROW(index->Insert(2, 50.3), RAMCloud::IndexException);
}

void
UniqueIndexTest::TestRemove()
{
    RAMCloud::UniqueIndex<int, double> *index = getIndex();
    index->Insert(1, 4.3);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(4.3, index->Lookup(1), D);
    index->Remove(1, 4.3);
    CPPUNIT_ASSERT_THROW(index->Lookup(1), RAMCloud::IndexException);
    CPPUNIT_ASSERT_THROW(index->Remove(1, 4.3), RAMCloud::IndexException);
}

void
UniqueIndexTest::TestLookup()
{
    RAMCloud::UniqueIndex<int, double> *index = getIndex();

    CPPUNIT_ASSERT_THROW(index->Lookup(1), RAMCloud::IndexException);
    index->Insert(1, 4.3);
    index->Insert(2, 4.5);
    index->Insert(3, 4.7);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(4.3, index->Lookup(1), D);
    CPPUNIT_ASSERT_THROW(index->Lookup(4), RAMCloud::IndexException);
}

void
UniqueRangeIndexTest::TestRangeQuery()
{
    RAMCloud::UniqueRangeIndex<int, double> *index = getIndex();

    RANGE_QUERY_ASSERT(" [-1, 10]   =>  {}");

    index->Insert(1, 4.3);
    RANGE_QUERY_ASSERT(" [1, 1]     =>  {1: 4.3}");
    RANGE_QUERY_ASSERT(" (1, 1]     =>  {}");
    RANGE_QUERY_ASSERT(" [1, 1)     =>  {}");
    RANGE_QUERY_ASSERT(" (1, 1)     =>  {}");

    RANGE_QUERY_ASSERT(" [-1, 10]   =>  {1: 4.3}");
    RANGE_QUERY_ASSERT(" [-1, 1]    =>  {1: 4.3}");
    RANGE_QUERY_ASSERT(" [1, 10]    =>  {1: 4.3}");
    RANGE_QUERY_ASSERT(" [1, 2)     =>  {1: 4.3}");
    RANGE_QUERY_ASSERT(" (0, 1]     =>  {1: 4.3}");
    RANGE_QUERY_ASSERT(" (0, 2)     =>  {1: 4.3}");

    index->Insert(2, 7.9);
    RANGE_QUERY_ASSERT(" [-1, 10]   => {1: 4.3, 2: 7.9}");
    RANGE_QUERY_ASSERT(" [1, 10]    => {1: 4.3, 2: 7.9}");
    RANGE_QUERY_ASSERT(" (1, 10]    => {2: 7.9}");
}

void
UniqueRangeIndexTest::TestRangeQueryNoKeys()
{
    RAMCloud::UniqueRangeIndex<int, double> *index = getIndex();

    index->Insert(1, 4.3);
    index->Insert(2, 7.9);

    {
        double valbuf[4];
        memset(valbuf, 0xCD, sizeof(valbuf));
        CPPUNIT_ASSERT(index->RangeQuery(1, true, 2, true, 3, valbuf + 1) == 2);
        CPPUNIT_ASSERT(*reinterpret_cast<uint64_t*>(&valbuf[0]) == \
                       0xCDCDCDCDCDCDCDCD);
        CPPUNIT_ASSERT(*reinterpret_cast<uint64_t*>(&valbuf[3]) == \
                       0xCDCDCDCDCDCDCDCD);
        std::set<double> expected;
        std::set<double> actual;
        expected.insert(4.3);
        expected.insert(7.9);
        actual.insert(valbuf[1]);
        actual.insert(valbuf[2]);
        CPPUNIT_ASSERT(sets_equal(&expected, &actual));
    }
}

void
UniqueRangeIndexTest::TestRangeQueryLimit()
{
    RAMCloud::UniqueRangeIndex<int, double> *index = getIndex();

    double valbuf[100];
    index->Insert(1, 4.3);
    index->Insert(2, 7.9);
    CPPUNIT_ASSERT(index->RangeQuery(1, true, 2, true, 0, valbuf + 1) == 0);
    CPPUNIT_ASSERT(index->RangeQuery(1, true, 2, true, 1, valbuf + 1) == 1);
    CPPUNIT_ASSERT(index->RangeQuery(1, true, 2, true, 2, valbuf + 1) == 2);
    CPPUNIT_ASSERT(index->RangeQuery(1, true, 2, true, 100, valbuf + 1) == 2);
}


void
MultiIndexTest::TestInsert()
{
    RAMCloud::MultiIndex<int, double> *index = getIndex();

    index->Insert(1, 4.3);
    MULTI_LOOKUP_ASSERT("1 => {4.3}");

    index->Insert(0, 0.9);
    index->Insert(2, 4.7);
    MULTI_LOOKUP_ASSERT("0 => {0.9}");
    MULTI_LOOKUP_ASSERT("1 => {4.3}");
    MULTI_LOOKUP_ASSERT("2 => {4.7}");
}

void
MultiIndexTest::TestRemove()
{
    RAMCloud::MultiIndex<int, double> *index = getIndex();

    CPPUNIT_ASSERT_THROW(index->Remove(1, 4.3), RAMCloud::IndexException);
    index->Insert(1, 4.3);
    index->Remove(1, 4.3);
    MULTI_LOOKUP_ASSERT("1 => {}");
    CPPUNIT_ASSERT_THROW(index->Remove(1, 4.3), RAMCloud::IndexException);
}

void
MultiIndexTest::TestLookup()
{
    RAMCloud::MultiIndex<int, double> *index = getIndex();

    MULTI_LOOKUP_ASSERT("1  => {}");

    index->Insert(1, 4.3);
    MULTI_LOOKUP_ASSERT("1  => {4.3}");
    MULTI_LOOKUP_ASSERT("2  => {}");

    index->Insert(1, 10.9);
    index->Insert(1, 22.2);
    index->Insert(1, 33.3);
    MULTI_LOOKUP_ASSERT("0 => {}");
    MULTI_LOOKUP_ASSERT("1 => {4.3, 10.9, 22.2, 33.3}");
    MULTI_LOOKUP_ASSERT("2 => {}");
}

void
MultiIndexTest::TestLookupLimit()
{
    RAMCloud::MultiIndex<int, double> *index = getIndex();

    double buf[100];

    index->Insert(1, 4.3);
    index->Insert(1, 9.5);
    index->Insert(1, 0.5);
    CPPUNIT_ASSERT(index->Lookup(1, 0,   buf) == 0);
    CPPUNIT_ASSERT(index->Lookup(1, 1,   buf) == 1);
    CPPUNIT_ASSERT(index->Lookup(1, 2,   buf) == 2);
    CPPUNIT_ASSERT(index->Lookup(1, 3,   buf) == 3);
    CPPUNIT_ASSERT(index->Lookup(1, 100, buf) == 3);
}

void
MultiRangeIndexTest::TestRangeQuery()
{
    RAMCloud::MultiRangeIndex<int, double> *index = getIndex();

    RANGE_QUERY_ASSERT(" [-1, 10]    =>  {}");

    index->Insert(1, 4.3);
    RANGE_QUERY_ASSERT(" [1, 1]     =>  {1: 4.3}");
    RANGE_QUERY_ASSERT(" (1, 1]     =>  {}");
    RANGE_QUERY_ASSERT(" [1, 1)     =>  {}");
    RANGE_QUERY_ASSERT(" (1, 1)     =>  {}");


    RANGE_QUERY_ASSERT(" [-1, 10]   =>  {1: 4.3}");
    RANGE_QUERY_ASSERT(" [-1, 1]    =>  {1: 4.3}");
    RANGE_QUERY_ASSERT(" [1, 10]    =>  {1: 4.3}");
    RANGE_QUERY_ASSERT(" [1, 2)     =>  {1: 4.3}");
    RANGE_QUERY_ASSERT(" (0, 1]     =>  {1: 4.3}");
    RANGE_QUERY_ASSERT(" (0, 2)     =>  {1: 4.3}");
    RANGE_QUERY_ASSERT(" (1, 2)     =>  {}");
    RANGE_QUERY_ASSERT(" (0, 1]     =>  {1: 4.3}");

    index->Insert(2, 58.4);
    RANGE_QUERY_ASSERT(" [-1, 10]   => {1: 4.3, 2: 58.4}");
    RANGE_QUERY_ASSERT(" [1, 10]    => {1: 4.3, 2: 58.4}");
    RANGE_QUERY_ASSERT(" (1, 10]    => {2: 58.4}");


    index->Insert(3, 60.6);
    index->Insert(8, 921.0);
    index->Insert(1, 10.9);
    index->Insert(1, 22.2);
    index->Insert(1, 33.3);
    RANGE_QUERY_ASSERT(" [0, 10]    =>  {1: 4.3, 1: 10.9, 1: 22.2, 1: 33.3, "\
                                        "2: 58.4, 3: 60.6, 8: 921.0}");
    RANGE_QUERY_ASSERT(" (1, 8)     =>  {2: 58.4, 3: 60.6}");
}

void
MultiRangeIndexTest::TestRangeQueryNoKeys()
{
    RAMCloud::MultiRangeIndex<int, double> *index = getIndex();

    index->Insert(1, 4.3);
    index->Insert(1, 9.5);
    index->Insert(2, 7.9);

    {
        double valbuf[5];
        memset(valbuf, 0xCD, sizeof(valbuf));
        CPPUNIT_ASSERT(index->RangeQuery(1, true, 2, true, 4, valbuf + 1) == 3);
        CPPUNIT_ASSERT(*reinterpret_cast<uint64_t*>(&valbuf[0]) ==
                       0xCDCDCDCDCDCDCDCD);
        CPPUNIT_ASSERT(*reinterpret_cast<uint64_t*>(&valbuf[4]) ==
                       0xCDCDCDCDCDCDCDCD);
        std::set<double> expected;
        std::set<double> actual;
        expected.insert(4.3);
        expected.insert(9.5);
        expected.insert(7.9);
        actual.insert(valbuf[1]);
        actual.insert(valbuf[2]);
        actual.insert(valbuf[3]);
        CPPUNIT_ASSERT(sets_equal(&expected, &actual));
    }
}

void
MultiRangeIndexTest::TestRangeQueryLimit()
{
    RAMCloud::MultiRangeIndex<int, double> *index = getIndex();

    double buf[100];
    index->Insert(1, 4.3);
    index->Insert(1, 9.5);
    index->Insert(2, 7.9);
    CPPUNIT_ASSERT(index->RangeQuery(1, true, 2, true, 0,   buf) == 0);
    CPPUNIT_ASSERT(index->RangeQuery(1, true, 2, true, 1,   buf) == 1);
    CPPUNIT_ASSERT(index->RangeQuery(1, true, 2, true, 2,   buf) == 2);
    CPPUNIT_ASSERT(index->RangeQuery(1, true, 2, true, 3,   buf) == 3);
    CPPUNIT_ASSERT(index->RangeQuery(1, true, 2, true, 100, buf) == 3);
}

class STLUniqueRangeIndexTest : public UniqueRangeIndexTest {
  public:
    STLUniqueRangeIndexTest() : index(NULL) {
    }

    void setUp() {
        index = new RAMCloud::STLUniqueRangeIndex<int, double>();
    }

    void tearDown() {
        delete index;
    }

  protected:
    RAMCloud::STLUniqueRangeIndex<int, double> * getIndex() {
        return index;
    }

  private:
    CPPUNIT_TEST_SUB_SUITE(STLUniqueRangeIndexTest, UniqueRangeIndexTest);
    CPPUNIT_TEST_SUITE_END();
    RAMCloud::STLUniqueRangeIndex<int, double> *index;
    DISALLOW_COPY_AND_ASSIGN(STLUniqueRangeIndexTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(STLUniqueRangeIndexTest);

class STLMultiRangeIndexTest : public MultiRangeIndexTest {
  public:
    STLMultiRangeIndexTest() : index(NULL) {
    }

    void setUp() {
        index = new RAMCloud::STLMultiRangeIndex<int, double>();
    }

    void tearDown() {
        delete index;
    }

  protected:
    RAMCloud::STLMultiRangeIndex<int, double> * getIndex() {
        return index;
    }

  private:
    CPPUNIT_TEST_SUB_SUITE(STLMultiRangeIndexTest, MultiRangeIndexTest);
    CPPUNIT_TEST_SUITE_END();
    RAMCloud::STLMultiRangeIndex<int, double> *index;
    DISALLOW_COPY_AND_ASSIGN(STLMultiRangeIndexTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(STLMultiRangeIndexTest);
