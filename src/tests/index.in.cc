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

class UniqueIndexTest : public CppUnit::TestFixture {
  public:
    virtual void setUp() = 0;
    virtual void tearDown() = 0;
    void TestInsert();
    void TestRemove();
    void TestLookup();
  protected:
    virtual RAMCloud::UniqueIndex * getIndex() = 0;
    virtual RAMCloud::UniqueIndex * getStringIndex() = 0;
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
    void TestRangeQueryContinuation() { /* TODO */ }
    void TestRangeQueryString();
  protected:
    virtual RAMCloud::UniqueRangeIndex * getIndex() = 0;
    virtual RAMCloud::UniqueRangeIndex * getStringIndex() = 0;
  private:
    CPPUNIT_TEST_SUB_SUITE(UniqueRangeIndexTest, UniqueIndexTest);
    CPPUNIT_TEST(TestRangeQuery);
    CPPUNIT_TEST(TestRangeQueryNoKeys);
    CPPUNIT_TEST(TestRangeQueryLimit);
    CPPUNIT_TEST(TestRangeQueryContinuation);
    CPPUNIT_TEST(TestRangeQueryString);
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
    void TestLookupContinuation() { /* TODO */ }
  protected:
    virtual RAMCloud::MultiIndex * getIndex() = 0;
    virtual RAMCloud::MultiIndex * getStringIndex() = 0;
  private:
    CPPUNIT_TEST_SUITE(MultiIndexTest);
    CPPUNIT_TEST(TestInsert);
    CPPUNIT_TEST(TestRemove);
    CPPUNIT_TEST(TestLookup);
    CPPUNIT_TEST(TestLookupLimit);
    CPPUNIT_TEST(TestLookupContinuation);
    CPPUNIT_TEST_SUITE_END_ABSTRACT();
};

class MultiRangeIndexTest : public MultiIndexTest {
  public:
    void TestRangeQuery();
    void TestRangeQueryNoKeys();
    void TestRangeQueryLimit();
    void TestRangeQueryContinuation() { /* TODO */ }
    void TestRangeQueryString();
  protected:
    virtual RAMCloud::MultiRangeIndex * getIndex() = 0;
    virtual RAMCloud::MultiRangeIndex * getStringIndex() = 0;
  private:
    CPPUNIT_TEST_SUB_SUITE(MultiRangeIndexTest, MultiIndexTest);
    CPPUNIT_TEST(TestRangeQuery);
    CPPUNIT_TEST(TestRangeQueryNoKeys);
    CPPUNIT_TEST(TestRangeQueryLimit);
    CPPUNIT_TEST(TestRangeQueryContinuation);
    CPPUNIT_TEST(TestRangeQueryString);
    CPPUNIT_TEST_SUITE_END_ABSTRACT();
};

static void
Insert(RAMCloud::UniqueIndex *index, int32_t key, uint64_t val) {
    RAMCloud::IndexKeyRef kr(&key, sizeof(key));
    index->Insert(kr, val);
}

static uint64_t
Lookup(RAMCloud::UniqueIndex *index, int32_t key) {
    RAMCloud::IndexKeyRef kr(&key, sizeof(key));
    return index->Lookup(kr);
}

static void
Remove(RAMCloud::UniqueIndex *index, int32_t key, uint64_t val) {
    RAMCloud::IndexKeyRef kr(&key, sizeof(key));
    index->Remove(kr, val);
}

static void
Insert(RAMCloud::MultiIndex *index, int32_t key, uint64_t val) {
    RAMCloud::IndexKeyRef kr(&key, sizeof(key));
    index->Insert(kr, val);
}

static void
Remove(RAMCloud::MultiIndex *index, int32_t key, uint64_t val) {
    RAMCloud::IndexKeyRef kr(&key, sizeof(key));
    index->Remove(kr, val);
}

void
UniqueIndexTest::TestInsert()
{
    RAMCloud::UniqueIndex *index = getIndex();
    Insert(index, 1, 43);
    CPPUNIT_ASSERT(43 == Lookup(index, 1));
    Insert(index, 0, 9);
    Insert(index, 2, 47);
    CPPUNIT_ASSERT(9 == Lookup(index, 0));
    CPPUNIT_ASSERT(43 == Lookup(index, 1));
    CPPUNIT_ASSERT(47 == Lookup(index, 2));
    CPPUNIT_ASSERT_THROW(Insert(index, 2, 503), RAMCloud::IndexException);
}

void
UniqueIndexTest::TestRemove()
{
    RAMCloud::UniqueIndex *index = getIndex();
    Insert(index, 1, 43);
    CPPUNIT_ASSERT(43 == Lookup(index, 1));
    Remove(index, 1, 43);
    CPPUNIT_ASSERT_THROW(Lookup(index, 1), RAMCloud::IndexException);
    CPPUNIT_ASSERT_THROW(Remove(index, 1, 43), RAMCloud::IndexException);
}

void
UniqueIndexTest::TestLookup()
{
    RAMCloud::UniqueIndex *index = getIndex();

    CPPUNIT_ASSERT_THROW(Lookup(index, 1), RAMCloud::IndexException);
    Insert(index, 1, 43);
    Insert(index, 2, 45);
    Insert(index, 3, 47);
    CPPUNIT_ASSERT(43 == Lookup(index, 1));
    CPPUNIT_ASSERT_THROW(Lookup(index, 4), RAMCloud::IndexException);
}

void
UniqueRangeIndexTest::TestRangeQuery()
{
    RAMCloud::UniqueRangeIndex *index = getIndex();

    RANGE_QUERY_ASSERT(" [-1, 10]   =>  {}");

    Insert(index, 1, 43);
    RANGE_QUERY_ASSERT(" [1, 1]     =>  {1: 43}");
    RANGE_QUERY_ASSERT(" (1, 1]     =>  {}");
    RANGE_QUERY_ASSERT(" [1, 1)     =>  {}");
    RANGE_QUERY_ASSERT(" (1, 1)     =>  {}");

    RANGE_QUERY_ASSERT(" [-1, 10]   =>  {1: 43}");
    RANGE_QUERY_ASSERT(" [-1, 1]    =>  {1: 43}");
    RANGE_QUERY_ASSERT(" [1, 10]    =>  {1: 43}");
    RANGE_QUERY_ASSERT(" [1, 2)     =>  {1: 43}");
    RANGE_QUERY_ASSERT(" (0, 1]     =>  {1: 43}");
    RANGE_QUERY_ASSERT(" (0, 2)     =>  {1: 43}");

    Insert(index, 2, 79);
    RANGE_QUERY_ASSERT(" [-1, 10]   => {1: 43, 2: 79}");
    RANGE_QUERY_ASSERT(" [1, 10]    => {1: 43, 2: 79}");
    RANGE_QUERY_ASSERT(" (1, 10]    => {2: 79}");
    RANGE_QUERY_ASSERT(" [3, 0]     => {}");
    RANGE_QUERY_ASSERT(" [2, 0]     => {}");
    RANGE_QUERY_ASSERT(" [2, 1]     => {}");
}

void
UniqueRangeIndexTest::TestRangeQueryNoKeys()
{
    RAMCloud::UniqueRangeIndex *index = getIndex();

    Insert(index, 1, 43);
    Insert(index, 2, 79);

    {
        uint64_t valbuf[4];
        memset(valbuf, 0xCD, sizeof(valbuf));
        RAMCloud::RangeQueryArgs rq;
        bool more;
        int32_t start = 1;
        RAMCloud::IndexKeyRef skr(&start, sizeof(start));
        rq.setKeyStart(skr, true);
        int32_t end = 2;
        RAMCloud::IndexKeyRef ekr(&end, sizeof(end));
        rq.setKeyEnd(ekr, true);
        rq.setLimit(3);
        RAMCloud::IndexOIDsRef ror(valbuf + 1, 2);
        rq.setResultBuf(ror);
        rq.setResultMore(&more);
        CPPUNIT_ASSERT(index->RangeQuery(&rq) == 2);
        CPPUNIT_ASSERT(!more);
        CPPUNIT_ASSERT(*reinterpret_cast<uint64_t*>(&valbuf[0])  == 0xCDCDCDCDCDCDCDCD);
        CPPUNIT_ASSERT(*reinterpret_cast<uint64_t*>(&valbuf[3]) == 0xCDCDCDCDCDCDCDCD);
        CPPUNIT_ASSERT(43 == valbuf[1]);
        CPPUNIT_ASSERT(79 == valbuf[2]);
    }
}

void
UniqueRangeIndexTest::TestRangeQueryLimit()
{
    RAMCloud::UniqueRangeIndex *index = getIndex();
    RAMCloud::RangeQueryArgs rq;
    uint64_t valbuf[100];

    int32_t start = 1;
    RAMCloud::IndexKeyRef skr(&start, sizeof(start));
    rq.setKeyStart(skr, true);
    int32_t end = 2;
    RAMCloud::IndexKeyRef ekr(&end, sizeof(end));
    rq.setKeyEnd(ekr, true);
    RAMCloud::IndexOIDsRef ror(valbuf + 1, 99);
    rq.setResultBuf(ror);

    Insert(index, 1, 43);
    Insert(index, 2, 79);

    rq.setLimit(0);
    CPPUNIT_ASSERT(index->RangeQuery(&rq) == 0);
    rq.setLimit(1);
    CPPUNIT_ASSERT(index->RangeQuery(&rq) == 1);
    rq.setLimit(2);
    CPPUNIT_ASSERT(index->RangeQuery(&rq) == 2);
    rq.setLimit(100);
    CPPUNIT_ASSERT(index->RangeQuery(&rq) == 2);
}

void
UniqueRangeIndexTest::TestRangeQueryString()
{
    RAMCloud::UniqueRangeIndex *index = getStringIndex();
    RAMCloud::RangeQueryArgs rq;
    char keybuf[100];
    uint64_t valbuf[2];

    const char *start = "roflcopter";
    RAMCloud::IndexKeyRef skr(start, strlen(start));
    rq.setKeyStart(skr, true);
    const char *end = "roflsaurus";
    RAMCloud::IndexKeyRef ekr(end, strlen(end));
    rq.setKeyEnd(ekr, true);
    RAMCloud::IndexKeysRef rkr(keybuf, 98);
    RAMCloud::IndexOIDsRef ror(valbuf, 98);
    rq.setResultBuf(rkr, ror);

    index->Insert(ekr, 79);
    index->Insert(skr, 43);

    rq.setLimit(10);
    CPPUNIT_ASSERT(index->RangeQuery(&rq) == 2);
    CPPUNIT_ASSERT(ror.used == 2);
    CPPUNIT_ASSERT(valbuf[0] = 43);
    CPPUNIT_ASSERT(valbuf[1] = 79);
    CPPUNIT_ASSERT(rkr.used == strlen(start) + strlen(end) + sizeof(uint8_t)*2);
    char *var = keybuf;
    CPPUNIT_ASSERT(*reinterpret_cast<uint8_t*>(var) == strlen(start));
    var += sizeof(uint8_t);
    CPPUNIT_ASSERT(strncmp(start, var, strlen(start)) == 0);
    var += strlen(start);
    CPPUNIT_ASSERT(*reinterpret_cast<uint8_t*>(var) == strlen(end));
    var += sizeof(uint8_t);
    CPPUNIT_ASSERT(strncmp(end, var, strlen(end)) == 0);
}

void
MultiIndexTest::TestInsert()
{
    RAMCloud::MultiIndex *index = getIndex();

    Insert(index, 1, 43);
    MULTI_LOOKUP_ASSERT("1 => {43}");

    Insert(index, 0, 9);
    Insert(index, 2, 47);
    MULTI_LOOKUP_ASSERT("0 => {9}");
    MULTI_LOOKUP_ASSERT("1 => {43}");
    MULTI_LOOKUP_ASSERT("2 => {47}");
}

void
MultiIndexTest::TestRemove()
{
    RAMCloud::MultiIndex *index = getIndex();

    CPPUNIT_ASSERT_THROW(Remove(index, 1, 43), RAMCloud::IndexException);
    Insert(index, 1, 43);
    Remove(index, 1, 43);
    MULTI_LOOKUP_ASSERT("1 => {}");
    CPPUNIT_ASSERT_THROW(Remove(index, 1, 43), RAMCloud::IndexException);
}

void
MultiIndexTest::TestLookup()
{
    RAMCloud::MultiIndex *index = getIndex();

    MULTI_LOOKUP_ASSERT("1  => {}");

    Insert(index, 1, 43);
    MULTI_LOOKUP_ASSERT("1  => {43}");
    MULTI_LOOKUP_ASSERT("2  => {}");

    Insert(index, 1, 109);
    Insert(index, 1, 222);
    Insert(index, 1, 333);
    MULTI_LOOKUP_ASSERT("0 => {}");
    MULTI_LOOKUP_ASSERT("1 => {43, 109, 222, 333}");
    MULTI_LOOKUP_ASSERT("2 => {}");
}

void
MultiIndexTest::TestLookupLimit()
{
    RAMCloud::MultiIndex *index = getIndex();
    RAMCloud::MultiLookupArgs ml;
    uint64_t buf[100];
    bool more;

    int32_t key = 1;
    RAMCloud::IndexKeyRef kr(&key, sizeof(key));
    ml.setKey(kr);
    RAMCloud::IndexOIDsRef ror(buf, 100);
    ml.setResultBuf(ror);
    ml.setResultMore(&more);

    Insert(index, 1, 43);
    Insert(index, 1, 95);
    Insert(index, 1, 05);

    ml.setLimit(0);
    CPPUNIT_ASSERT(index->Lookup(&ml) == 0);

    ml.setLimit(1);
    CPPUNIT_ASSERT(index->Lookup(&ml) == 1);

    ml.setLimit(2);
    CPPUNIT_ASSERT(index->Lookup(&ml) == 2);

    ml.setLimit(3);
    CPPUNIT_ASSERT(index->Lookup(&ml) == 3);

    ml.setLimit(100);
    CPPUNIT_ASSERT(index->Lookup(&ml) == 3);
}

void
MultiRangeIndexTest::TestRangeQuery()
{
    RAMCloud::MultiRangeIndex *index = getIndex();

    RANGE_QUERY_ASSERT(" [-1, 10]    =>  {}");

    Insert(index, 1, 43);
    RANGE_QUERY_ASSERT(" [1, 1]     =>  {1: 43}");
    RANGE_QUERY_ASSERT(" (1, 1]     =>  {}");
    RANGE_QUERY_ASSERT(" [1, 1)     =>  {}");
    RANGE_QUERY_ASSERT(" (1, 1)     =>  {}");


    RANGE_QUERY_ASSERT(" [-1, 10]   =>  {1: 43}");
    RANGE_QUERY_ASSERT(" [-1, 1]    =>  {1: 43}");
    RANGE_QUERY_ASSERT(" [1, 10]    =>  {1: 43}");
    RANGE_QUERY_ASSERT(" [1, 2)     =>  {1: 43}");
    RANGE_QUERY_ASSERT(" (0, 1]     =>  {1: 43}");
    RANGE_QUERY_ASSERT(" (0, 2)     =>  {1: 43}");
    RANGE_QUERY_ASSERT(" (1, 2)     =>  {}");
    RANGE_QUERY_ASSERT(" (0, 1]     =>  {1: 43}");

    Insert(index, 2, 584);
    RANGE_QUERY_ASSERT(" [-1, 10]   => {1: 43, 2: 584}");
    RANGE_QUERY_ASSERT(" [1, 10]    => {1: 43, 2: 584}");
    RANGE_QUERY_ASSERT(" (1, 10]    => {2: 584}");
    RANGE_QUERY_ASSERT(" [3, 0]     => {}");
    RANGE_QUERY_ASSERT(" [2, 0]     => {}");
    RANGE_QUERY_ASSERT(" [2, 1]     => {}");


    Insert(index, 3, 606);
    Insert(index, 8, 9210);
    Insert(index, 1, 222);
    Insert(index, 1, 333);
    Insert(index, 1, 109);
    RANGE_QUERY_ASSERT(" [0, 10]    =>  {1: 43, 1: 109, 1: 222, 1: 333, "\
                                        "2: 584, 3: 606, 8: 9210}");
    RANGE_QUERY_ASSERT(" (1, 8)     =>  {2: 584, 3: 606}");
}

void
MultiRangeIndexTest::TestRangeQueryNoKeys()
{
    RAMCloud::MultiRangeIndex *index = getIndex();

    Insert(index, 1, 43);
    Insert(index, 1, 95);
    Insert(index, 2, 79);

    {
        uint64_t valbuf[5];
        memset(valbuf, 0xCD, sizeof(valbuf));
        RAMCloud::RangeQueryArgs rq;
        bool more;
        int32_t start = 1;
        RAMCloud::IndexKeyRef skr(&start, sizeof(start));
        rq.setKeyStart(skr, true);
        int32_t end = 2;
        RAMCloud::IndexKeyRef ekr(&end, sizeof(end));
        rq.setKeyEnd(ekr, true);
        rq.setLimit(4);
        RAMCloud::IndexOIDsRef ror(valbuf + 1, 3);
        rq.setResultBuf(ror);
        rq.setResultMore(&more);
        CPPUNIT_ASSERT(index->RangeQuery(&rq) == 3);
        CPPUNIT_ASSERT(!more);
        CPPUNIT_ASSERT(*reinterpret_cast<uint64_t*>(&valbuf[0]) ==
                       0xCDCDCDCDCDCDCDCD);
        CPPUNIT_ASSERT(*reinterpret_cast<uint64_t*>(&valbuf[4]) ==
                       0xCDCDCDCDCDCDCDCD);
        CPPUNIT_ASSERT(43 == valbuf[1]);
        CPPUNIT_ASSERT(95 == valbuf[2]);
        CPPUNIT_ASSERT(79 == valbuf[3]);
    }
}

void
MultiRangeIndexTest::TestRangeQueryLimit()
{
    RAMCloud::MultiRangeIndex *index = getIndex();
    RAMCloud::RangeQueryArgs rq;
    uint64_t valbuf[100];

    int32_t start = 1;
    RAMCloud::IndexKeyRef skr(&start, sizeof(start));
    rq.setKeyStart(skr, true);
    int32_t end = 2;
    RAMCloud::IndexKeyRef ekr(&end, sizeof(end));
    rq.setKeyEnd(ekr, true);
    RAMCloud::IndexOIDsRef ror(valbuf + 1, 99);
    rq.setResultBuf(ror);

    Insert(index, 1, 43);
    Insert(index, 1, 95);
    Insert(index, 2, 79);

    rq.setLimit(0);
    CPPUNIT_ASSERT(index->RangeQuery(&rq) == 0);
    rq.setLimit(1);
    CPPUNIT_ASSERT(index->RangeQuery(&rq) == 1);
    rq.setLimit(2);
    CPPUNIT_ASSERT(index->RangeQuery(&rq) == 2);
    rq.setLimit(3);
    CPPUNIT_ASSERT(index->RangeQuery(&rq) == 3);
    rq.setLimit(100);
    CPPUNIT_ASSERT(index->RangeQuery(&rq) == 3);
}

void
MultiRangeIndexTest::TestRangeQueryString()
{
    RAMCloud::MultiRangeIndex *index = getStringIndex();
    RAMCloud::RangeQueryArgs rq;
    char keybuf[100];
    uint64_t valbuf[2];

    const char *start = "roflcopter";
    RAMCloud::IndexKeyRef skr(start, strlen(start));
    rq.setKeyStart(skr, true);
    const char *end = "roflsaurus";
    RAMCloud::IndexKeyRef ekr(end, strlen(end));
    rq.setKeyEnd(ekr, true);
    RAMCloud::IndexKeysRef rkr(keybuf, 98);
    RAMCloud::IndexOIDsRef ror(valbuf, 98);
    rq.setResultBuf(rkr, ror);

    index->Insert(ekr, 79);
    index->Insert(skr, 238);
    index->Insert(skr, 43);

    rq.setLimit(10);
    CPPUNIT_ASSERT(index->RangeQuery(&rq) == 3);
    CPPUNIT_ASSERT(ror.used == 3);
    CPPUNIT_ASSERT(valbuf[0] = 43);
    CPPUNIT_ASSERT(valbuf[1] = 238);
    CPPUNIT_ASSERT(valbuf[2] = 79);
    CPPUNIT_ASSERT(rkr.used == strlen(start)*2 + strlen(end) + sizeof(uint8_t)*3);
    char *var = keybuf;
    CPPUNIT_ASSERT(*reinterpret_cast<uint8_t*>(var) == strlen(start));
    var += sizeof(uint8_t);
    CPPUNIT_ASSERT(strncmp(start, var, strlen(start)) == 0);
    var += strlen(start);
    CPPUNIT_ASSERT(*reinterpret_cast<uint8_t*>(var) == strlen(start));
    var += sizeof(uint8_t);
    CPPUNIT_ASSERT(strncmp(start, var, strlen(start)) == 0);
    var += strlen(start);
    CPPUNIT_ASSERT(*reinterpret_cast<uint8_t*>(var) == strlen(end));
    var += sizeof(uint8_t);
    CPPUNIT_ASSERT(strncmp(end, var, strlen(end)) == 0);
}

class STLUniqueRangeIndexTest : public UniqueRangeIndexTest {
  public:
    STLUniqueRangeIndexTest() : index(NULL), stringIndex(NULL) {
    }

    void setUp() {
        index = new RAMCloud::STLUniqueRangeIndex(RCRPC_INDEX_TYPE_SINT32);
        stringIndex = new RAMCloud::STLUniqueRangeIndex(RCRPC_INDEX_TYPE_STRING);
    }

    void tearDown() {
        delete index;
        delete stringIndex;
    }

  protected:
    RAMCloud::STLUniqueRangeIndex * getIndex() {
        return index;
    }

    RAMCloud::STLUniqueRangeIndex * getStringIndex() {
        return stringIndex;
    }

  private:
    CPPUNIT_TEST_SUB_SUITE(STLUniqueRangeIndexTest, UniqueRangeIndexTest);
    CPPUNIT_TEST_SUITE_END();
    RAMCloud::STLUniqueRangeIndex *index;
    RAMCloud::STLUniqueRangeIndex *stringIndex;
    DISALLOW_COPY_AND_ASSIGN(STLUniqueRangeIndexTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(STLUniqueRangeIndexTest);

class STLMultiRangeIndexTest : public MultiRangeIndexTest {
  public:
    STLMultiRangeIndexTest() : index(NULL), stringIndex(NULL) {
    }

    void setUp() {
        index = new RAMCloud::STLMultiRangeIndex(RCRPC_INDEX_TYPE_SINT32);
        stringIndex = new RAMCloud::STLMultiRangeIndex(RCRPC_INDEX_TYPE_STRING);
    }

    void tearDown() {
        delete index;
        delete stringIndex;
    }

  protected:
    RAMCloud::STLMultiRangeIndex * getIndex() {
        return index;
    }

    RAMCloud::STLMultiRangeIndex * getStringIndex() {
        return stringIndex;
    }

  private:
    CPPUNIT_TEST_SUB_SUITE(STLMultiRangeIndexTest, MultiRangeIndexTest);
    CPPUNIT_TEST_SUITE_END();
    RAMCloud::STLMultiRangeIndex *index;
    RAMCloud::STLMultiRangeIndex *stringIndex;
    DISALLOW_COPY_AND_ASSIGN(STLMultiRangeIndexTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(STLMultiRangeIndexTest);
