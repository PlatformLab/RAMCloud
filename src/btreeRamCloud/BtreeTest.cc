/* Copyright (c) 2014 Stanford University
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
#include "ObjectManager.h"

// the tree code may not compile if this flag is enabled.
// This is not critical and just incurs implementation overhead
// TODO(anyone): later
// #define BTREE_DEBUG

#include <gtest/gtest.h>
#include "BtreeMultimap.h"
#include "BtreeMultiset.h"

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <vector>
#include <set>
#include <sstream>
#include <iostream>

namespace RAMCloud {

template <typename KeyType>
struct traits_nodebug : str::btree_default_set_traits<KeyType>
{
    static const bool selfverify = false;
    static const bool debug = false;

    // number of slots affects running time of the tests
    static const int leafslots = 128;
    static const int innerslots = 128;
};

class BtreeTest: public ::testing::Test {
  public:
    Context context;
    ServerId serverId;
    ServerList serverList;
    ServerConfig masterConfig;
    MasterTableMetadata masterTableMetadata;
    TabletManager tabletManager;
    ObjectManager objectManager;
    uint64_t tableId;

    BtreeTest()
        : context()
        , serverId(5)
        , serverList(&context)
        , masterConfig(ServerConfig::forTesting())
        , masterTableMetadata()
        , tabletManager()
        , objectManager(&context,
                        &serverId,
                        &masterConfig,
                        &tabletManager,
                        &masterTableMetadata)
        , tableId(1)
    {
        objectManager.initOnceEnlisted();
        tabletManager.addTablet(1, 0, ~0UL, TabletManager::NORMAL);
    }

void test_multi(const unsigned int insnum, const int modulo)
{
    typedef str::btree_multimap<unsigned int, unsigned int,
        std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;
    btree_type bt(tableId, &objectManager);

    typedef std::multiset<unsigned int> multiset_type;
    multiset_type set;

    // *** insert
    srand(34234235);
    for(unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = rand() % modulo;
        unsigned int v = 234;

        EXPECT_TRUE( bt.size() == set.size() );
        bt.insert2(k, v);
        set.insert(k);
        EXPECT_TRUE( bt.count(k) == set.count(k) );

        EXPECT_TRUE( bt.size() == set.size() );
    }

    EXPECT_TRUE( bt.size() == insnum );

    // *** iterate
    {
        btree_type::iterator bi = bt.begin();
        multiset_type::const_iterator si = set.begin();
        for(; bi != bt.end() && si != set.end(); ++bi, ++si)
        {
            EXPECT_TRUE( *si == bi.key() );
        }
        EXPECT_TRUE( bi == bt.end() );
        EXPECT_TRUE( si == set.end() );
    }

    // *** existance
    srand(34234235);
    for(unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = rand() % modulo;

        EXPECT_TRUE( bt.exists(k) );
    }

    // *** counting
    srand(34234235);
    for(unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = rand() % modulo;

        EXPECT_TRUE( bt.count(k) == set.count(k) );
    }

    // *** lower_bound
    for(int k = 0; k < modulo + 100; k++)
    {
        multiset_type::const_iterator si = set.lower_bound(k);
        btree_type::iterator bi = bt.lower_bound(k);

        if ( bi == bt.end() )
            EXPECT_TRUE( si == set.end() );
        else if ( si == set.end() )
            EXPECT_TRUE( bi == bt.end() );
        else
            EXPECT_TRUE( *si == bi.key() );
    }

    // *** upper_bound
    for(int k = 0; k < modulo + 100; k++)
    {
        multiset_type::const_iterator si = set.upper_bound(k);
        btree_type::iterator bi = bt.upper_bound(k);

        if ( bi == bt.end() )
            EXPECT_TRUE( si == set.end() );
        else if ( si == set.end() )
            EXPECT_TRUE( bi == bt.end() );
        else
            EXPECT_TRUE( *si == bi.key() );
    }

    // *** equal_range
    for(int k = 0; k < modulo + 100; k++)
    {
        std::pair<multiset_type::const_iterator,
                    multiset_type::const_iterator> si = set.equal_range(k);
        std::pair<btree_type::iterator,
                    btree_type::iterator> bi = bt.equal_range(k);

        if ( bi.first == bt.end() )
            EXPECT_TRUE( si.first == set.end() );
        else if ( si.first == set.end() )
            EXPECT_TRUE( bi.first == bt.end() );
        else
            EXPECT_TRUE( *si.first == bi.first.key() );

        if ( bi.second == bt.end() )
            EXPECT_TRUE( si.second == set.end() );
        else if ( si.second == set.end() )
            EXPECT_TRUE( bi.second == bt.end() );
        else
            EXPECT_TRUE( *si.second == bi.second.key() );
    }

    // *** deletion
    srand(34234235);
    for(unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = rand() % modulo;

        if (set.find(k) != set.end())
        {
            EXPECT_TRUE( bt.size() == set.size() );

            EXPECT_TRUE( bt.exists(k) );
            EXPECT_TRUE( bt.erase_one(k) );
            set.erase( set.find(k) );

            EXPECT_TRUE( bt.size() == set.size() );
        }
    }

    EXPECT_TRUE( bt.empty() );
    EXPECT_TRUE( set.empty() );
}

void test_multi2(const unsigned int insnum, const unsigned int modulo)
{
    typedef str::btree_multiset<unsigned int,
        std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    btree_type bt(tableId, &objectManager);

    typedef std::multiset<unsigned int> multiset_type;
    multiset_type set;

    // *** insert
    srand(34234235);
    for(unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = rand() % modulo;

        EXPECT_TRUE( bt.size() == set.size() );
        bt.insert(k);
        set.insert(k);
        EXPECT_TRUE( bt.count(k) == set.count(k) );

        EXPECT_TRUE( bt.size() == set.size() );
    }

    EXPECT_TRUE( bt.size() == insnum );

    // *** iterate
    btree_type::iterator bi = bt.begin();
    multiset_type::const_iterator si = set.begin();
    for(; bi != bt.end() && si != set.end(); ++bi, ++si)
    {
        EXPECT_TRUE( *si == bi.key() );
    }
    EXPECT_TRUE( bi == bt.end() );
    EXPECT_TRUE( si == set.end() );

    // *** existance
    srand(34234235);
    for(unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = rand() % modulo;

        EXPECT_TRUE( bt.exists(k) );
    }

    // *** counting
    srand(34234235);
    for(unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = rand() % modulo;

        EXPECT_TRUE( bt.count(k) == set.count(k) );
    }

    // *** deletion
    srand(34234235);
    for(unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = rand() % modulo;

        if (set.find(k) != set.end())
        {
            EXPECT_TRUE( bt.size() == set.size() );

            EXPECT_TRUE( bt.exists(k) );
            EXPECT_TRUE( bt.erase_one(k) );
            set.erase( set.find(k) );

            EXPECT_TRUE( bt.size() == set.size() );
            EXPECT_TRUE( std::equal(bt.begin(), bt.end(), set.begin()) );
        }
    }

    EXPECT_TRUE( bt.empty() );
    EXPECT_TRUE( set.empty() );
}

    DISALLOW_COPY_AND_ASSIGN(BtreeTest);
};

TEST_F(BtreeTest, empty) {
    typedef str::btree_multiset<unsigned int,
        std::less<unsigned int>, traits_nodebug<int> > btree_type;

    btree_type bt(tableId, &objectManager);
    btree_type bt2(tableId, &objectManager);
    bt.verify();
    EXPECT_EQ(bt.erase(42), false);
}

TEST_F(BtreeTest, test_set_insert_erase_3200)
{
    typedef str::btree_multiset<unsigned int,
        std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    btree_type bt(tableId, &objectManager);
    bt.verify();

    srand(34234235);
    for(unsigned int i = 0; i < 3200; i++)
    {
        EXPECT_EQ(bt.size(), i);
        bt.insert(rand() % 100);
        EXPECT_EQ(bt.size(), i + 1);
    }

    bt.verify();

    srand(34234235);
    for(unsigned int i = 0; i < 3200; i++)
    {
        EXPECT_EQ(bt.size(), 3200 - i);
        EXPECT_TRUE( bt.erase_one(rand() % 100) );
        EXPECT_EQ(bt.size(), 3200 - i - 1);
    }

    EXPECT_TRUE( bt.empty() );
}

TEST_F(BtreeTest, test_map_insert_erase_3200_descending)
{
    typedef str::btree_multimap<unsigned int, std::string,
        std::greater<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    btree_type bt(tableId, &objectManager);

    srand(34234235);
    for(unsigned int i = 0; i < 3200; i++)
    {
        EXPECT_EQ(bt.size(), i);
        bt.insert2(rand() % 100, "101");
        EXPECT_EQ(bt.size(), i + 1);
    }

    // test find also. Technically, erase working
    // implies that find() also works.
    srand(34234235);
    for(unsigned int i = 0; i < 3200; i++)
    {
        EXPECT_TRUE(bt.exists(rand() % 100));
        EXPECT_EQ("101", bt.find(rand() % 100)->second);
    }

    srand(34234235);
    for(unsigned int i = 0; i < 3200; i++)
    {
        EXPECT_EQ(bt.size(), 3200 - i);
        int key = rand() % 100;
        EXPECT_TRUE( bt.erase_one(key) );
        EXPECT_EQ(bt.size(), 3200 - i - 1);
    }
    EXPECT_TRUE( bt.empty() );
    bt.verify();
}

TEST_F(BtreeTest, test2_map_insert_erase_strings)
{
    typedef str::btree_multimap<std::string, unsigned int,
        std::less<std::string>, traits_nodebug<std::string> > btree_type;

    std::string letters = "abcdefghijklmnopqrstuvwxyz";

    btree_type bt(tableId, &objectManager);

    for(unsigned int a = 0; a < letters.size(); ++a)
    {
        for(unsigned int b = 0; b < letters.size(); ++b)
        {
            bt.insert2(std::string(1, letters[a]) + letters[b],
                       a * (unsigned int)letters.size() + b);
        }
    }

    for(unsigned int b = 0; b < letters.size(); ++b)
    {
        for(unsigned int a = 0; a < letters.size(); ++a)
        {
            std::string key = std::string(1, letters[a]) + letters[b];

            EXPECT_EQ( bt.find(key)->second, a * letters.size() + b );
            EXPECT_TRUE( bt.erase_one(key) );
        }
    }

    EXPECT_TRUE( bt.empty() );
    bt.verify();
}

TEST_F(BtreeTest, test_multiset_82500_uint32)
{
    str::btree_multiset<uint32_t> bt(tableId, &objectManager);

    for(uint64_t i = 0; i < 82500; ++i)
    {
        uint64_t key = i % 1000;
        bt.insert((unsigned int)key);
    }

    EXPECT_EQ( bt.size(), (unsigned int)82500 );
}

TEST_F(BtreeTest, test_3200_10)
{
    test_multi(3200, 10);
}

TEST_F(BtreeTest, test_320_1000)
{
    test_multi(320, 1000);
}



/* 
TODO: GCC VERSION MISMATCH (UNION)
TEST_F(BTreeTest, test_dump_restore_3200)
{
    typedef str::btree_multiset<unsigned int,
        std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    std::string dumpstr;

    {
        btree_type bt;
        srand(34234235);
        for(unsigned int i = 0; i < 3200; i++)
        {
            bt.insert(rand() % 100);
        }

        EXPECT_EQ(bt.size(), 3200);

        std::ostringstream os;
        bt.dump(os);

        dumpstr = os.str();
    }

    // Also cannot check the length, because it depends on the rand()
    // algorithm in stdlib.
    // EXPECT_TRUE( dumpstr.size() == 47772 );

    // cannot check the string with a hash function, because it contains
    // memory pointers

    {   // restore the btree image
        btree_type bt2;

        std::istringstream iss(dumpstr);
        EXPECT_TRUE( bt2.restore(iss) );

        EXPECT_EQ( bt2.size(), 3200 );

        srand(34234235);
        for(unsigned int i = 0; i < 3200; i++)
        {
        EXPECT_TRUE( bt2.exists(rand() % 100) );
        }
    }

    { // try restore the btree image using a different instantiation

        typedef str::btree_multiset<long long,
        std::less<long long>, traits_nodebug<long long> > otherbtree_type;

        otherbtree_type bt3;

        std::istringstream iss(dumpstr);
        EXPECT_TRUE( !bt3.restore(iss) );
    }

}
*/

TEST_F(BtreeTest, test_iterator1)
{
    typedef str::btree_multiset<unsigned int,
        std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    std::vector<unsigned int> vector;
    srand(34234235);
    for(unsigned int i = 0; i < 3200; i++)
    {
        vector.push_back( rand() % 1000 );
    }

    EXPECT_TRUE( vector.size() == 3200 );

    btree_type bt(tableId, &objectManager, vector.begin(), vector.end());
    EXPECT_TRUE( bt.size() == 3200 );

    // empty out the first bt
    srand(34234235);
    for(unsigned int i = 0; i < 3200; i++)
    {
        EXPECT_TRUE(bt.size() == 3200 - i);
        EXPECT_TRUE( bt.erase_one(rand() % 1000) );
        EXPECT_TRUE(bt.size() == 3200 - i - 1);
    }

    EXPECT_TRUE( bt.empty() );

    // fill up the tree again to the initial values
    bt.insert(vector.begin(), vector.end());

    // copy btree values back to a vector
    std::vector<unsigned int> vector2;
    vector2.assign( bt.begin(), bt.end() );

    // afer sorting the vector, the two must be the same
    std::sort(vector.begin(), vector.end());

    EXPECT_TRUE( vector == vector2 );

    // test iterator
    vector2.clear();
    vector2.assign( bt.begin(), bt.end() );

    btree_type::iterator ri = bt.begin();
    for(unsigned int i = 0; i < vector2.size(); ++i)
    {
        EXPECT_TRUE( vector[i] == vector2[i] );
        EXPECT_TRUE( vector[i] == *ri );

        ri++;
    }

    EXPECT_TRUE( ri == bt.end() );
}

TEST_F(BtreeTest, test_iterator2)
{
    typedef str::btree_multimap<unsigned int, unsigned int,
        std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    std::vector< btree_type::value_type > vector;

    srand(34234235);
    for(unsigned int i = 0; i < 3200; i++)
    {
        vector.push_back( btree_type::value_type(rand() % 1000, 0) );
    }

    EXPECT_TRUE( vector.size() == 3200 );

    // test construction and insert(iter, iter) function
    btree_type bt(tableId, &objectManager, vector.begin(), vector.end());

    EXPECT_TRUE( bt.size() == 3200 );

    // empty out the first bt
    srand(34234235);
    for(unsigned int i = 0; i < 3200; i++)
    {
        EXPECT_TRUE(bt.size() == 3200 - i);
        EXPECT_TRUE( bt.erase_one(rand() % 1000) );
        EXPECT_TRUE(bt.size() == 3200 - i - 1);
    }

    EXPECT_TRUE( bt.empty() );

    // fill up the tree again to the initial values
    bt.insert(vector.begin(), vector.end());

    // copy btree values back to a vector
    std::vector< btree_type::value_type > vector2;
    vector2.assign( bt.begin(), bt.end() );

    // afer sorting the vector, the two must be the same
    std::sort(vector.begin(), vector.end());

    EXPECT_TRUE( vector == vector2 );

    // test reverse iterator
    vector2.clear();
    vector2.assign( bt.begin(), bt.end() );

    btree_type::iterator ri = bt.begin();
    for(unsigned int i = 0; i < vector2.size(); ++i, ++ri)
    {
        EXPECT_TRUE( vector[i].first == vector2[i].first );
        EXPECT_TRUE( vector[i].first == ri->first );
        EXPECT_TRUE( vector[i].second == ri->second );
    }

    EXPECT_TRUE( ri == bt.end() );
}

TEST_F(BtreeTest, test_erase_iterator1)
{
    typedef str::btree_multimap<int, int,
    std::less<int>, traits_nodebug<unsigned int> > btree_type;

    btree_type map(tableId, &objectManager);

    const int size1 = 32; 
    const int size2 = 256; 

    for (int i = 0; i < size1; ++i)
    {
        for (int j = 0; j < size2; ++j)
        {
            map.insert2(i,j);
        }
    }

    EXPECT_TRUE( map.size() == size1 * size2 );

    // erase in reverse order. that should be the worst case for
    for (int i = size1-1; i >= 0; --i)
    {
        for (int j = size2-1; j >= 0; --j)
        {
            // find iterator
            btree_type::iterator it = map.find(i);
            
            while (it != map.end() && it.key() == i && it.data() != j)
                ++it;

            EXPECT_TRUE( it.key() == i );
            EXPECT_TRUE( it.data() == j );

            unsigned int mapsize = (unsigned int)map.size();
            map.erase_one(it.key());
            EXPECT_TRUE( map.size() == mapsize - 1 );
        }
    }

    EXPECT_TRUE( map.size() == 0 );
}

TEST_F(BtreeTest, test_int_mod_1)
{
    test_multi2(3200, 10);
    test_multi2(3200, 100);
}

// this is a separate test to avoid
// overflowing the log
TEST_F(BtreeTest, test_int_mod_2)
{
    test_multi2(3200, 1000);
    test_multi2(3200, 10000);
}

TEST_F(BtreeTest, test_sequence)
{
    typedef str::btree_multiset<unsigned int,
        std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    btree_type bt(tableId, &objectManager);

    const unsigned int insnum = 10000;

    typedef std::multiset<unsigned int> multiset_type;
    multiset_type set;

    // *** insert
    srand(34234235);
    for(unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = i;

        EXPECT_TRUE( bt.size() == set.size() );
        bt.insert(k);
        set.insert(k);
        EXPECT_TRUE( bt.count(k) == set.count(k) );

        EXPECT_TRUE( bt.size() == set.size() );
    }

    EXPECT_TRUE( bt.size() == insnum );

    // *** iterate
    btree_type::iterator bi = bt.begin();
    multiset_type::const_iterator si = set.begin();
    for(; bi != bt.end() && si != set.end(); ++bi, ++si)
    {
        EXPECT_TRUE( *si == bi.key() );
    }
    EXPECT_TRUE( bi == bt.end() );
    EXPECT_TRUE( si == set.end() );

    // *** existance
    srand(34234235);
    for(unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = i;

        EXPECT_TRUE( bt.exists(k) );
    }

    // *** counting
    srand(34234235);
    for(unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = i;

        EXPECT_TRUE( bt.count(k) == set.count(k) );
    }

    // *** deletion
    srand(34234235);
    for(unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = i;

        if (set.find(k) != set.end())
        {
            EXPECT_TRUE( bt.size() == set.size() );

            EXPECT_TRUE( bt.exists(k) );
            EXPECT_TRUE( bt.erase_one(k) );
            set.erase( set.find(k) );

            EXPECT_TRUE( bt.size() == set.size() );
            EXPECT_TRUE( std::equal(bt.begin(), bt.end(), set.begin()) );
        }
    }

    EXPECT_TRUE( bt.empty() );
    EXPECT_TRUE( set.empty() );
}

}
