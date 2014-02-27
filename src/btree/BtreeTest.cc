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

#define BTREE_DEBUG

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

template <typename KeyType>
struct traits_nodebug : stx::btree_default_set_traits<KeyType>
{
    static const bool selfverify = false;
    static const bool debug = false;

    static const int leafslots = 8;
    static const int innerslots = 8;
};

void test_set_instance(size_t numkeys, unsigned int mod)
{
    typedef stx::btree_multiset<unsigned int,
        std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    std::vector<unsigned int> keys (numkeys);

    srand(34234235);
    for(unsigned int i = 0; i < numkeys; i++)
    {
        keys[i] = rand() % mod;
    }

    std::sort(keys.begin(), keys.end());
    btree_type bt;
    bt.bulk_load(keys.begin(), keys.end());

    unsigned int i = 0;
    for(btree_type::iterator it = bt.begin(); it != bt.end(); ++it, ++i)
    {
        EXPECT_EQ(*it, keys[i]);
    }
}

TEST(BtreeTest, set_load)
{
    for (size_t n = 6; n < 3200; ++n)
        test_set_instance(n, 1000);

    test_set_instance(31996, 10000);
    test_set_instance(32000, 10000);
    test_set_instance(117649, 100000);
}


void test_map_instance(size_t numkeys, unsigned int mod)
{
    typedef stx::btree_multimap<int, std::string,
        std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    std::vector< std::pair<int,std::string> > pairs (numkeys);

    srand(34234235);
    for(unsigned int i = 0; i < numkeys; i++)
    {
        pairs[i].first = rand() % mod;
        pairs[i].second = "key";
    }

    std::sort(pairs.begin(), pairs.end());

    btree_type bt;
    bt.bulk_load(pairs.begin(), pairs.end());

    unsigned int i = 0;
    for(btree_type::iterator it = bt.begin();
        it != bt.end(); ++it, ++i)
    {
        EXPECT_EQ(*it, pairs[i]);
    }
}

TEST(BtreeTest, map_load)
{
    for (size_t n = 6; n < 3200; ++n)
        test_map_instance(n, 1000);

    test_map_instance(31996, 10000);
    test_map_instance(32000, 10000);
    test_map_instance(117649, 100000);
}



TEST(BtreeTest, empty) {
    typedef stx::btree_multiset<unsigned int,
        std::less<unsigned int>, traits_nodebug<int> > btree_type;

    btree_type bt, bt2;
    bt.verify();
    EXPECT_EQ(bt.erase(42), false);
    EXPECT_EQ(bt, bt2);
}


TEST(BtreeTest, test_set_insert_erase_3200)
{
    typedef stx::btree_multiset<unsigned int,
        std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    btree_type bt;
    bt.verify();

    srand(34234235);
    for(unsigned int i = 0; i < 3200; i++)
    {
        EXPECT_EQ(bt.size(), i);
        bt.insert(rand() % 100);
        EXPECT_EQ(bt.size(), i + 1);
    }

    srand(34234235);
    for(unsigned int i = 0; i < 3200; i++)
    {
        EXPECT_EQ(bt.size(), 3200 - i);
        EXPECT_TRUE( bt.erase_one(rand() % 100) );
        EXPECT_EQ(bt.size(), 3200 - i - 1);
    }

    EXPECT_TRUE( bt.empty() );
}



TEST(BTreeTest, test_map_insert_erase_3200_descending)
{
    typedef stx::btree_multimap<unsigned int, std::string,
        std::greater<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    btree_type bt;

    srand(34234235);
    for(unsigned int i = 0; i < 3200; i++)
    {
        EXPECT_EQ(bt.size(), i);
        bt.insert2(rand() % 100, "101");
        EXPECT_EQ(bt.size(), i + 1);
    }

    srand(34234235);
    for(unsigned int i = 0; i < 3200; i++)
    {
        EXPECT_EQ(bt.size(), 3200 - i);
        EXPECT_TRUE( bt.erase_one(rand() % 100) );
        EXPECT_EQ(bt.size(), 3200 - i - 1);
    }

    EXPECT_TRUE( bt.empty() );
    bt.verify();
}


TEST(BtreeTest, test2_map_insert_erase_strings)
{
    typedef stx::btree_multimap<std::string, unsigned int,
        std::less<std::string>, traits_nodebug<std::string> > btree_type;

    std::string letters = "abcdefghijklmnopqrstuvwxyz";

    btree_type bt;

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


TEST(BTreeTest, test_multiset_100000_uint32)
{
    stx::btree_multiset<uint32_t> bt;

    for(uint64_t i = 0; i < 100000; ++i)
    {
        uint64_t key = i % 1000;
        bt.insert((unsigned int)key);
    }

    EXPECT_EQ( bt.size(), (unsigned int)100000 );
}


void test_multi(const unsigned int insnum, const int modulo)
{
    typedef stx::btree_multimap<unsigned int, unsigned int,
        std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;
    btree_type bt;

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
        btree_type::const_iterator bi = bt.lower_bound(k);

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
        btree_type::const_iterator bi = bt.upper_bound(k);

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
        std::pair<btree_type::const_iterator,
                    btree_type::const_iterator> bi = bt.equal_range(k);

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

TEST(BtreeTest, test_3200_10)
{
    test_multi(3200, 10);
}

TEST(BtreeTest, test_320_1000)
{
    test_multi(320, 1000);
}



/* 
TODO: GCC VERSION MISMATCH (UNION)
TEST(BTreeTest, test_dump_restore_3200)
{
    typedef stx::btree_multiset<unsigned int,
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

        typedef stx::btree_multiset<long long,
        std::less<long long>, traits_nodebug<long long> > otherbtree_type;

        otherbtree_type bt3;

        std::istringstream iss(dumpstr);
        EXPECT_TRUE( !bt3.restore(iss) );
    }

}
*/


TEST(BtreeTest, test_iterator1)
{
    typedef stx::btree_multiset<unsigned int,
        std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    std::vector<unsigned int> vector;
    srand(34234235);
    for(unsigned int i = 0; i < 3200; i++)
    {
        vector.push_back( rand() % 1000 );
    }

    EXPECT_TRUE( vector.size() == 3200 );

    // test construction and insert(iter, iter) function
    btree_type bt(vector.begin(), vector.end());
    EXPECT_TRUE( bt.size() == 3200 );

    // copy for later use
    btree_type bt2 = bt;

    // empty out the first bt
    srand(34234235);
    for(unsigned int i = 0; i < 3200; i++)
    {
        EXPECT_TRUE(bt.size() == 3200 - i);
        EXPECT_TRUE( bt.erase_one(rand() % 1000) );
        EXPECT_TRUE(bt.size() == 3200 - i - 1);
    }

    EXPECT_TRUE( bt.empty() );

    // copy btree values back to a vector
    std::vector<unsigned int> vector2;
    vector2.assign( bt2.begin(), bt2.end() );

    // afer sorting the vector, the two must be the same
    std::sort(vector.begin(), vector.end());

    EXPECT_TRUE( vector == vector2 );

    // test reverse iterator
    vector2.clear();
    vector2.assign( bt2.rbegin(), bt2.rend() );

    std::reverse(vector.begin(), vector.end());

    btree_type::reverse_iterator ri = bt2.rbegin();
    for(unsigned int i = 0; i < vector2.size(); ++i)
    {
        EXPECT_TRUE( vector[i] == vector2[i] );
        EXPECT_TRUE( vector[i] == *ri );

        ri++;
    }

    EXPECT_TRUE( ri == bt2.rend() );
}



TEST(BtreeTest, test_iterator2)
{
    typedef stx::btree_multimap<unsigned int, unsigned int,
        std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    std::vector< btree_type::value_type > vector;

    srand(34234235);
    for(unsigned int i = 0; i < 3200; i++)
    {
        vector.push_back( btree_type::value_type(rand() % 1000, 0) );
    }

    EXPECT_TRUE( vector.size() == 3200 );

    // test construction and insert(iter, iter) function
    btree_type bt(vector.begin(), vector.end());

    EXPECT_TRUE( bt.size() == 3200 );

    // copy for later use
    btree_type bt2 = bt;

    // empty out the first bt
    srand(34234235);
    for(unsigned int i = 0; i < 3200; i++)
    {
        EXPECT_TRUE(bt.size() == 3200 - i);
        EXPECT_TRUE( bt.erase_one(rand() % 1000) );
        EXPECT_TRUE(bt.size() == 3200 - i - 1);
    }

    EXPECT_TRUE( bt.empty() );

    // copy btree values back to a vector

    std::vector< btree_type::value_type > vector2;
    vector2.assign( bt2.begin(), bt2.end() );

    // afer sorting the vector, the two must be the same
    std::sort(vector.begin(), vector.end());

    EXPECT_TRUE( vector == vector2 );

    // test reverse iterator
    vector2.clear();
    vector2.assign( bt2.rbegin(), bt2.rend() );

    std::reverse(vector.begin(), vector.end());

    btree_type::reverse_iterator ri = bt2.rbegin();
    for(unsigned int i = 0; i < vector2.size(); ++i, ++ri)
    {
        EXPECT_TRUE( vector[i].first == vector2[i].first );
        EXPECT_TRUE( vector[i].first == ri->first );
        EXPECT_TRUE( vector[i].second == ri->second );
    }

    EXPECT_TRUE( ri == bt2.rend() );
}


TEST(BtreeTest, test_erase_iterator1)
{
    typedef stx::btree_multimap<int, int,
    std::less<int>, traits_nodebug<unsigned int> > btree_type;

    btree_type map;

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
// erase_iter()

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
    map.erase(it);
    EXPECT_TRUE( map.size() == mapsize - 1 );
    }
}

EXPECT_TRUE( map.size() == 0 );
}




void test_multi2(const unsigned int insnum, const unsigned int modulo)
{
    typedef stx::btree_multiset<unsigned int,
        std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    btree_type bt;

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

TEST(BtreeTest, test_int_mod)
{
    test_multi2(3200, 10);
    test_multi2(3200, 100);
    test_multi2(3200, 1000);
    test_multi2(3200, 10000);
}

TEST(BtreeTest, test_sequence)
{
    typedef stx::btree_multiset<unsigned int,
        std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    btree_type bt;

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

TEST(BtreeTest, test_relations)
{
    typedef stx::btree_multiset<unsigned int,
        std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    btree_type bt1, bt2;

    srand(34234236);
    for(unsigned int i = 0; i < 320; i++)
    {
        unsigned int key = rand() % 1000;

        bt1.insert(key);
        bt2.insert(key);
    }

    EXPECT_TRUE( bt1 == bt2 );

    bt1.insert(499);
    bt2.insert(500);

    EXPECT_TRUE( bt1 != bt2 );
    EXPECT_TRUE( bt1 < bt2 );
    EXPECT_TRUE( !(bt1 > bt2) );

    bt1.insert(500);
    bt2.insert(499);

    EXPECT_TRUE( bt1 == bt2 );
    EXPECT_TRUE( bt1 <= bt2 );

    // test assignment operator
    btree_type bt3;

    bt3 = bt1;
    EXPECT_TRUE( bt1 == bt3 );
    EXPECT_TRUE( bt1 >= bt3 );

    // test copy constructor
    btree_type bt4 = bt3;

    EXPECT_TRUE( bt1 == bt4 );
}
