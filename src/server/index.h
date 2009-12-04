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

#ifndef RAMCLOUD_SERVER_INDEX_H
#define RAMCLOUD_SERVER_INDEX_H

#include <config.h>
#include <shared/common.h>

#include <map>
#include <string>

namespace RAMCloud {

struct IndexException {
    explicit IndexException(std::string msg)
            : message(msg) {}
    IndexException &operator=(const IndexException &e) {
        if (&e == this)
            return *this;
        message = e.message;
        return *this;
    }
    virtual ~IndexException() {}
    std::string message;
};

template<class K, class V>
class Index {
  public:
    virtual ~Index(){}
};

template<class K, class V>
class UniqueIndex : public Index<K, V> {
  public:
    virtual void Insert(K key, V value) = 0; // throws IndexException if key exists
    virtual void Remove(K key, V value) = 0; // throws IndexException if key not found or if (key, value) not found
    virtual V Lookup(K key) const = 0;       // throws IndexException if key not found
    virtual ~UniqueIndex(){}
  private:
};

template<class K, class V>
class MultiIndex : public Index<K, V> {
  public:
    virtual void Insert(K key, V value) = 0;
    virtual void Remove(K key, V value) = 0; // throws IndexException if (key, value) not found
    virtual unsigned int Lookup(K key, unsigned int limit, V *values) const = 0;
    virtual ~MultiIndex(){}
  private:
};

template<class K, class V>
class UniqueRangeIndex : public UniqueIndex<K, V> {
  public:

    virtual unsigned int
    RangeQuery(K key_start, bool start_inclusive,
               K key_end,   bool end_inclusive,
               unsigned int limit,
               K *keys,
               V *values) const = 0;

    virtual unsigned int
    RangeQuery(K key_start, bool start_inclusive,
               K key_end,   bool end_inclusive,
               unsigned int limit,
               V *values) const = 0;

  private:
};

template<class K, class V>
class MultiRangeIndex : public MultiIndex<K, V> {
  public:

    virtual unsigned int
    RangeQuery(K key_start, bool start_inclusive,
               K key_end,   bool end_inclusive,
               unsigned int limit,
               K *keys,
               V *values) const = 0;

    virtual unsigned int
    RangeQuery(K key_start, bool start_inclusive,
               K key_end,   bool end_inclusive,
               unsigned int limit,
               V *values) const = 0;

  private:
};

template<class K, class V>
class STLUniqueRangeIndex : public UniqueRangeIndex<K, V> {

  typedef std::map<K, V> mkv;
  typedef std::pair<typename mkv::iterator,
                    typename mkv::iterator> mipair;
  typedef std::pair<typename mkv::const_iterator,
                    typename mkv::const_iterator> micpair;

  public:
    STLUniqueRangeIndex() : map_() {
    }

    ~STLUniqueRangeIndex() {
    }

    void
    Insert(K key, V value) {
        if (!map_.insert(std::pair<K, V>(key, value)).second) {
            throw IndexException("Key exists");
        }
    }

    void
    Remove(K key, V value) {
        if (Lookup(key) == value) {
            map_.erase(key);
        } else {
            throw IndexException("Incorrect value");
        }
    }

    V
    Lookup(K key) const {
        typename mkv::const_iterator i = map_.find(key);
        if (i != map_.end()) {
            return i->second;
        } else {
            throw IndexException("Not found");
        }
    }

    unsigned int
    RangeQuery(K key_start, bool start_inclusive,
               K key_end,   bool end_inclusive,
               unsigned int limit,
               V *values) const {
        typename mkv::const_iterator i;
        micpair range;
        unsigned int count;

        range = RangeQueryRange(key_start, start_inclusive,
                                key_end,   end_inclusive);
        for (count = 0, i = range.first;
             count < limit && i != range.second;
             ++count, ++i) {

            values[count] = i->second;
        }
        return count;
    }

    unsigned int
    RangeQuery(K key_start, bool start_inclusive,
               K key_end,   bool end_inclusive,
               unsigned int limit,
               K *keys,
               V *values) const {
        typename mkv::const_iterator i;
        micpair range;
        unsigned int count;

        range = RangeQueryRange(key_start, start_inclusive,
                                key_end,   end_inclusive);
        for (count = 0, i = range.first;
             count < limit && i != range.second;
             ++count, ++i) {

            keys[count] = i->first;
            values[count] = i->second;
        }
        return count;
    }

  private:

    micpair
    RangeQueryRange(K key_start, bool start_inclusive,
                    K key_end,   bool end_inclusive) const
    {
        typename mkv::const_iterator start;
        typename mkv::const_iterator stop;

        if (start_inclusive) {
            start = map_.lower_bound(key_start);
        } else {
            start = map_.upper_bound(key_start);
        }
        if (start == map_.end()) {
            return micpair(map_.end(), map_.end());
        }

        if (end_inclusive) {
            stop = map_.upper_bound(key_end);
        } else {
            stop = map_.lower_bound(key_end);
        }

        if (stop == map_.end() || start->first <= stop->first) {
            return micpair(start, stop);
        } else {
            return micpair(map_.end(), map_.end());
        }
    }

    mkv map_;

    DISALLOW_COPY_AND_ASSIGN(STLUniqueRangeIndex);
};

template<class K, class V>
class STLMultiRangeIndex : public MultiRangeIndex<K, V> {

  typedef std::multimap<K, V> mmkv;
  typedef std::pair<typename mmkv::iterator,
                    typename mmkv::iterator> mmipair;
  typedef std::pair<typename mmkv::const_iterator,
                    typename mmkv::const_iterator> mmicpair;

  public:
    STLMultiRangeIndex() : map_() {
    }

    ~STLMultiRangeIndex() {
    }

    void
    Insert(K key, V value) {
        map_.insert(std::pair<K, V>(key, value));
    }

    void
    Remove(K key, V value) {
        typename mmkv::iterator i;
        mmipair range;

        range = map_.equal_range(key);
        for (i = range.first; i != range.second; ++i) {
            if (i->second == value) {
                map_.erase(i);
                return;
            }
        }
        throw IndexException("Not found");
    }

    unsigned int
    Lookup(K key, unsigned int limit, V *values) const {
        typename mmkv::const_iterator i;
        mmicpair range;
        unsigned int count;

        range = map_.equal_range(key);
        for (count = 0, i = range.first;
             count < limit && i != range.second;
             ++count, ++i) {

            *values = i->second;
            ++values;
        }
        return count;
    }

    unsigned int
    RangeQuery(K key_start, bool start_inclusive,
               K key_end,   bool end_inclusive,
               unsigned int limit,
               V *values) const {
        typename mmkv::const_iterator i;
        mmicpair range;
        unsigned int count;

        range = RangeQueryRange(key_start, start_inclusive,
                                key_end,   end_inclusive);
        for (count = 0, i = range.first;
             count < limit && i != range.second;
             ++count, ++i) {

            values[count] = i->second;
        }
        return count;
    }

    unsigned int
    RangeQuery(K key_start, bool start_inclusive,
               K key_end,   bool end_inclusive,
               unsigned int limit,
               K *keys,
               V *values) const {
        typename mmkv::const_iterator i;
        mmicpair range;
        unsigned int count;

        range = RangeQueryRange(key_start, start_inclusive,
                                key_end,   end_inclusive);
        for (count = 0, i = range.first;
             count < limit && i != range.second;
             ++count, ++i) {

            keys[count] = i->first;
            values[count] = i->second;
        }
        return count;
    }

  private:

    mmicpair
    RangeQueryRange(K key_start, bool start_inclusive,
                    K key_end,   bool end_inclusive) const
    {
        typename mmkv::const_iterator start;
        typename mmkv::const_iterator stop;

        if (start_inclusive) {
            start = map_.lower_bound(key_start);
        } else {
            start = map_.upper_bound(key_start);
        }
        if (start == map_.end()) {
            return mmicpair(map_.end(), map_.end());
        }

        if (end_inclusive) {
            stop = map_.upper_bound(key_end);
        } else {
            stop = map_.lower_bound(key_end);
        }

        if (stop == map_.end() || start->first <= stop->first) {
            return mmicpair(start, stop);
        } else {
            return mmicpair(map_.end(), map_.end());
        }
    }

    mmkv map_;

    DISALLOW_COPY_AND_ASSIGN(STLMultiRangeIndex);
};


} // namespace RAMCloud

#endif
