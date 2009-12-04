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

#include <list>
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

  typedef std::map<K, V> M;
  typedef typename M::iterator MI;
  typedef typename M::const_iterator CMI;

  typedef std::pair<MI,  MI>  MIP;
  typedef std::pair<CMI, CMI> CMIP;

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
        CMI map_iter;

        map_iter = map_.find(key);
        if (map_iter == map_.end()) {
            throw IndexException("Not found");
        }

        return map_iter->second;
    }

    unsigned int
    RangeQuery(K key_start, bool start_inclusive,
               K key_end,   bool end_inclusive,
               unsigned int limit,
               V *values) const {
        CMI map_iter;
        CMIP range;
        unsigned int count;

        range = RangeQueryRange(key_start, start_inclusive,
                                key_end,   end_inclusive);
        for (count = 0,       map_iter = range.first;
             count < limit && map_iter != range.second;
             ++count,         ++map_iter) {

            values[count] = map_iter->second;
        }
        return count;
    }

    unsigned int
    RangeQuery(K key_start, bool start_inclusive,
               K key_end,   bool end_inclusive,
               unsigned int limit,
               K *keys,
               V *values) const {
        CMI map_iter;
        CMIP range;
        unsigned int count;

        range = RangeQueryRange(key_start, start_inclusive,
                                key_end,   end_inclusive);
        for (count = 0,       map_iter = range.first;
             count < limit && map_iter != range.second;
             ++count,         ++map_iter) {

            keys[count] = map_iter->first;
            values[count] = map_iter->second;
        }
        return count;
    }

  private:

    CMIP
    RangeQueryRange(K key_start, bool start_inclusive,
                    K key_end,   bool end_inclusive) const
    {
        CMI start;
        CMI stop;

        if (start_inclusive) {
            start = map_.lower_bound(key_start);
        } else {
            start = map_.upper_bound(key_start);
        }
        if (start == map_.end()) {
            return CMIP(map_.end(), map_.end());
        }

        if (end_inclusive) {
            stop = map_.upper_bound(key_end);
        } else {
            stop = map_.lower_bound(key_end);
        }

        if (stop == map_.end() || start->first <= stop->first) {
            return CMIP(start, stop);
        } else {
            return CMIP(map_.end(), map_.end());
        }
    }

    M map_;

    DISALLOW_COPY_AND_ASSIGN(STLUniqueRangeIndex);
};

template<class K, class V>
class STLMultiRangeIndex : public MultiRangeIndex<K, V> {

  typedef std::list<V> LV;
  typedef typename LV::iterator LVI;
  typedef typename LV::const_iterator CLVI;

  typedef std::map<K, LV> M;
  typedef typename M::iterator MI;
  typedef typename M::const_iterator CMI;

  typedef std::pair<MI,  MI>  MIP;
  typedef std::pair<CMI, CMI> CMIP;

  public:
    STLMultiRangeIndex() : map_() {
    }

    ~STLMultiRangeIndex() {
    }

    void
    Insert(K key, V value) {
        std::pair<MI, bool> ret;
        MI map_iter;
        bool inserted;

        LV *vlist;
        LVI vlist_iter;

        ret = map_.insert(std::pair<K, LV>(key, LV(1, value)));
        map_iter = ret.first;
        inserted = ret.second;

        if (!inserted) {
            vlist = &map_iter->second;
            for (vlist_iter = vlist->begin();
                 vlist_iter != vlist->end();
                 ++vlist_iter) {

                if (*vlist_iter > value) {
                    break;
                }
            }
            vlist->insert(vlist_iter, value);
        }
    }

    void
    Remove(K key, V value) {
        MI map_iter;
        LV *vlist;
        LVI vlist_iter;

        map_iter = map_.find(key);
        if (map_iter == map_.end()) {
            throw IndexException("Not found");
        }
        vlist = &map_iter->second;

        for (vlist_iter = vlist->begin();
             vlist_iter != vlist->end();
             ++vlist_iter) {

            if (*vlist_iter == value) {
                vlist->erase(vlist_iter);
                if (vlist->size() == 0) {
                    map_.erase(map_iter);
                }
                return;
            }
        }
        throw IndexException("Not found");
    }

    unsigned int
    Lookup(K key, unsigned int limit, V *values) const {
        CMI map_iter;
        const LV *vlist;
        CLVI vlist_iter;
        unsigned int count;

        map_iter = map_.find(key);
        if (map_iter == map_.end()) {
            return 0;
        }
        vlist = &map_iter->second;

        for (count = 0,       vlist_iter = vlist->begin();
             count < limit && vlist_iter != vlist->end();
             ++count,         ++vlist_iter) {

            values[count] = *vlist_iter;
        }
        return count;
    }

    unsigned int
    RangeQuery(K key_start, bool start_inclusive,
               K key_end,   bool end_inclusive,
               unsigned int limit,
               V *values) const {
        CMI map_iter;
        CMIP range;
        const LV *vlist;
        CLVI vlist_iter;
        unsigned int count;

        range = RangeQueryRange(key_start, start_inclusive,
                                key_end,   end_inclusive);
        count = 0;
        for (map_iter = range.first; map_iter != range.second; ++map_iter) {
            vlist = &map_iter->second;
            for (                 vlist_iter = vlist->begin();
                 count < limit && vlist_iter != vlist->end();
                 ++count,         ++vlist_iter) {

                values[count] = *vlist_iter;
            }
        }
        return count;
    }

    unsigned int
    RangeQuery(K key_start, bool start_inclusive,
               K key_end,   bool end_inclusive,
               unsigned int limit,
               K *keys,
               V *values) const {
        CMI map_iter;
        CMIP range;
        const LV *vlist;
        CLVI vlist_iter;
        unsigned int count;

        range = RangeQueryRange(key_start, start_inclusive,
                                key_end,   end_inclusive);
        count = 0;
        for (map_iter = range.first; map_iter != range.second; ++map_iter) {
            vlist = &map_iter->second;
            for (                 vlist_iter = vlist->begin();
                 count < limit && vlist_iter != vlist->end();
                 ++count,         ++vlist_iter) {

                values[count] = *vlist_iter;
                keys[count] = map_iter->first;
            }
        }
        return count;
    }

  private:

    CMIP
    RangeQueryRange(K key_start, bool start_inclusive,
                    K key_end,   bool end_inclusive) const
    {
        CMI start;
        CMI stop;

        if (start_inclusive) {
            start = map_.lower_bound(key_start);
        } else {
            start = map_.upper_bound(key_start);
        }
        if (start == map_.end()) {
            return CMIP(map_.end(), map_.end());
        }

        if (end_inclusive) {
            stop = map_.upper_bound(key_end);
        } else {
            stop = map_.lower_bound(key_end);
        }

        if (stop == map_.end() || start->first <= stop->first) {
            return CMIP(start, stop);
        } else {
            return CMIP(map_.end(), map_.end());
        }
    }

    M map_;

    DISALLOW_COPY_AND_ASSIGN(STLMultiRangeIndex);
};


} // namespace RAMCloud

#endif
