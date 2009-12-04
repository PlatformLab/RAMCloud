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

#include <assert.h>

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
struct RangeQueryArgs {

  public:

    RangeQueryArgs() : start_(),
                       start_present_(false),
                       start_inclusive_(false),
                       end_(),
                       end_present_(false),
                       end_inclusive_(false),
                       start_following_(),
                       start_following_present_(false),
                       limit_(static_cast<unsigned int>(-1)),
                       result_buf_keys_(NULL),
                       result_buf_values_(NULL),
                       result_more_(NULL) {
    }

    /* optional */
    void
    setKeyStart(K start, bool inclusive) {
        start_ = start;
        start_present_ = true;
        start_inclusive_ = inclusive;
    }

    /* optional */
    void
    setKeyEnd(K end, bool inclusive) {
        end_ = end;
        end_present_ = true;
        end_inclusive_ = inclusive;
    }

    /* optional */
    void
    setStartFollowing(V value) {
        start_following_present_ = true;
        start_following_ = value;
    }

    /* required */
    void
    setLimit(unsigned int limit) {
        limit_ = limit;
    }

    /* pick one of the following */
    void
    setResultBuf(K *keys, V *values) {
        result_buf_keys_ = keys;
        result_buf_values_ = values;
    }

    void
    setResultBuf(V *values) {
        result_buf_values_ = values;
    }

    /* optional */
    void
    setResultMore(bool *more) {
        result_more_ = more;
    }

    ////////////////////

    bool
    IsValid() const {
        if (limit_ == static_cast<unsigned int>(-1)) {
            return false;
        }
        if (result_buf_values_ == NULL) {
            return false;
        }
        if (start_following_present_) {
            if (!start_present_ || !start_inclusive_) {
                return false;
            }
        }
        return true;
    }

    K start_;
    bool start_present_;
    bool start_inclusive_;

    K end_;
    bool end_present_;
    bool end_inclusive_;

    V start_following_;
    bool start_following_present_;

    unsigned int limit_;

    K *result_buf_keys_;
    V *result_buf_values_;

    bool *result_more_;
};

template<class K, class V>
class UniqueRangeIndex : public UniqueIndex<K, V> {
  public:
    virtual unsigned int
    RangeQuery(const RangeQueryArgs<K, V> *args) const = 0;
  private:
};

template<class K, class V>
class MultiRangeIndex : public MultiIndex<K, V> {
  public:
    virtual unsigned int
    RangeQuery(const RangeQueryArgs<K, V> *args) const = 0;
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
    RangeQuery(const RangeQueryArgs<K, V> *args) const {
        CMI map_iter;
        CMIP range;
        unsigned int count;
        bool more;

        assert(args->IsValid());

        range = RangeQueryRange(args);
        map_iter = range.first;
        count = 0;
        more = false;

        if (map_iter != range.second) {
            if (args->start_following_present_) {
                // this flag doesn't make that much sense for unique indexes
                // start following present implies both start present and
                // start inclusive
                if (map_iter->first == args->start_ &&
                    map_iter->second <= args->start_following_) {
                    ++map_iter;
                }
            }

            // stream result from map_iter through range.second
            while (map_iter != range.second) {
                if (count == args->limit_) {
                    more = true;
                    break;
                }
                if (args->result_buf_keys_ != NULL) {
                    args->result_buf_keys_[count] = map_iter->first;
                }
                args->result_buf_values_[count] = map_iter->second;

                ++count;
                ++map_iter;
            }
        }

        if (args->result_more_ != NULL) {
            *args->result_more_ = more;
        }
        return count;
    }

  private:

    CMIP
    RangeQueryRange(const RangeQueryArgs<K, V> *args) const {
        CMI start;
        CMI stop;

        if (args->start_present_) {
            if (args->start_inclusive_) {
                start = map_.lower_bound(args->start_);
            } else {
                start = map_.upper_bound(args->start_);
            }
            if (start == map_.end()) {
                return CMIP(map_.end(), map_.end());
            }
        } else {
            start = map_.begin();
        }

        if (args->end_present_) {
            if (args->end_inclusive_) {
                stop = map_.upper_bound(args->end_);
            } else {
                stop = map_.lower_bound(args->end_);
            }
        } else {
            stop = map_.end();
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
    RangeQuery(const RangeQueryArgs<K, V> *args) const {
        CMI map_iter;
        CMIP range;
        const LV *vlist;
        CLVI vlist_iter;
        unsigned int count;
        bool more;

        assert(args->IsValid());

        range = RangeQueryRange(args);
        map_iter = range.first;
        count = 0;
        more = false;

        if (map_iter != map_.end()) {
            vlist = &map_iter->second;
            vlist_iter = vlist->begin();

            if (args->start_following_present_) {
                // start following present implies both start present and
                // start inclusive
                if (map_iter->first == args->start_) {
                    while (vlist_iter != vlist->end() &&
                           *vlist_iter <= args->start_following_) {
                        ++vlist_iter;
                    }
                }
            }

            // stream result from map_iter, vlist_iter
            // through map_iter=range.second, vlist_iter=map_iter->second.end()
            while (map_iter != range.second) {
                while (vlist_iter != vlist->end()) {
                    if (count == args->limit_) {
                        more = true;
                        goto OOM;
                    }

                    if (args->result_buf_keys_ != NULL) {
                        args->result_buf_keys_[count] = map_iter->first;
                    }
                    args->result_buf_values_[count] = *vlist_iter;

                    ++count;
                    ++vlist_iter;
                }

                ++map_iter;
                vlist = &map_iter->second;
                vlist_iter = vlist->begin();
            }
        OOM:
            /*pass*/;
        }

        if (args->result_more_ != NULL) {
            *args->result_more_ = more;
        }
        return count;
    }

  private:
    CMIP
    RangeQueryRange(const RangeQueryArgs<K, V> *args) const {
        CMI start;
        CMI stop;

        if (args->start_present_) {
            if (args->start_inclusive_) {
                start = map_.lower_bound(args->start_);
            } else {
                start = map_.upper_bound(args->start_);
            }
            if (start == map_.end()) {
                return CMIP(map_.end(), map_.end());
            }
        } else {
            start = map_.begin();
        }

        if (args->end_present_) {
            if (args->end_inclusive_) {
                stop = map_.upper_bound(args->end_);
            } else {
                stop = map_.lower_bound(args->end_);
            }
        } else {
            stop = map_.end();
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
