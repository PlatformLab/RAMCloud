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

#include <server/index.h>

namespace RAMCloud {

// struct MallocIndexKey

struct MallocIndexKey : public IndexKeyRef {
    explicit MallocIndexKey(const IndexKeyRef& key);

    ~MallocIndexKey();
  private:
    DISALLOW_COPY_AND_ASSIGN(MallocIndexKey);
};

MallocIndexKey::MallocIndexKey(const IndexKeyRef& key) {
    assert(key.len != 0);
    this->len = 0;
    this->buf = malloc(key.len);
    assert(this->buf != NULL);
    memcpy(this->buf, key.buf, key.len);
    this->len = key.len;
}

MallocIndexKey::~MallocIndexKey() {
    if (this->buf != NULL) {
        free(this->buf);
        this->buf = NULL;
    }
    this->len = 0;
}

// struct VarLenKeyComparator

struct VarLenKeyComparator : public IndexKeyComparator {
    VarLenKeyComparator() {}
    bool operator()(IndexKeyRef* const &x, IndexKeyRef* const &y) const;
};

bool VarLenKeyComparator::operator()(IndexKeyRef* const &x,
                                     IndexKeyRef* const &y) const {
    unsigned int i = 0;
    while (i < x->len && i < y->len) {
        char xchar = reinterpret_cast<const char*>(x->buf)[i];
        char ychar = reinterpret_cast<const char*>(y->buf)[i];
        if (xchar < ychar) {
            return true;
        } else if (xchar > ychar) {
            return false;
        }
        i++;
    }
    if (x->len == y->len) {
        return false;
    } else if (x->len < y->len) {
        return true;
    } else {
        return false;
    }
    return memcmp(x->buf, y->buf, x->len) < 0;
}

// struct ScalarKeyComparator

template<class T>
struct ScalarKeyComparator : public IndexKeyComparator {
    ScalarKeyComparator() {}
    bool operator()(IndexKeyRef* const &x, IndexKeyRef* const &y) const;
};

template<class T>
bool ScalarKeyComparator<T>::operator()(IndexKeyRef* const &x,
                                        IndexKeyRef* const &y) const {
    T xval = *reinterpret_cast<T*>(x->buf);
    T yval = *reinterpret_cast<T*>(y->buf);
    return (xval < yval);
}

// struct IndexKeyComparator

IndexKeyComparator*
IndexKeyComparator::Factory(enum RCRPC_INDEX_TYPE type) {
    switch (type) {
        case RCRPC_INDEX_TYPE_SINT8:
            return new ScalarKeyComparator<int8_t>();
        case RCRPC_INDEX_TYPE_UINT8:
            return new ScalarKeyComparator<uint8_t>();
        case RCRPC_INDEX_TYPE_SINT16:
            return new ScalarKeyComparator<int16_t>();
        case RCRPC_INDEX_TYPE_UINT16:
            return new ScalarKeyComparator<uint16_t>();
        case RCRPC_INDEX_TYPE_SINT32:
            return new ScalarKeyComparator<int32_t>();
        case RCRPC_INDEX_TYPE_UINT32:
            return new ScalarKeyComparator<uint32_t>();
        case RCRPC_INDEX_TYPE_SINT64:
            return new ScalarKeyComparator<int64_t>();
        case RCRPC_INDEX_TYPE_UINT64:
            return new ScalarKeyComparator<uint64_t>();
        case RCRPC_INDEX_TYPE_FLOAT32:
            return new ScalarKeyComparator<float>();
        case RCRPC_INDEX_TYPE_FLOAT64:
            return new ScalarKeyComparator<double>();
        case RCRPC_INDEX_TYPE_STRING:
            return new VarLenKeyComparator();
        default:
            throw "bad index type";
    }
}

// struct IndexKeyComparatorWrapper

IndexKeyComparatorWrapper::IndexKeyComparatorWrapper(enum RCRPC_INDEX_TYPE type) :
        type_(type), comparator_(IndexKeyComparator::Factory(type_)) {}

IndexKeyComparatorWrapper::IndexKeyComparatorWrapper(
    const IndexKeyComparatorWrapper& other) :
        type_(other.type_), comparator_(IndexKeyComparator::Factory(type_)) {}

const IndexKeyComparatorWrapper&
IndexKeyComparatorWrapper::operator=(IndexKeyComparatorWrapper& other) {
    delete comparator_;
    comparator_ = NULL;
    type_ = other.type_;
    comparator_ = IndexKeyComparator::Factory(other.type_);
    return *this;
}

IndexKeyComparatorWrapper::~IndexKeyComparatorWrapper() {
    delete comparator_;
    comparator_ = NULL;
}

bool IndexKeyComparatorWrapper::operator()(IndexKeyRef* const &x,
                                           IndexKeyRef* const &y) const {
    return (*comparator_)(x, y);
}

// class STLUniqueRangeIndex

STLUniqueRangeIndex::STLUniqueRangeIndex(enum RCRPC_INDEX_TYPE type) :
        comparator_wrapper_(IndexKeyComparatorWrapper(type)),
        map_(comparator_wrapper_) {
    this->range_queryable = true;
    this->unique = true;
    this->type = type;
}

STLUniqueRangeIndex::~STLUniqueRangeIndex() {
}

void
STLUniqueRangeIndex::Insert(const IndexKeyRef &key, IndexOID value) {
    if (!map_.insert(std::pair<IndexKeyRef*, IndexOID>(new MallocIndexKey(key), value)).second) {
        throw IndexException("Key exists");
    }
}

void
STLUniqueRangeIndex::Remove(const IndexKeyRef &key, IndexOID value) {
    MI map_iter;

    map_iter = map_.find(const_cast<IndexKeyRef*>(&key));
    if (map_iter == map_.end()) {
        throw IndexException("Not found");
    }

    if (map_iter->second == value) {
        map_.erase(map_iter);
        /**
         * map._erase will call ~IndexKeyRef() on map_iter->first, which maps to
         * a no-op. We know map_iter->first is a MallocIndexKey pointer, though,
         * so we need to delete it (thereby also calling ~MallocIndexKey()
         */
        delete static_cast<MallocIndexKey*>(map_iter->first);
    } else {
        throw IndexException("Incorrect value");
    }
}

IndexOID
STLUniqueRangeIndex::Lookup(const IndexKeyRef &key) const {
    CMI map_iter;

    map_iter = map_.find(const_cast<IndexKeyRef*>(&key));
    if (map_iter == map_.end()) {
        throw IndexException("Not found");
    }

    return map_iter->second;
}

unsigned int
STLUniqueRangeIndex::RangeQuery(const RangeQueryArgs *args) const {
    CMI map_iter;
    CMIP range;
    unsigned int count;
    bool more;
    bool varlen;

    assert(args->IsValid());

    range = RangeQueryRange(args);
    map_iter = range.first;
    count = 0;
    more = false;
    varlen = is_varlen_index_type(this->type);

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
                if (!args->result_buf_keys_->AddKey(*map_iter->first, varlen)) {
                    more = true;
                    break;
                }
            }
            if (!args->result_buf_values_->AddOID(map_iter->second)) {
                more = true;
                break;
            }

            ++count;
            ++map_iter;
        }
    }

    if (args->result_more_ != NULL) {
        *args->result_more_ = more;
    }
    return count;
}

STLUniqueRangeIndex::CMIP
STLUniqueRangeIndex::RangeQueryRange(const RangeQueryArgs *args) const {
    CMI start;
    CMI stop;

    if (args->start_present_) {
        if (args->start_inclusive_) {
            start = map_.lower_bound(const_cast<IndexKeyRef*>(args->start_));
        } else {
            start = map_.upper_bound(const_cast<IndexKeyRef*>(args->start_));
        }
        if (start == map_.end()) {
            return CMIP(map_.end(), map_.end());
        }
    } else {
        start = map_.begin();
    }

    if (args->end_present_) {
        if (args->end_inclusive_) {
            stop = map_.upper_bound(const_cast<IndexKeyRef*>(args->end_));
        } else {
            stop = map_.lower_bound(const_cast<IndexKeyRef*>(args->end_));
        }
    } else {
        stop = map_.end();
    }

    if (stop == map_.end() || this->comparator_wrapper_(start->first, stop->first)) {
        return CMIP(start, stop);
    } else {
        return CMIP(map_.end(), map_.end());
    }
}

// class STLMultiRangeIndex

STLMultiRangeIndex::STLMultiRangeIndex(enum RCRPC_INDEX_TYPE type) :
        comparator_wrapper_(IndexKeyComparatorWrapper(type)),
        map_(comparator_wrapper_) {
    this->range_queryable = true;
    this->unique = false;
    this->type = type;
}

STLMultiRangeIndex::~STLMultiRangeIndex() {
}

void
STLMultiRangeIndex::Insert(const IndexKeyRef &key, IndexOID value) {
    std::pair<MI, bool> ret;
    MI map_iter;
    bool inserted;

    LV *vlist;
    LVI vlist_iter;

    ret = map_.insert(std::pair<IndexKeyRef*, LV>(new MallocIndexKey(key), LV(1, value)));
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
STLMultiRangeIndex::Remove(const IndexKeyRef &key, IndexOID value) {
    MI map_iter;
    LV *vlist;
    LVI vlist_iter;

    map_iter = map_.find(const_cast<IndexKeyRef*>(&key));
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
                /**
                 * map._erase will call ~IndexKeyRef() on map_iter->first,
                 * which maps to a no-op. We know map_iter->first is a
                 * MallocIndexKey pointer, though, so we need to delete it
                 * (thereby also calling ~MallocIndexKey()
                 */
                delete static_cast<MallocIndexKey*>(map_iter->first);
            }
            return;
        }
    }
    throw IndexException("Not found");
}

unsigned int
STLMultiRangeIndex::Lookup(const MultiLookupArgs *args) const {
    CMI map_iter;
    const LV *vlist;
    CLVI vlist_iter;
    unsigned int count;
    bool more;

    assert(args->IsValid());

    count = 0;
    more = false;

    map_iter = map_.find(const_cast<IndexKeyRef*>(args->key_));
    if (map_iter != map_.end()) {
        vlist = &map_iter->second;
        vlist_iter = vlist->begin();

        if (args->start_following_present_) {
            while (vlist_iter != vlist->end() &&
                   *vlist_iter < args->start_following_) {
                ++vlist_iter;
            }
        }

        while (vlist_iter != vlist->end()) {
            if (count == args->limit_) {
                more = true;
                break;
            }
            if (!args->result_buf_values_->AddOID(*vlist_iter)) {
                more = true;
                break;
            }
            ++count;
            ++vlist_iter;
        }
    }

    if (args->result_more_ != NULL) {
        *args->result_more_ = more;
    }
    return count;
}

unsigned int
STLMultiRangeIndex::RangeQuery(const RangeQueryArgs *args) const {
    CMI map_iter;
    CMIP range;
    const LV *vlist;
    CLVI vlist_iter;
    unsigned int count;
    bool more;
    bool varlen;

    assert(args->IsValid());

    range = RangeQueryRange(args);
    map_iter = range.first;
    count = 0;
    more = false;
    varlen = is_varlen_index_type(this->type);

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
                    if (!args->result_buf_keys_->AddKey(*map_iter->first, varlen)) {
                        more = true;
                        goto OOM;
                    }
                }
                if (!args->result_buf_values_->AddOID(*vlist_iter)) {
                    more = true;
                    goto OOM;
                }

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

STLMultiRangeIndex::CMIP
STLMultiRangeIndex::RangeQueryRange(const RangeQueryArgs *args) const {
    CMI start;
    CMI stop;

    if (args->start_present_) {
        if (args->start_inclusive_) {
            start = map_.lower_bound(const_cast<IndexKeyRef*>(args->start_));
        } else {
            start = map_.upper_bound(const_cast<IndexKeyRef*>(args->start_));
        }
        if (start == map_.end()) {
            return CMIP(map_.end(), map_.end());
        }
    } else {
        start = map_.begin();
    }

    if (args->end_present_) {
        if (args->end_inclusive_) {
            stop = map_.upper_bound(const_cast<IndexKeyRef*>(args->end_));
        } else {
            stop = map_.lower_bound(const_cast<IndexKeyRef*>(args->end_));
        }
    } else {
        stop = map_.end();
    }

    if (stop == map_.end() || this->comparator_wrapper_(start->first, stop->first)) {
        return CMIP(start, stop);
    } else {
        return CMIP(map_.end(), map_.end());
    }
}

}
