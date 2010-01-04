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
#include <shared/rcrpc.h>

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <list>
#include <map>
#include <string>

namespace RAMCloud {

static const bool index_tracing = true;

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

struct IndexKeyRef {
  public:
    IndexKeyRef(void *buf, uint64_t len) : buf(buf), len(len) {}

    // TODO(ongaro): this is a little evil
    IndexKeyRef(const void *buf, uint64_t len) : buf(const_cast<void*>(buf)), len(len) {}

    bool operator==(const IndexKeyRef &other) const {
        return len == other.len &&
               memcmp(buf, other.buf, len) == 0;
    }

    void *buf;
    uint64_t len; /* in bytes */

  protected:
    IndexKeyRef() : buf(NULL), len(0) {}

  private:
    DISALLOW_COPY_AND_ASSIGN(IndexKeyRef);
};

struct IndexKeysRef {
  public:
    IndexKeysRef(char *buf, uint64_t len) : buf(buf), total(len), used(0),
        type_(static_cast<enum RCRPC_INDEX_TYPE>(-1)) {}

    void SetType(enum RCRPC_INDEX_TYPE type) {
        type_ = type;
    }

    bool AddKey(const IndexKeyRef &key) {
        switch (type_) {
            case RCRPC_INDEX_TYPE_SINT8:
            case RCRPC_INDEX_TYPE_UINT8:
            case RCRPC_INDEX_TYPE_SINT16:
            case RCRPC_INDEX_TYPE_UINT16:
            case RCRPC_INDEX_TYPE_SINT32:
            case RCRPC_INDEX_TYPE_UINT32:
            case RCRPC_INDEX_TYPE_SINT64:
            case RCRPC_INDEX_TYPE_UINT64:
            case RCRPC_INDEX_TYPE_FLOAT32:
            case RCRPC_INDEX_TYPE_FLOAT64:
                return AddFixedLenKey(key);
            case RCRPC_INDEX_TYPE_BYTES8:
                assert(is_varlen_index_type(type_));
                return AddVarLenKey<uint8_t>(key);
            case RCRPC_INDEX_TYPE_BYTES16:
                assert(is_varlen_index_type(type_));
                return AddVarLenKey<uint16_t>(key);
            case RCRPC_INDEX_TYPE_BYTES32:
                assert(is_varlen_index_type(type_));
                return AddVarLenKey<uint32_t>(key);
            case RCRPC_INDEX_TYPE_BYTES64:
                assert(is_varlen_index_type(type_));
                return AddVarLenKey<uint64_t>(key);
            default:
                throw "bad index type";
        }
    }

    char *buf;
    uint64_t total; /* total number of bytes that can be stored in buf */
    uint64_t used;  /* number of bytes that have been stored in buf */

  private:

    bool AddFixedLenKey(const IndexKeyRef &key) {
        if (used + key.len <= total) {
            memcpy(buf + used, key.buf, key.len);
            used += key.len;
            return true;
        } else {
            return false;
        }
    }

    template <class LT>
    bool AddVarLenKey(const IndexKeyRef &key) {
        if (used + sizeof(LT) + key.len <= total) {
            assert(key.len <= static_cast<LT>(-1));
            *reinterpret_cast<LT*>(buf + used) = static_cast<LT>(key.len);
            memcpy(buf + used + sizeof(LT), key.buf, key.len);
            used += sizeof(LT) + key.len;
            return true;
        } else {
            return false;
        }
    }

    enum RCRPC_INDEX_TYPE type_;
    DISALLOW_COPY_AND_ASSIGN(IndexKeysRef);
};


typedef uint64_t IndexOID;

struct IndexOIDsRef {
  public:
    IndexOIDsRef(uint64_t *buf, uint64_t len) : buf(buf), total(len), used(0) {}
    IndexOIDsRef() : buf(NULL), total(0), used(0) {}

    bool AddOID(IndexOID oid) {
        if (used + 1 <= total) {
            buf[used++] = oid;
            return true;
        } else {
            return false;
        }
    }

    uint64_t *buf;
    uint64_t total; /* total number of OIDs that can be stored in buf */
    uint64_t used;  /* number of OIDs that have been stored in buf */

  private:
    DISALLOW_COPY_AND_ASSIGN(IndexOIDsRef);
};

struct MultiLookupArgs {

  public:

    MultiLookupArgs() : key_(NULL),
                        key_present_(false),
                        start_following_(),
                        start_following_present_(false),
                        limit_(static_cast<unsigned int>(-1)),
                        result_buf_values_(),
                        result_more_(NULL) {
    }

    /* required */
    void
    setKey(const IndexKeyRef &key) {
        key_ = &key;
        key_present_ = true;
    }

    /* optional */
    void
    setStartFollowing(IndexOID value) {
        start_following_present_ = true;
        start_following_ = value;
    }

    /* required */
    void
    setLimit(unsigned int limit) {
        limit_ = limit;
    }

    /* required */
    void
    setResultBuf(IndexOIDsRef &values) {
        result_buf_values_ = &values;
    }

    /* optional */
    void
    setResultMore(bool *more) {
        result_more_ = more;
    }

    ////////////////////

    bool
    IsValid() const {
        if (!key_present_) {
            return false;
        }
        if (limit_ == static_cast<unsigned int>(-1)) {
            return false;
        }
        if (result_buf_values_ == NULL) {
            return false;
        }
        return true;
    }

    IndexKeyRef const *key_;
    bool key_present_;

    IndexOID start_following_;
    bool start_following_present_;

    unsigned int limit_;

    IndexOIDsRef *result_buf_values_;

    bool *result_more_;

    DISALLOW_COPY_AND_ASSIGN(MultiLookupArgs);
};

struct RangeQueryArgs {

  public:

    RangeQueryArgs() : start_(NULL),
                       start_present_(false),
                       start_inclusive_(false),
                       end_(NULL),
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
    setKeyStart(const IndexKeyRef &start, bool inclusive) {
        start_ = &start;
        start_present_ = true;
        start_inclusive_ = inclusive;
    }

    /* optional */
    void
    setKeyEnd(const IndexKeyRef &end, bool inclusive) {
        end_ = &end;
        end_present_ = true;
        end_inclusive_ = inclusive;
    }

    /* optional */
    void
    setStartFollowing(IndexOID value) {
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
    setResultBuf(IndexKeysRef &keys, IndexOIDsRef &values) {
        result_buf_keys_ = &keys;
        result_buf_values_ = &values;
    }

    void
    setResultBuf(IndexOIDsRef &values) {
        result_buf_values_ = &values;
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

    IndexKeyRef const *start_;
    bool start_present_;
    bool start_inclusive_;

    IndexKeyRef const *end_;
    bool end_present_;
    bool end_inclusive_;

    IndexOID start_following_;
    bool start_following_present_;

    unsigned int limit_;

    IndexKeysRef *result_buf_keys_;
    IndexOIDsRef *result_buf_values_;

    bool *result_more_;

    DISALLOW_COPY_AND_ASSIGN(RangeQueryArgs);
};

class Index {
  public:
    Index() : range_queryable(false), unique(false), type(RCRPC_INDEX_TYPE_UINT8) {}
    bool range_queryable;
    bool unique;
    enum RCRPC_INDEX_TYPE type; // TODO(ongaro) shouldn't depend on rcrpc.h
    virtual ~Index(){}
};

class UniqueIndex : public Index {
  public:
    virtual void Insert(const IndexKeyRef& key, IndexOID value) = 0; // throws IndexException if key exists
    virtual void Remove(const IndexKeyRef& key, IndexOID value) = 0; // throws IndexException if key not found or if (key, oid) not found
    virtual IndexOID Lookup(const IndexKeyRef& key) const = 0;       // throws IndexException if key not found
    virtual ~UniqueIndex(){}
};

class MultiIndex : public Index {
  public:
    virtual void Insert(const IndexKeyRef& key, IndexOID value) = 0;
    virtual void Remove(const IndexKeyRef& key, IndexOID value) = 0;  // throws IndexException if (key, oid) not found
    virtual unsigned int Lookup(const MultiLookupArgs *args) const = 0;
    virtual ~MultiIndex(){}
};

class UniqueRangeIndex : public UniqueIndex {
  public:
    virtual unsigned int
    RangeQuery(const RangeQueryArgs *args) const = 0;
  private:
};

class MultiRangeIndex : public MultiIndex {
  public:
    virtual unsigned int
    RangeQuery(const RangeQueryArgs *args) const = 0;
  private:
};

struct IndexKeyComparator {
    virtual ~IndexKeyComparator() {}
    virtual bool operator()(IndexKeyRef* const &x, IndexKeyRef* const &y) const = 0;
    static IndexKeyComparator* Factory(enum RCRPC_INDEX_TYPE type);
};

class IndexKeyComparatorWrapper {
  public:
    IndexKeyComparatorWrapper(enum RCRPC_INDEX_TYPE type);

    IndexKeyComparatorWrapper(const IndexKeyComparatorWrapper& other);

    ~IndexKeyComparatorWrapper();

    const IndexKeyComparatorWrapper&
    operator=(IndexKeyComparatorWrapper& other);

    bool operator()(IndexKeyRef* const &x, IndexKeyRef* const &y) const;

  private:
    enum RCRPC_INDEX_TYPE type_;
    IndexKeyComparator *comparator_;
};

class STLUniqueRangeIndex : public UniqueRangeIndex {

  public:
    STLUniqueRangeIndex(enum RCRPC_INDEX_TYPE type);
    ~STLUniqueRangeIndex();
    void Insert(const IndexKeyRef& key, IndexOID value);
    void Remove(const IndexKeyRef& key, IndexOID value);
    IndexOID Lookup(const IndexKeyRef& key) const;
    unsigned int RangeQuery(const RangeQueryArgs *args) const; 

  private:

    typedef std::map<IndexKeyRef*, IndexOID, IndexKeyComparatorWrapper> M;
    typedef M::iterator MI;
    typedef M::const_iterator CMI;

    typedef std::pair<MI,  MI>  MIP;
    typedef std::pair<CMI, CMI> CMIP;

    CMIP RangeQueryRange(const RangeQueryArgs *args) const;

    IndexKeyComparatorWrapper comparator_wrapper_;
    M map_;

    DISALLOW_COPY_AND_ASSIGN(STLUniqueRangeIndex);
};


class STLMultiRangeIndex : public MultiRangeIndex {

  public:
    STLMultiRangeIndex(enum RCRPC_INDEX_TYPE type);
    ~STLMultiRangeIndex();
    void Insert(const IndexKeyRef &key, IndexOID value);
    void Remove(const IndexKeyRef &key, IndexOID value);
    unsigned int Lookup(const MultiLookupArgs *args) const;
    unsigned int RangeQuery(const RangeQueryArgs *args) const;

  private:

    typedef std::list<IndexOID> LV;
    typedef LV::iterator LVI;
    typedef LV::const_iterator CLVI;

    typedef std::map<IndexKeyRef*, LV, IndexKeyComparatorWrapper> M;
    typedef M::iterator MI;
    typedef M::const_iterator CMI;

    typedef std::pair<MI,  MI>  MIP;
    typedef std::pair<CMI, CMI> CMIP;

    CMIP RangeQueryRange(const RangeQueryArgs *args) const;

    IndexKeyComparatorWrapper comparator_wrapper_;
    M map_;

    DISALLOW_COPY_AND_ASSIGN(STLMultiRangeIndex);
};


} // namespace RAMCloud

#endif
