/* Copyright (c) 2010 Stanford University
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

#ifndef RAMCLOUD_OUSTERCOUNT_H
#define RAMCLOUD_OUSTERCOUNT_H

#include <vector>
#include "Common.h"

namespace RAMCloud {

struct Partition {
    uint64_t firstKey;
    uint64_t lastKey;
};
typedef std::vector<Partition> PartitionList;

class Oustercount {
  public:
    Oustercount();
    ~Oustercount();

    // public methods
    void           addObject(uint64_t key, uint32_t bytes, LogTime time);
    void           removeObject(uint64_t key, uint32_t bytes, LogTime time);
    PartitionList* getPartitions(uint64_t maxPartitionBytes,
                                 uint64_t maxPartitionObjects);

  private:
    static const int      BITS_PER_LEVEL = 8;
    static const uint64_t BUCKET_SPLIT_BYTES = 8 * 1024 * 1024;
    static const uint64_t BUCKET_SPLIT_OBJS  = BUCKET_SPLIT_BYTES / 100;
    static const uint64_t BUCKET_MERGE_BYTES = BUCKET_SPLIT_BYTES * 0.75;
    static const uint64_t BUCKET_MERGE_OBJS  = BUCKET_SPLIT_OBJS  * 0.75;

    class PartitionCollector {
      public:
        PartitionCollector(uint64_t maxPartitionBytes,
                           uint64_t maxPartitionObjects,
                           PartitionList* partitions);
        void addRangeLeaf(uint64_t firstKey, uint64_t lastKey,
                          uint64_t rangeBytes, uint64_t rangeObjects);
        void addRangeNonLeaf(uint64_t rangeBytes, uint64_t rangeObjects);
        void done();

      private:
        PartitionList* partitions;

        void pushCurrentTally(uint64_t lastKey);

        // current tally
        uint64_t maxPartitionBytes;
        uint64_t maxPartitionObjects;
        uint64_t nextFirstKey;
        uint64_t currentFirstKey;
        uint64_t currentTotalBytes;
        uint64_t currentTotalObjects;
        uint64_t globalTotalBytes;
        uint64_t globalTotalObjects;
        bool     isDone;

        DISALLOW_COPY_AND_ASSIGN(PartitionCollector);
    };

    // forward decl
    class Subrange;

    struct Bucket {
        Subrange *child;
        uint64_t  totalBytes;
        uint64_t  totalObjects;
    };

    class Subrange {
      public:
        class BucketHandle {
          public:
            BucketHandle(Subrange *subrange, int bucketIndex);
            Subrange* getSubrange();
            Bucket*   getBucket();
            uint64_t  getFirstKey();
            uint64_t  getLastKey();

          private:
            Subrange* subrange;
            int       bucketIndex;
        };

        Subrange(BucketHandle parent, uint64_t firstKey, uint64_t lastKey,
                 LogTime time);
       ~Subrange();

        void         addObject(BucketHandle bh, uint64_t key, uint32_t bytes,
                               LogTime time);
        bool         removeObject(BucketHandle bh, uint64_t key, uint32_t bytes,
                                  LogTime time);
        BucketHandle findBucket(uint64_t key, LogTime *time = NULL);
        Bucket*      getBucket(int bucketIndex);
        uint64_t     getBucketFirstKey(BucketHandle bh);
        uint64_t     getBucketLastKey(BucketHandle bh);
        bool         isBottom();
        void         partitionWalk(PartitionCollector *pc);
        LogTime      getCreateTime();
        uint64_t     getFirstKey();
        uint64_t     getLastKey();

      private:
        BucketHandle parent;
        uint64_t     bucketWidth;
        Bucket      *buckets;
        int          numBuckets;
        uint64_t     firstKey;
        uint64_t     lastKey;
        uint64_t     totalBytes;
        uint64_t     totalObjects;
        uint32_t     totalChildren;
        LogTime      createTime;

        DISALLOW_COPY_AND_ASSIGN(Subrange);
    };

    // private methods
    Subrange::BucketHandle findBucket(uint64_t key, LogTime *time = NULL);

    // Oustercount private variables
    Subrange*              root;
    Subrange::BucketHandle findHint;

    DISALLOW_COPY_AND_ASSIGN(Oustercount);
};

} // namespace

#endif // !RAMCLOUD_OUSTERCOUNT_H
