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

#ifndef RAMCLOUD_TABLETPROFILER_H
#define RAMCLOUD_TABLETPROFILER_H

#include <vector>
#include "Common.h"
#include "Log.h"

namespace RAMCloud {

class Partition {
 public:
    Partition()
        : firstKey(0), lastKey(0), minBytes(0), maxBytes(0),
          minReferants(0), maxReferants(0)
    {
    }
    uint64_t firstKey;
    uint64_t lastKey;
    uint64_t minBytes;
    uint64_t maxBytes;
    uint64_t minReferants;
    uint64_t maxReferants;
};
typedef std::vector<Partition> PartitionList;

class TabletProfiler {
  public:
    TabletProfiler();
    ~TabletProfiler();

    // public methods
    void           track(uint64_t key, uint32_t bytes, LogTime time);
    void           untrack(uint64_t key, uint32_t bytes, LogTime time);
    PartitionList* getPartitions(uint64_t maxPartitionBytes,
                                 uint64_t maxPartitionReferants,
                                 uint64_t residualMaxBytes,
                                 uint64_t residualMaxReferants);

  private:
    static const int      BITS_PER_LEVEL = 8;
    static const uint64_t BUCKET_SPLIT_BYTES = 8 * 1024 * 1024;
    static const uint64_t BUCKET_SPLIT_OBJS  = BUCKET_SPLIT_BYTES / 100;
    static const uint64_t BUCKET_MERGE_BYTES = BUCKET_SPLIT_BYTES * 0.75;
    static const uint64_t BUCKET_MERGE_OBJS  = BUCKET_SPLIT_OBJS  * 0.75;

    class PartitionCollector {
      public:
        PartitionCollector(uint64_t maxPartitionBytes,
                           uint64_t maxPartitionReferants,
                           PartitionList* partitions,
                           uint64_t residualMaxBytes,
                           uint64_t residualMaxReferants);
        void addRange(uint64_t firstKey, uint64_t lastKey,
                      uint64_t rangeBytes, uint64_t rangeReferants,
                      uint64_t possibleBytes, uint64_t possibleReferants);
        void done();

      private:
        PartitionList* partitions;

        void pushCurrentTally(uint64_t lastKey,
                              uint64_t minBytes,
                              uint64_t maxBytes,
                              uint64_t minReferants,
                              uint64_t maxReferants);

        // residual counts passed in to the constructor, for use in the
        // first partition generated
        uint64_t residualMaxBytes;
        uint64_t residualMaxReferants;

        // current tally
        uint64_t maxPartitionBytes;
        uint64_t maxPartitionReferants;
        uint64_t nextFirstKey;
        uint64_t currentFirstKey;
        uint64_t currentKnownBytes;
        uint64_t currentKnownReferants;
        uint64_t previousPossibleBytes;
        uint64_t previousPossibleReferants;
        bool     isDone;

        friend class TabletProfilerTest;

        DISALLOW_COPY_AND_ASSIGN(PartitionCollector);
    };

    // forward decl
    class Subrange;

    struct Bucket {
        Subrange *child;
        uint64_t  totalBytes;
        uint64_t  totalReferants;
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

            bool operator==(const BucketHandle& other) const {
                return (this->subrange == other.subrange &&
                       this->bucketIndex == other.bucketIndex);
            }

          private:
            Subrange* subrange;
            int       bucketIndex;

            friend class TabletProfilerTest;
        };

        Subrange(BucketHandle parent, uint64_t firstKey, uint64_t lastKey,
                 LogTime time);
       ~Subrange();

        void         track(BucketHandle bh, uint64_t key, uint32_t bytes,
                               LogTime time);
        bool         untrack(BucketHandle bh, uint64_t key, uint32_t bytes,
                                  LogTime time);
        BucketHandle findBucket(uint64_t key, LogTime *time = NULL);
        Bucket*      getBucket(int bucketIndex);
        uint64_t     getBucketFirstKey(BucketHandle bh);
        uint64_t     getBucketLastKey(BucketHandle bh);
        bool         isBottom();
        void         partitionWalk(PartitionCollector *pc, uint64_t parentBytes = 0,
                                   uint64_t parentReferants = 0);
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
        uint64_t     totalReferants;
        uint32_t     totalChildren;
        LogTime      createTime;

        friend class TabletProfilerTest;

        DISALLOW_COPY_AND_ASSIGN(Subrange);
    };

    // private methods
    Subrange::BucketHandle findBucket(uint64_t key, LogTime *time = NULL);

    // TabletProfiler private variables
    Subrange*              root;
    Subrange::BucketHandle findHint;
    LogTime                lastTracked;
    uint64_t               totalTracked;
    uint64_t               totalTrackedBytes;

    friend class TabletProfilerTest;

    DISALLOW_COPY_AND_ASSIGN(TabletProfiler);
};

} // namespace

#endif // !RAMCLOUD_TABLETPROFILER_H
