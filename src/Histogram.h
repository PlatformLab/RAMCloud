/* Copyright (c) 2012 Stanford University
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

#ifndef RAMCLOUD_HISTOGRAM_H
#define RAMCLOUD_HISTOGRAM_H

#include "Common.h"

#include "Histogram.pb.h"

namespace RAMCloud {

/**
 * This class records a distribution of integer samples may provides methods
 * for generating a string histogram representation, as well as serializing
 * the contents for transfer across the network.
 *
 * The histogram is defined by a number of buckets and the integer width of
 * each bucket. Each bucket stores counts for one or more consecutive integer
 * values. That is, when a sample is stored, it is divded by the bucket width
 * and rounded to the nearest integer. The value in that bucket is then
 * incremented. If the bucket index is beyond the number of allocated buckets,
 * a single outlier count is maintained. Minimum and maximum samples are also
 * tracked.
 *
 * Histograms are useful in measuring various things from hash table lookup
 * times to frequencies of various segment utilizations during log cleaning.
 */
class Histogram {
  public:
    /**
     * Construct a new, empty histogram.
     *
     * \param numBuckets
     *      The number of buckets samples will be stored in. Fewer buckets uses
     *      less space, but restricts the maximum value we will keep a distinct
     *      count of (ouliers will all fall into one logical "outlier bucket").
     * \param bucketWidth
     *      The width of each bucket determines how many consecutive integer
     *      values will be collaspsed down and treated as essentially the same
     *      sample. Smaller widths reduce ambiguity by collapsing fewer distinct
     *      samples together, but restrict the number of distinct integer values
     *      we will be able to fit into buckets.
     */
    Histogram(uint64_t numBuckets, uint64_t bucketWidth)
        : numBuckets(numBuckets),
          bucketWidth(bucketWidth),
          buckets(),
          sampleSum(0),
          outliers(0),
          max(0),
          min(-1UL)
    {
        if (bucketWidth == 0)
            throw FatalError(HERE, "bucketWidth must be > 0");
        buckets.resize(numBuckets, 0);
    }

    /**
     * Construct a new histogram, initializing its values from the given
     * protocol buffer.
     *
     * \param histogram
     *      Protocol buffer serialization of another histogram.
     */
    explicit Histogram(const ProtoBuf::Histogram& histogram)
        : numBuckets(histogram.num_buckets()),
          bucketWidth(histogram.bucket_width()),
          buckets(),
          sampleSum(0),
          outliers(histogram.outliers()),
          max(histogram.max()),
          min(histogram.min())
    {
        buckets.resize(numBuckets, 0);

        foreach (const ProtoBuf::Histogram::Bucket& bucket,
          histogram.bucket()) {
            assert(bucket.index() < numBuckets);
            buckets[bucket.index()] = bucket.count();
        }

        sampleSum = static_cast<__uint128_t>(histogram.sample_sum_high()) << 64;
        sampleSum |= static_cast<__uint128_t>(histogram.sample_sum_low());
    }

    /**
     * Store a given sample in the histogram, placing it in the appropriate
     * bucket, if possible.
     *
     * \param sample
     *      The sample to store.
     */
    void
    storeSample(uint64_t sample)
    {
        // round to the nearest bucket
        uint64_t bucket = (sample + (bucketWidth / 2)) / bucketWidth;

        if (bucket < numBuckets)
            buckets[bucket]++;
        else
            outliers++;

        if (sample < min)
            min = sample;
        if (sample > max)
            max = sample;

        sampleSum += sample;
    }

    /**
     * Zero out the entire histogram, returning it to it's original state after
     * construction.
     */
    void
    reset()
    {
        memset(&buckets[0], 0, sizeof(buckets[0]) * numBuckets);
        sampleSum = outliers = max = 0;
        min = -1UL;
    }

    /**
     * Return a string representation of the histogram.
     *
     * \param minIncludedCount
     *      Optional parameter (defaults to 1) dictating how many samples must
     *      have fallen into a bucket to report it. This can be used to suppress
     *      empty buckets, or buckets with few samples.
     */
    string
    toString(uint64_t minIncludedCount = 1) const
    {
        string s;
        uint64_t totalSamples = getTotalSamples();

        s += format("# Histogram: buckets = %lu, bucket width = %lu\n",
            numBuckets, bucketWidth);
        s += format("# %lu samples, %lu outliers, min = %lu, max = %lu\n",
            totalSamples, outliers, min, max);
        s += format("# median = %lu, average = %lu\n",
            getMedian(), getAverage());

        uint64_t sum = 0;
        for (uint32_t i = 0; i < numBuckets; i++) {
            uint64_t count = buckets[i];
            sum += count;
            if (count >= minIncludedCount) {
                // Try to keep the format easily parsable by scripts (for
                // example, numbers only and in fixed columns).
                s += format("%9u  %12lu  %3.9f  %3.9f\n", i, count,
                    static_cast<double>(count) /
                      static_cast<double>(totalSamples) * 100,
                    static_cast<double>(sum) /
                      static_cast<double>(totalSamples) * 100);
            }
        }

        return s;
    }

    /**
     * Return the number of outlier samples (ones that were too large to fit in
     * any particular bucket). Optionally retrieve the largest outlier.
     *
     * \param outHighestOutlier
     *      Optional pointer in which to store the largest outlier, if there is
     *      one.
     */
    uint64_t
    getOutliers(uint64_t* outHighestOutlier = NULL) const
    {
        if (outliers > 0 && outHighestOutlier != NULL)
            *outHighestOutlier = max;
        return outliers;
    }

    /**
     * Return the total number of samples stored in the histogram, including any
     * outliers.
     */
    uint64_t
    getTotalSamples() const
    {
        // We could easily keep a counter, but I've optimized for the minimum
        // amount of data that needs to be serialized. Getting this count ultra
        // quickly isn't very important.
        uint64_t totalSamples = outliers;
        foreach (uint64_t count, buckets)
            totalSamples += count;
        return totalSamples;
    }

    /**
     * Get the maximum sample stored in the histogram. This may be an outlier.
     * If no samples were stored, the value returned will be 0.
     */
    uint64_t
    getMax() const
    {
        return max;
    }

    /**
     * Get the minimum sample stored in the histogram. This may be an outlier.
     * If no samples were stored, the value returned will be -1UL.
     */
    uint64_t
    getMin() const
    {
        return min;
    }

    /**
     * Get the average sample stored in the histogram. Any outliers will be
     * included in the result.
     */
    uint64_t
    getAverage() const
    {
        uint64_t totalSamples = getTotalSamples();
        if (totalSamples == 0)
            return 0;
        return downCast<uint64_t>(sampleSum / getTotalSamples());
    }

    /**
     * Get the median sample stored in the histogram.
     *
     * If no samples were stored, returns 0. If the median happens to fall
     * within one of the outliers, this method returns -1 (since we don't
     * store the values of all outliers). This is unlikely to be a big deal
     * since if the median is an outlier, then the histogram probably isn't
     * sized properly in begin with.
     */
    uint64_t
    getMedian() const
    {
        uint64_t totalSamples = getTotalSamples();
        if (totalSamples == 0)
            return 0;

        uint64_t currentCount = 0;
        for (uint32_t i = 0; i < numBuckets; i++) {
            currentCount += buckets[i];

            // Doesn't average in the case where there are an even number
            // of samples. Sue me.
            if (currentCount > totalSamples/2)
                return i;
        }

        // Looks like either numBuckets is 0, or our median was an outlier.
        // We don't currently keep around all of the outliers, so we can't
        // return a median in that case.
        assert(numBuckets == 0 || outliers > 0);
        return -1;
    }

    /**
     * Serialize the histogram to a protocol buffer for network transmission.
     */
    void
    serialize(ProtoBuf::Histogram& histogram) const
    {
        histogram.set_num_buckets(numBuckets);
        histogram.set_bucket_width(bucketWidth);
        for (size_t i = 0; i < numBuckets; i++) {
            if (buckets[i] > 0) {
                ProtoBuf::Histogram_Bucket& bucket(*histogram.add_bucket());
                bucket.set_index(i);
                bucket.set_count(buckets[i]);
            }
        }
        histogram.set_sample_sum_high(downCast<uint64_t>(sampleSum >> 64));
        histogram.set_sample_sum_low(downCast<uint64_t>(sampleSum & ~0UL));
        histogram.set_outliers(outliers);
        histogram.set_max(max);
        histogram.set_min(min);
    }

  PRIVATE:
    /// The number of buckets in our histogram. Each bucket stores counts for
    /// samples falling into a particular range, as determined by the buckets'
    /// width.
    const uint64_t numBuckets;

    /// The width of each bucket determines which bucket a particular sample
    /// falls in. The sample is divided by this value and rounded to the nearest
    /// integer to choose the bucket index.
    const uint64_t bucketWidth;

    /// The histogram itself as a vector of sample counters, one counter for
    /// each bucket.
    vector<uint64_t> buckets;

    /// Sum of all samples stored.
    __uint128_t sampleSum;

    /// The number of samples added to the histogram that exceeded the maximum
    /// bucket index.
    uint64_t outliers;

    /// The highest-valued sample. May be an outlier.
    uint64_t max;

    /// The lowest-valued sample. May be an outlier.
    uint64_t min;
};

} // namespace RAMCloud

#endif // !RAMCLOUD_HISTOGRAM_H
