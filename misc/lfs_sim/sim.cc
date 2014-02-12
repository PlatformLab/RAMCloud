/* Copyright (c) 2013 Stanford University
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

/*
 * This is a reimplementation of the LFS simulator described in Mendel Rosenblum's
 * PhD dissertation. Assuming it's correct, it appears to show that Mendel's
 * simulator actually implemented a better cost-benefit policy than what was
 * described in the text and implemented in Sprite LFS. See Steve Rumble's
 * dissertation for details.
 */

#include <assert.h>
#include <math.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>
#include <unistd.h>
#include <limits>
#include <vector>
#include <algorithm>
#include <string>

// -Constants-
// These are LFS simulator defaults (see 5.5.3 in Mendel's dissertation)
static const int    DEAD_OBJECT_ID = -1;                        // Sentinel. Do not change.
static const size_t OBJECTS_PER_SEGMENT = 512;                  // Simulates 4KB objects and 2MB segments.
static const int    TOTAL_LIVE_OBJECTS = 51200;                 // 100 segments of live data
static const int    MAX_LIVE_OBJECTS_WRITTEN_PER_PASS = 2560;   // Controls amount of live data processed per cleaning pass.
                                                                // Usually a multiple of OBJECTS_PER_SEGMENT.
static bool   ORDER_SEGMENTS_BY_MIN_AGE = true;                 // Cost-benefit uses the min (not average) object age.
static bool   ORDER_SEGMENTS_BY_SEGMENT_AGE = false;            // Cost-benefit uses the segment (rather than object) age.
static bool   TAKE_ROOT_OF_AGE = false;                         // For reasons not well-understood, this helps cost-benefit sometimes.
static bool   TAKE_LOG_OF_AGE = false;                          // For reasons not well-understood, this helps cost-benefit sometimes.
static bool   SORT_SURVIVOR_OBJECTS = true;                     // Set to true to sort survivors. Most of Mendel's experiments
                                                                // appear to have set this to true, but I don't think they all
                                                                // did (e.g. the initial simulations in 5-2).
static bool  RESET_OBJECT_TIMESTAMPS_WHEN_CLEANING = false;     // If true, set object timestamps become 'currentTimestamp' when moved.
static bool  USE_RAMCLOUD_COST_BENEFIT = false;                 // If true, use RAMCloud's slightly different cost-benefit equation.
static bool  USE_DECAY_FOR_COST_BENEFIT = false;                // If true, use segment decay rate rather than data age in cost-benefit.
static bool  USE_LIFETIMES_WHEN_REPLAYING = false;              // If true and we're replaying a script, use actual lifetimes rather than estimates based on age.
static double ROOT_EXP = 0.5;                                   // If TAKE_ROOT_OF_AGE is true, this is what age will be raised to.
static double LOG_BASE = 2;                                     // If TAKE_LOG_OF_AGE is true, this is the base of our log.

// -Counters-
static size_t segmentsCleaned = 0; 
static size_t cleaningPasses = 0;
static size_t segmentsFreeAfterCleaning = 0;
static size_t emptySegmentsCleaned = 0; 
static size_t newObjectsWritten = 0;
static size_t cleanerObjectsWritten = 0;
static int    currentTimestamp = 0;
static size_t objectWriteCounts[TOTAL_LIVE_OBJECTS];

double
log(double x, double base)
{
    return log(x) / log(base);
}

class Segment;

class ObjectReference {
  public:
    ObjectReference(Segment* objectSegment, size_t objectIndex)
        : segment(objectSegment)
        , index(objectIndex)
    {
    }

    // Segment containing the object.
    Segment* segment;

    // Offset of the object within the segment.
    size_t index;
};

class Object {
  public:
    Object(int objectId, int objectTimestamp, int objectTimeOfDeath)
        : id(objectId)
        , timestamp(objectTimestamp)
        , timeOfDeath(objectTimeOfDeath)
    {
    }

    bool
    isDead() const
    {
        return (id == DEAD_OBJECT_ID);
    }

    int
    getId() const
    {
        return id;
    }

    int
    getTimestamp() const
    {
        return timestamp;
    }

    int
    getTimeOfDeath() const
    {
        return timeOfDeath;
    }

  private:
    // Unique identifier of the object.
    int id;

    // Simulation time at which the object was written.
    int timestamp;

    // When the object will be overwritten (die). This only makes sense if
    // you're replaying a script from a previous run, or if your computer
    // happens to be an oracle.
    int timeOfDeath;
};

class Segment {
  public:
    Segment()
        : objects(),
          liveObjectCount(0),
          timestampSum(0),
          latestTimestamp(0),
          createTimestamp(currentTimestamp)
	{
		objects.reserve(OBJECTS_PER_SEGMENT);
	}

    ObjectReference
	append(int objectId, int timestamp, int timeOfDeath)
	{
		assert(objects.size() < OBJECTS_PER_SEGMENT);
        timestampSum += timestamp;
        latestTimestamp = std::max(latestTimestamp, timestamp);
		objects.push_back({ objectId, timestamp, timeOfDeath });
		liveObjectCount++;
        return { this, objects.size() - 1 };
	}

	void
	free(ObjectReference* ref)
	{
        assert(!objects[ref->index].isDead());
        int timestamp = objects[ref->index].getTimestamp();
        timestampSum -= timestamp;
        objects[ref->index] = { DEAD_OBJECT_ID, 0, 0 };

        // If this was our youngest object, then linearly search for the next youngest.
        // This occurs infrequently, so it's probably not worth optimising.
        //
        // I don't think Sprite LFS did this, and I'm not sure about Mendel's simulator.
        // In any event, it doesn't appear to make a big difference. That seems sensible,
        // since we'll segregate objects by age and the next live minimum is probably close
        // to the previous minimum.
        if (timestamp == latestTimestamp) {
            latestTimestamp = 0;
            for (int i = 0; i < objects.size(); i++) {
                if (objects[i].isDead())
                    continue;
                latestTimestamp = std::max(latestTimestamp, objects[i].getTimestamp());
            }
        }

        liveObjectCount--;
	}

    int
    size()
    {
        return objects.size();
    }

    Object&
    operator[](size_t index)
    {
        assert(index < size());
        return objects[index];
    }

	double
	utilisation()
	{
		return (double)liveObjectCount / (double)OBJECTS_PER_SEGMENT;
	}

	bool
	full()
	{
		return (objects.size() == OBJECTS_PER_SEGMENT);
	}

    int
    liveObjects()
    {
        return liveObjectCount;
    }

    int
    getTimestamp()
    {
        if (liveObjectCount == 0)
            return 0;

        if (ORDER_SEGMENTS_BY_MIN_AGE)
            return latestTimestamp;

         return timestampSum / liveObjectCount;
    }

    int
    getCreationTimestamp()
    {
        return createTimestamp;
    }

  private:
	// Log order vector of objects written.
	std::vector<Object> objects;

    // Number of objects currently alive in this segment.
    size_t liveObjectCount;

    // Sum of the timestamps of all objects appended to this segment.
    uint64_t timestampSum;

    // Timestamp of the the youngest object appended to this segment.
    int latestTimestamp;

    // Timestamp when this segment was created.
    int createTimestamp;
};

class Distribution {
  private:
    std::string name;
    int (*nextObjectFunction)(void);

  protected:
    static int
    randInt(int min, int max)
    {
        assert(max >= min);
        return min + (random() % (max - min + 1));
    }

    static int
    uniform()
    {
        return randInt(0, TOTAL_LIVE_OBJECTS - 1);
    }

    // Return a number selected uniform randomly in the unit interval (0, 1)
    static double
    uniformUnit()
    {
        double r;
        do {
            r = (double)random() / RAND_MAX;
            assert(r >= 0 && r <= 1);
        } while (r == 0 || r == 1);
        return r;
    }

    static int
    hotAndCold()
    {
        static const int hotDataSpacePct = 10;
        static const int hotDataAccessPct = 90;

        double hotFraction = hotDataSpacePct / 100.0;
        int lastHotObjectId = hotFraction * TOTAL_LIVE_OBJECTS;

        if (randInt(0, 99) < hotDataAccessPct) {
            return randInt(0, lastHotObjectId - 1);
        } else {
            return randInt(lastHotObjectId, TOTAL_LIVE_OBJECTS - 1);
        }
    }

    static int
    exponential()
    {
        // Mendel's value. Supposedly 90% of writes go to 2% of data.
        // In order for that to happen we need to cap the range of
        // our random variants to be in [0, 2.5), essentially shifting
        // our exponential curve to the left.
        static const double mu = 0.02172;
        static const double cap = 2.5;

        // Note that mu = 1/lambda
        double expRnd;
        do {
            expRnd = -log(uniformUnit()) * mu;
        } while (expRnd >= cap);

        // Normalise to [0, 1) and choose the corresponding object
        // (each one corresponds to an equal-sized slice in that range).
        return (int)(expRnd / cap * TOTAL_LIVE_OBJECTS);
    }

  public:
    Distribution(std::string distributionName)
        : name(distributionName)
        , nextObjectFunction(NULL)
    {
        if (name == "hotAndCold") {
            nextObjectFunction = hotAndCold;
        } else if (name == "uniform") {
            nextObjectFunction = uniform;
        } else if (name == "exponential") {
            nextObjectFunction = exponential;
        } else if (name == "zipfian") {
            // it's a subclass. XXX clean this up.
        } else {
            throw 0;
        }
    }

    virtual int
    getNextObject()
    {
        int n = nextObjectFunction();
        assert(n >= 0 && n < TOTAL_LIVE_OBJECTS);
        return n;
    }

    std::string
    toString()
    {
        return name;
    }
};

class ZipfianDistribution : public Distribution {
  private:
    double
    generalizedHarmonic(uint64_t N, double s)
    {
        static uint64_t lastN = -1;
        static double lastS = 0;
        static double cachedResult = 0;

        if (N != lastN || s != lastS) {
            lastN = N;
            lastS = s;
            cachedResult = 0;
            for (uint64_t n = 1; n <= N; n++)
                cachedResult += 1.0 / pow(static_cast<double>(n), s);
        }

        return cachedResult;
    }

    /*
     * Zipfian frequency formula from wikipedia.
     */
    double
    f(uint64_t k, double s, uint64_t N)
    {
        return 1.0 / pow(static_cast<double>(k), s) / generalizedHarmonic(N, s);
    }

    double
    getHotDataPct(uint64_t N, double s, int hotAccessPct)
    {
        double sum = 0;
        for (uint64_t i = 1; i <= N; i++) {
            double p = f(i, s, N);
            sum += 100.0 * p;
            if (sum >= hotAccessPct)
                return 100.0 * static_cast<double>(i) / static_cast<double>(N);
        }
        return 100;
    }

    /**
     * Try to compute the proper skew value to get a Zipfian distribution
     * over N keys where hotAccessPct of the requests go to hotDataPct of
     * the data.
     *
     * This method simply does a binary search across a range of skew
     * values until it finds something close. For large values of N this
     * can take a while (a few minutes when N = 100e6). If there's a fast
     * approximation of the generalizedHarmonic method above (which supposedly
     * converges to the Riemann Zeta Function for large N), this could be
     * significantly improved.
     *
     * But, it's good enough for current purposes.
     */
    double
    calculateSkew(uint64_t N, int hotAccessPct, int hotDataPct)
    {
        double minS = 0.01;
        double maxS = 10;

        for (int i = 0; i < 100; i++) {
            double s = (minS + maxS) / 2;
            double r = getHotDataPct(N, s, hotAccessPct);
            if (fabs(r - hotDataPct) < 0.5)
                return s;
            if (r > hotDataPct)
                minS = s;
            else
                maxS = s;
        }

        // Bummer. We didn't find a good value. Just bail.
        fprintf(stderr, "%s: Failed to calculate reasonable skew for "
            "N = %lu, %d%% -> %d%%\n", __func__, N, hotAccessPct, hotDataPct);
        exit(1);
    }

    /**
     * Each of these entries represents a contiguous portion of the CDF curve
     * of keys vs. frequency. We use these to sample keys out to fit an
     * approximation of the Zipfian distribution. See generateTable for more
     * details on what's going on here.
     */
    struct Group {
        Group(uint64_t firstKey, uint64_t lastKey, double firstCdf)
            : firstKey(firstKey)
            , lastKey(lastKey)
            , firstCdf(firstCdf)
        {
        }
        uint64_t firstKey;
        uint64_t lastKey;
        double firstCdf;
    };

    std::vector<Group> groupsTable;

    /**
     * It'd require too much space and be too slow to generate a full histogram
     * representing the frequency of each individual key when there are a large
     * number of them. Instead, we divide the keys into a number of groups and
     * choose a group based on the cumulative frequency of keys it contains. We
     * then uniform-randomly select a key within the chosen group (see the
     * chooseNextKey method).
     *
     * The number of groups is fixed (1000) and each contains 0.1% of the key
     * frequency space. This means that groups tend to increase exponentially in
     * the size of the range of keys they contain (unless the Zipfian distribution
     * was skewed all the way to uniform). The result is that we are very accurate
     * for the most popular keys (the most popular keys are singleton groups) and
     * only lose accuracy for ones further out in the curve where it's much flatter
     * anyway.
     */
    void
    generateTable(const uint64_t N, const double s)
    {
        const double groupFrequencySpan = 0.001;

        double cdf = 0;
        double startCdf = 0;
        uint64_t startI = 1;
        for (uint64_t i = startI; i <= N; i++) {
            double p = f(i, s, N);
            cdf += p;
            if ((cdf - startCdf) >= groupFrequencySpan || i == N) {
                groupsTable.push_back({startI, i, startCdf});
                startCdf = cdf;
                startI = i + 1;
            }
        }
    }

    /**
     * Obtain the next key fitting the Zipfian distribution described by the
     * 'groupsTable' table.
     *
     * This method binary searches (logn complexity), but the groups table
     * is small enough to fit in L2 cache, so it's quite fast (~6M keys
     * per second -- more than sufficient for now).
     */
  public:
    uint64_t
    chooseNextKey()
    {
        double p = uniformUnit();
        size_t min =  0;
        size_t max = groupsTable.size() - 1;
        while (true) {
            size_t i = (min + max) / 2;
            double start = groupsTable[i].firstCdf;
            double end = 1.1;
            if (i != groupsTable.size() - 1)
                end = groupsTable[i + 1].firstCdf;
            if (p < start) {
                max = i - 1;
            } else if (p >= end) {
                min = i + 1;
            } else {
                return randInt(groupsTable[i].firstKey,
                               groupsTable[i].lastKey);
            }
        }
    }

    ZipfianDistribution(int totalObjects,
                        int hotDataAccessPercentage = 90,
                        int hotDataSpacePercentage = 15)
        : Distribution("zipfian")
        , groupsTable()
        , totalObjects(totalObjects)
    {
        assert(totalObjects > 0);
        fprintf(stderr, "%s: Calculating skew value (may take a while)...\n",
            __func__);
        double s = calculateSkew(totalObjects - 1,
                                 hotDataAccessPercentage,
                                 hotDataSpacePercentage);
        fprintf(stderr, "%s: done.\n", __func__);
        generateTable(totalObjects - 1, s);
    }

    int
    getNextObject()
    {
        return (int)chooseNextKey();
    }

    std::string
    toString()
    {
        return "zipfian";
    }

  private:
    int totalObjects;
};



class Strategy {
  private:
    std::string name;
    void (*chooseSegmentFunction)(std::vector<Segment*>& activeList, std::vector<Segment*>& segmentsToClean);

    static void
    greedy(std::vector<Segment*>& activeList, std::vector<Segment*>& segmentsToClean)
    {
        int objectCount = 0;
        while (true) {
            if (activeList.size() == 0)
                break;

            int leastIdx = 0;
            for (int i = 0; i < activeList.size(); i++) {
                if (activeList[i]->utilisation() < activeList[leastIdx]->utilisation())
                    leastIdx = i;
            }

            objectCount += activeList[leastIdx]->liveObjects();
            if (objectCount > MAX_LIVE_OBJECTS_WRITTEN_PER_PASS)
                break;

            segmentsToClean.push_back(activeList[leastIdx]);

            // remove it from the activeList
            activeList[leastIdx] = activeList.back();
            activeList.pop_back();
        }
    }

    // Used to sort our list of active segments by cost-benefit score, from
    // lowest-scoring at the front to highest-scoring at the back.
    struct CostBenefitComparer {
        static double
        costBenefit(Segment* segment)
        {
            double u = segment->utilisation();
            if (u == 0)
                return std::numeric_limits<double>::max();
            if (u == 1)
                return std::numeric_limits<double>::min();

            uint32_t segmentAge = currentTimestamp - segment->getCreationTimestamp();
            if (USE_DECAY_FOR_COST_BENEFIT) {
                segmentAge = std::max(1U, segmentAge);
                double decay = (1 - u) / segmentAge;
                return (1.0 - u) / ((1 + u) * pow(decay, ROOT_EXP));
            }

            uint64_t age = currentTimestamp - segment->getTimestamp();
            if (ORDER_SEGMENTS_BY_SEGMENT_AGE)
                age = segmentAge;

            if (TAKE_ROOT_OF_AGE)
                age = pow(age, ROOT_EXP);
            else if (TAKE_LOG_OF_AGE)
                age = log(age, LOG_BASE);

            if (USE_RAMCLOUD_COST_BENEFIT)
                return ((1.0 - u) * age) / (u);

            // Default LFS cost-benefit formula.
            return ((1.0 - u) * age) / (1.0 + u);
        }

        bool
        operator()(Segment* a, Segment* b)
        {
            return costBenefit(a) < costBenefit(b);
        }
    };

    static void
    costBenefit(std::vector<Segment*>& activeList, std::vector<Segment*>& segmentsToClean)
    {
        std::sort(activeList.begin(), activeList.end(), CostBenefitComparer());

        int objectCount = 0;
        for (int i = 0; !activeList.empty(); i++) {
            objectCount += activeList.back()->liveObjects();
            if (objectCount > MAX_LIVE_OBJECTS_WRITTEN_PER_PASS)
                break;

            segmentsToClean.push_back(activeList.back());
            activeList.pop_back();
        }
    }

  public:
    Strategy(std::string strategyName)
        : name(strategyName)
        , chooseSegmentFunction(NULL)
    {
        if (name == "costBenefit") {
            chooseSegmentFunction = costBenefit;
        } else if (name == "greedy") {
            chooseSegmentFunction = greedy;
        } else {
            throw 0;
        }
    }

    std::string
    toString()
    {
        return name;
    }

    void
    chooseSegments(std::vector<Segment*>& activeList, std::vector<Segment*>& segmentsToClean)
    {
        chooseSegmentFunction(activeList, segmentsToClean);
    }
};

// -Commandline parameters-
static int utilisation = 75;
static Distribution* distribution;
static Strategy* strategy;

class Utilisations {
  public:
    Utilisations()
        : totalSamples(0),
          counts()
    {
        memset(counts, 0, sizeof(counts));
    }

    enum { BUCKETS = 1000 };

    void
    store(double u)
    {
        counts[(int)round(BUCKETS * u)]++;
        totalSamples++;
        total += u;
    }

    void
    dump(const char* description)
    {
        printf("####\n# %s (avg: %.3f)\n####\n", description, total / totalSamples);

        int64_t sum = 0;
        for (int i = 0; i <= BUCKETS; i++) {
            if (counts[i] == 0)
                continue;
            sum += counts[i];
            printf("%f %f %f\n", (double)i / BUCKETS, (double)counts[i] / totalSamples, (double)sum / totalSamples);
        }
    }

    int totalSamples;
    double total;
    int counts[BUCKETS + 1];
};

Utilisations preCleaningUtilisations;
Utilisations cleanedSegmentUtilisations;

void
recordUtilisations(Utilisations& util, std::vector<Segment*>& list)
{
    for (int i = 0; i < list.size(); i++)
        util.store(list[i]->utilisation());
}

int
nextObject(FILE* inScriptFile, int* timeOfDeath)
{
    // If we're replaying a script, just return the next value from it.
    if (inScriptFile != NULL) {
        static size_t line = 0;
        char buf[50];
        if (fgets(buf, sizeof(buf), inScriptFile) == NULL) {
            // we're done
            return -1;
        }

        int objId;
        if (sscanf(buf, "%d %d\n", &objId, timeOfDeath) != 2) {
            fprintf(stderr, "corrupt script file: line #%Zd = [%s]\n", line, buf);
            exit(1);
        }
        if (*timeOfDeath == -1) {
            // -1 means that the file is never overwritten again
            *timeOfDeath = INT_MAX;
        }
        assert(*timeOfDeath >= 0);
        line++;
        return objId;
    }

    // Otherwise, we're generating the accesses from the given distribution.

    // Pre-filling by first writing each file before doing any overwrites is
    // what Mendel's dissertation says happens (5.3.1). Diego wondered if this
    // makes a difference. It doesn't appear to matter much if one prefills all
    // objects or uses the distribution the whole time.
    //
    // However, for distributions with substantial amounts of very cold data
    // (like exponential), we need to be careful to randomly prefill. If we
    // don't, we can end up packing together objects that will never be
    // overwritten into perfectly cold segments from the get-go, which makes
    // the behaviour appear artificially good. This occurs because Distribution
    // often chooses objects such that objects with close ids have similar
    // expected lifetimes.
    static bool generated = false;
    static std::vector<int> fillIds;
    if (fillIds.empty() && !generated) {
        for (int i = 0; i < TOTAL_LIVE_OBJECTS; i++)
            fillIds.push_back(i);
        std::random_shuffle(fillIds.begin(), fillIds.end());
        generated = true;
    }
    if (!fillIds.empty()) {
        int id = fillIds.back();
        fillIds.pop_back();
        return id;
    }

    return distribution->getNextObject();
}

void
getSegmentsToClean(std::vector<Segment*>& activeList, std::vector<Segment*>& segmentsToClean)
{
    strategy->chooseSegments(activeList, segmentsToClean);
    recordUtilisations(cleanedSegmentUtilisations, segmentsToClean);
}

// Used to sort survivors such that the oldest are written first. This is slightly
// preferable to writing oldest last because the last survivor segment is likely to
// not be completely full and will mix in new writes. We'd prefer to mix hotter
// data with the new writes (most of which will be hot) than with colder data.
struct TimestampComparer {
    bool
    operator()(const Object& a, const Object& b)
    {
        return (a.getTimestamp() < b.getTimestamp());
    }
};

// Used to sort survivors when we're replaying from a script and using future
// knowledge to our benefit. This sorts objects such that the ones to be
// rewritten furthest in the future are written out first.
struct LifetimeComparer {
    bool
    operator()(const Object& a, const Object& b)
    {
        return (a.getTimeOfDeath() > b.getTimeOfDeath());
    }
};

void
getLiveObjects(std::vector<Segment*>& segments, std::vector<Object>& liveObjects)
{
    for (int i = 0; i < segments.size(); i++) {
        Segment& s = *segments[i];
        for (int j = 0; j < s.size(); j++) {
            if (s[j].isDead())
                continue;
            liveObjects.push_back(s[j]);
        }
    }

    if (SORT_SURVIVOR_OBJECTS) {
        if (USE_LIFETIMES_WHEN_REPLAYING)
            std::sort(liveObjects.begin(), liveObjects.end(), LifetimeComparer());
        else
            std::sort(liveObjects.begin(), liveObjects.end(), TimestampComparer());
    } 
}

Segment*
clean(std::vector<Segment*>& activeList, std::vector<Segment*>& freeList, std::vector<ObjectReference>& objectToSegment)
{
    assert(freeList.size() == 0);

	std::vector<Segment*> survivorList;

    // We don't clean to the current head segment for the simple reason that
    // it is full (cleaning occurs only when we've completely run out of
    // free segments).
	Segment* newHead = NULL;

    cleaningPasses++;
    recordUtilisations(preCleaningUtilisations, activeList);

    std::vector<Segment*> segmentsToClean;
    getSegmentsToClean(activeList, segmentsToClean);

    std::vector<Object> liveObjects;
    getLiveObjects(segmentsToClean, liveObjects);
    cleanerObjectsWritten += liveObjects.size();

    int survivorsAllocated = 0;

    for (int i = 0; i < liveObjects.size(); i++) {
        assert(!liveObjects[i].isDead());

        if (newHead != NULL && newHead->full()) {
            survivorList.push_back(newHead);
            newHead = NULL;
        }

        if (newHead == NULL) {
            newHead = new Segment();
            survivorsAllocated++;
        }

        int timestamp = liveObjects[i].getTimestamp();

        // It seems like you wouldn't want to do this, but it actually
        // appears to give better results with the LFS cost-benefit
        // equation. I suspect what's happening is that it prevents
        // generating rare, but extremely cold and old segments that
        // are cleaned more often than is desirable.
        //
        // Perhaps this is what Mendel's simulator used to do?
        //
        // The downside is that it retards sorting of objects by age.
        // See the ORDER_SEGMENTS_BY_SEGMENT_AGE flag for a better
        // method that does the same thing in terms of C-B, but keeps
        // absolute ages when sorting.
        if (RESET_OBJECT_TIMESTAMPS_WHEN_CLEANING)
            timestamp = currentTimestamp;

        int id = liveObjects[i].getId();
        int timeOfDeath = liveObjects[i].getTimeOfDeath();
        objectToSegment[id] = newHead->append(id, timestamp, timeOfDeath);
    }

    for (int i = 0; i < segmentsToClean.size(); i++) {
        Segment* s = segmentsToClean[i];

        for (int j = 0; j < objectToSegment.size(); j++)
            assert(objectToSegment[j].segment != s);

        segmentsCleaned++;
        if (s->liveObjects() == 0)
            emptySegmentsCleaned++;
        delete s;

        // Only "return" this segment to the free list once we've
        // already made up for any segments allocated during this
        // cleaning pass.
        if (survivorsAllocated > 0) {
            survivorsAllocated--;
        } else {
            freeList.push_back(new Segment());
        }
    }

    assert(survivorsAllocated == 0);

	for (int i = 0; i < survivorList.size(); i++)
		activeList.push_back(survivorList[i]);

    segmentsFreeAfterCleaning += freeList.size();

	return newHead;
}

static double
lfsWriteCost()
{
    return (double)(newObjectsWritten + cleanerObjectsWritten + (segmentsCleaned - emptySegmentsCleaned) * OBJECTS_PER_SEGMENT) /
        (double)newObjectsWritten;
}

static double
ramcloudWriteCost()
{
    return (double)(newObjectsWritten + cleanerObjectsWritten) / (double)newObjectsWritten;
}

void
generateScriptFile(std::vector<int>& writes, FILE* out)
{
    // Our vector contains the full ordered history of writes from this run.
    // We want to scan it and compute the time of death of each object (when it
    // is overwritten in the future), then we'll dump an ascii file of integer
    // <objectId, timeOfDeath> pairs for a future invocation of the simulator to
    // replay.

    // This is the vector we'll generate and dump. It contains <id, timeOfDeath>
    // tuples.
    std::vector<std::pair<int, int>> idAndTimeOfDeath;
    idAndTimeOfDeath.resize(writes.size(), {-1, -1});

    // This map references the previous write of each object. It is used to
    // record the lifetime of an object when we encounter the next one that
    // overwrites of it.
    std::vector<ssize_t> lastWriteIndexMap;
    lastWriteIndexMap.resize(TOTAL_LIVE_OBJECTS, -1);

    for (size_t i = 0; i < writes.size(); i++) {
        int objId = writes[i];
        idAndTimeOfDeath[i] = { objId, -1 };
        ssize_t lastIndex = lastWriteIndexMap[objId];
        if (lastIndex != -1) {
            // When the object was written is implicit in the order.
            assert(i > lastIndex);
            assert(idAndTimeOfDeath[lastIndex].first == objId);
            idAndTimeOfDeath[lastIndex].second = i;
        }
        lastWriteIndexMap[objId] = i;
    }

    for (size_t i = 0; i < idAndTimeOfDeath.size(); i++) {
        if (fprintf(out, "%d %d\n", idAndTimeOfDeath[i].first, idAndTimeOfDeath[i].second) <= 0) {
            fprintf(stderr, "fprintf returned <= 0; errno = %d (%s)\n", errno, strerror(errno));
            exit(1);
        }
    }
}

// Dump a histogram of object writes to stderr so that the access distribution
// can be sanity checked.
//
// The output is as follows:
//  [% of total objects]        [probability %]     [cumulative %]
//
// The first column serves as an x-value. The second and third columns are
// y-values that can be used for PDFs and CDFs, respectively.
void
dumpObjectWriteHistogram()
{
    printf("####\n# Object Write Distribution\n####\n");

    std::sort(&objectWriteCounts[0], &objectWriteCounts[TOTAL_LIVE_OBJECTS]);
    std::reverse(&objectWriteCounts[0], &objectWriteCounts[TOTAL_LIVE_OBJECTS]);

    size_t sum = 0;
    size_t cumulativeSum = 0;
    for (size_t i = 0; i < TOTAL_LIVE_OBJECTS; i++) {
        sum += objectWriteCounts[i];
        cumulativeSum += objectWriteCounts[i];
        if (i != 0 && (i % (TOTAL_LIVE_OBJECTS / 100)) == 0) {
            printf("%.6f %.6f %.6f\n",
                (double)i / (TOTAL_LIVE_OBJECTS / 100) / 100,
                (double)sum / newObjectsWritten,
                (double)cumulativeSum / newObjectsWritten);
            sum = 0;
        }
    }
}

void
usage()
{
    fprintf(stderr, "valid parameters include:\n");
    fprintf(stderr, "  -a                        (make cost-benefit use average age, not min)\n");
    fprintf(stderr, "  -A                        (make cost-benefit use segment, not object, age)\n");
    fprintf(stderr, "  -d hotAndCold|uniform|    (the access distribution)\n");
    fprintf(stderr, "       exponential|zipfian\n");
    fprintf(stderr, "  -D                        (use segment decay rate in cost-benefit strategy)\n");
    fprintf(stderr, "  -o scriptFile             (dump the full list of object ids written here)\n");
    fprintf(stderr, "  -i scriptFile             (replay the given script of object writes)\n");
    fprintf(stderr, "  -l                        (if replaying from script, use actual object lifetimes in cleaning)\n");
    fprintf(stderr, "  -L base                   (if using cost-benefit, take the log of age with given base)\n");
    fprintf(stderr, "  -r                        (do not reorder live objects by age when cleaning)\n");
    fprintf(stderr, "  -R                        (use RAMCloud equation for the costBenefit strategy)\n");
    fprintf(stderr, "  -s greedy|costBenefit     (the segment selection strategy)\n");
    fprintf(stderr, "  -S exponent               (if using cost-benefit, take the root of age with given exponent)\n");
    fprintf(stderr, "  -t                        (reset object timestamps when moved during cleaning)\n");
    fprintf(stderr, "  -u utilisation            (%% of memory utilisation; 1-99%%)\n");
    exit(1);
}

int
main(int argc, char** argv)
{
	std::vector<Segment*> freeList;
	std::vector<Segment*> activeList;
	std::vector<ObjectReference> objectToSegment;
	Segment* head = NULL;
    FILE* outScriptFile = NULL;
    FILE* inScriptFile = NULL;
    std::vector<int> writes;

    memset(objectWriteCounts, 0, sizeof(objectWriteCounts));

    for (int ch; (ch = getopt(argc, argv, "aAd:Do:i:lL:rRs:S:tu:")) != -1;) {
        switch (ch) {
        case 'a':
            ORDER_SEGMENTS_BY_MIN_AGE = false;
            ORDER_SEGMENTS_BY_SEGMENT_AGE = false;
            break;
        case 'A':
            ORDER_SEGMENTS_BY_SEGMENT_AGE = true;
            ORDER_SEGMENTS_BY_MIN_AGE = false;
            break;
        case 'd':
            try {
                if (strcmp(optarg, "zipfian") == 0)
                    distribution = new ZipfianDistribution(TOTAL_LIVE_OBJECTS);
                else
                    distribution = new Distribution(optarg);
            } catch (...) {
                fprintf(stderr, "invalid distribution name \"%s\"\n", optarg);
                usage();
            }
            break;
        case 'D':
            USE_DECAY_FOR_COST_BENEFIT = true;
            break;
        case 'o':
            outScriptFile = fopen(optarg, "w");
            if (outScriptFile == NULL) {
                fprintf(stderr, "failed to open output script file \"%s\"\n", optarg);
                exit(1);
            }
            break;
        case 'i':
            inScriptFile = fopen(optarg, "r");
            if (inScriptFile == NULL) {
                fprintf(stderr, "failed to open input script file \"%s\"\n", optarg);
                exit(1);
            }
            break;
        case 'l':
            USE_LIFETIMES_WHEN_REPLAYING = true;
            break;
        case 'L':
            TAKE_LOG_OF_AGE = true;
            if (std::string(optarg) == "E" || std::string(optarg) == "e")
                LOG_BASE = M_E;
            else
                LOG_BASE = atof(optarg);
            break;
        case 'r':
            SORT_SURVIVOR_OBJECTS = false;
            break;
        case 'R':
            USE_RAMCLOUD_COST_BENEFIT = true;
            break;
        case 's':
            try {
                strategy = new Strategy(optarg);
            } catch (...) {
                fprintf(stderr, "invalid strategy name \"%s\"\n", optarg);
                usage();
            }
            break;
        case 'S':
            TAKE_ROOT_OF_AGE = true;
            ROOT_EXP = atof(optarg);
            break;
        case 't':
            RESET_OBJECT_TIMESTAMPS_WHEN_CLEANING = true;
            break;
        case 'u':
            utilisation = atoi(optarg);
            if (utilisation < 1 || utilisation > 99)
                usage();
            break;
        case 'h':
        case '?':
        default:
            usage();
        }
    }

    if (TAKE_ROOT_OF_AGE && TAKE_LOG_OF_AGE) {
        fprintf(stderr, "error: only one of -L or -S may be passed\n");
        usage();
    }

    if (USE_LIFETIMES_WHEN_REPLAYING && inScriptFile == NULL) {
        fprintf(stderr, "error: cannot use -l flag without -i as well\n");
        usage();
    }

    if (distribution == NULL)
        distribution = new Distribution("hotAndCold");
    if (strategy == NULL)
        strategy = new Strategy("costBenefit");

    printf("########### Parameters ###########\n");
    printf("# DISTRIBUTON = %s\n", distribution->toString().c_str());
    printf("# STRATEGY = %s\n", strategy->toString().c_str());
    printf("# UTILISATION = %d\n", utilisation);
    printf("# OBJECTS_PER_SEGMENT = %zd\n", OBJECTS_PER_SEGMENT);
    printf("# TOTAL_LIVE_OBJECTS = %d\n", TOTAL_LIVE_OBJECTS);
    printf("# MAX_LIVE_OBJECTS_WRITTEN_PER_PASS = %d\n", MAX_LIVE_OBJECTS_WRITTEN_PER_PASS);
    printf("# ORDER_SEGMENTS_BY_MIN_AGE = %s\n", (ORDER_SEGMENTS_BY_MIN_AGE) ? "true" : "false");
    printf("# ORDER_SEGMENTS_BY_SEGMENT_AGE = %s\n", (ORDER_SEGMENTS_BY_SEGMENT_AGE) ? "true" : "false");
    printf("# TAKE_ROOT_OF_AGE = %s (exp = %.5f)\n", (TAKE_ROOT_OF_AGE) ? "true" : "false", ROOT_EXP);
    printf("# TAKE_LOG_OF_AGE = %s (base = %.5f)\n", (TAKE_LOG_OF_AGE) ? "true" : "false", LOG_BASE);
    printf("# SORT_SURVIVOR_OBJECTS = %s\n", (SORT_SURVIVOR_OBJECTS) ? "true" : "false");
    printf("# RESET_OBJECT_TIMESTAMPS_WHEN_CLEANING = %s\n", (RESET_OBJECT_TIMESTAMPS_WHEN_CLEANING) ? "true" : "false");
    printf("# USE_RAMCLOUD_COST_BENEFIT = %s\n", (USE_RAMCLOUD_COST_BENEFIT) ? "true" : "false");
    printf("# USE_DECAY_FOR_COST_BENEFIT = %s\n", (USE_DECAY_FOR_COST_BENEFIT) ? "true" : "false");
    printf("# USE_LIFETIMES_WHEN_REPLAYING = %s\n", (USE_LIFETIMES_WHEN_REPLAYING) ? "true" : "false");
    printf("##################################\n");

	const int liveDataSegments = TOTAL_LIVE_OBJECTS / OBJECTS_PER_SEGMENT;
	const int totalSegments = 100.0 / utilisation * liveDataSegments;

	printf("# Total Segments: %d, Live Data Segments: %d\n",
        totalSegments, liveDataSegments);
    printf("# Desired u = %.2f, actual u = %.2f\n",
        utilisation / 100.0,
        (double)liveDataSegments / totalSegments);

	for (int i = 0; i < totalSegments; i++)
		freeList.push_back(new Segment());
	for (int i = 0; i < TOTAL_LIVE_OBJECTS; i++)
		objectToSegment.push_back(ObjectReference(NULL, -1));

    double lastWc = 0;
    int writesSinceChange = 0;
    time_t lastPrintTime = 0;

    time_t startTime = time(NULL);
	while (true) {
		if (head != NULL && head->full()) {
			activeList.push_back(head);
			head = NULL;
		}
			
		if (head == NULL) {
			if (freeList.empty()) {
				head = clean(activeList, freeList, objectToSegment);
				// It's possible that the head is full, so just continue.
				continue;
			} else {
				head = freeList.back();
				freeList.pop_back();
			}
		}

        int lifetime = -1;
		int obj = nextObject(inScriptFile, &lifetime);
        if (obj == -1)
            break;

		if (objectToSegment[obj].segment != NULL)
			objectToSegment[obj].segment->free(&objectToSegment[obj]);
		objectToSegment[obj] = head->append(obj, currentTimestamp++, lifetime);
        objectWriteCounts[obj]++;
        newObjectsWritten++;

        if (outScriptFile != NULL)
            writes.push_back(obj);

        // Detect and quit when u stabilises.
        double wcLFS = lfsWriteCost();
        double wcRC = ramcloudWriteCost();
        if (fabs(wcLFS - lastWc) >= 0.0001) {
            lastWc = wcLFS;
            writesSinceChange = 0;
        }

        writesSinceChange++;
        if (writesSinceChange == (2 * totalSegments * OBJECTS_PER_SEGMENT) && inScriptFile == NULL)
            break;

        // Print out every ~1 second. Don't check every time to cut overhead.
		if ((newObjectsWritten % 99991) == 0) {
            time_t runTime = std::max((time_t)1, time(NULL) - startTime);
            if (runTime > lastPrintTime) {
                fprintf(stderr, "\rWrote %zu new objects (%zu / sec), Cleaned %zd segments (%zd empty), %zd survivor objects, LFSwc = %.3f, RCwc = %.3f", newObjectsWritten, newObjectsWritten / runTime, segmentsCleaned, emptySegmentsCleaned, cleanerObjectsWritten, wcLFS, wcRC);
                fflush(stderr);
                lastPrintTime = runTime;
            }
		}

        assert((activeList.size() + freeList.size() + 1) == totalSegments);
	}

	fprintf(stderr, "\nDone!\n");

    printf("# Total simulation time = %d seconds\n", (int)(time(NULL) - startTime));
    printf("# New object writes = %zd\n", newObjectsWritten);
    printf("# Survivor objects written by cleaner = %zd\n", cleanerObjectsWritten);
    printf("# LFS write cost = %.3f\n", lfsWriteCost());
    printf("# Cleaning passes = %zd\n", cleaningPasses);
    printf("# Segments cleaned = %zd\n", segmentsCleaned);
    printf("# Average segments cleaned per pass = %.2f\n", (double)segmentsCleaned / cleaningPasses);
    printf("# Average segments free after cleaning = %.2f\n", (double)segmentsFreeAfterCleaning / cleaningPasses);
    printf("# Objects read from cleaned segments = %zd\n", segmentsCleaned * OBJECTS_PER_SEGMENT);

    preCleaningUtilisations.dump("Live Segment Utilisations Prior to Cleaning");
    printf("\n\n"); // So we can use the gnuplot 'index' to choose between them
    cleanedSegmentUtilisations.dump("Cleaned Segment Utilisations");
    printf("\n\n");
    dumpObjectWriteHistogram();

    if (outScriptFile != NULL) {
        setvbuf(outScriptFile, NULL, _IOFBF, 0);
        generateScriptFile(writes, outScriptFile);
        fflush(outScriptFile);
        fclose(outScriptFile);
    }

	return 0;
}
