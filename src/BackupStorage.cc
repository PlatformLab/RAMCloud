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

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#include "Common.h"
#include "BackupStorage.h"
#include "Memory.h"
#include "ShortMacros.h"

namespace RAMCloud {

// --- BackupStorage ---

/**
 * Report the read and write speed of this storage in MB/s.
 *
 * \return
 *      A pair of storage read and write speeds in MB/s.
 */
pair<uint32_t, uint32_t>
BackupStorage::benchmark(BackupStrategy backupStrategy)
{
    const uint32_t count = 16;
    uint32_t readSpeeds[count];
    uint32_t writeSpeeds[count];
    BackupStorage::Handle* handles[count];

    void* p = Memory::xmemalign(HERE,
                                segmentSize,
                                segmentSize);
    char* segment = static_cast<char*>(p);

    try {
        for (uint32_t i = 0; i < count; ++i)
            handles[i] = NULL;

        for (uint32_t i = 0; i < count; ++i)
            handles[i] = allocate();

        // Measuring write speeds takes too long with fans on with
        // commodity disks and we don't use the result.  Just fake it.
        for (uint32_t i = 0; i < count; ++i)
            writeSpeeds[i] = 100;
        for (uint32_t i = 0; i < count; ++i) {
            CycleCounter<> counter;
            getSegment(handles[i], segment);
            uint64_t ns = Cycles::toNanoseconds(counter.stop());
            readSpeeds[i] = downCast<uint32_t>(
                                segmentSize * 1000UL * 1000 * 1000 /
                                (1 << 20) / ns);
        }
    } catch (...) {
        std::free(segment);
        for (uint32_t i = 0; i < count; ++i)
            if (handles[i])
                free(handles[i]);
        throw;
    }

    std::free(segment);
    for (uint32_t i = 0; i < count; ++i)
        free(handles[i]);

    uint32_t minRead = *std::min_element(readSpeeds,
                                         readSpeeds + count);
    uint32_t minWrite = *std::min_element(writeSpeeds,
                                          writeSpeeds + count);
    uint32_t avgRead = ({
        uint32_t sum = 0;
        foreach (uint32_t speed, readSpeeds)
            sum += speed;
        sum / count;
    });
    uint32_t avgWrite = ({
        uint32_t sum = 0;
        foreach (uint32_t speed, writeSpeeds)
            sum += speed;
        sum / count;
    });

    LOG(NOTICE, "Backup storage speeds (min): %u MB/s read, %u MB/s write",
        minRead, minWrite);
    LOG(NOTICE, "Backup storage speeds (avg): %u MB/s read, %u MB/s write",
        avgRead, avgWrite);

    if (backupStrategy == RANDOM_REFINE_MIN) {
        LOG(NOTICE, "RANDOM_REFINE_MIN BackupStrategy selected");
        return {minRead, minWrite};
    } else if (backupStrategy == RANDOM_REFINE_AVG) {
        LOG(NOTICE, "RANDOM_REFINE_AVG BackupStrategy selected");
        return {avgRead, avgWrite};
    } else if (backupStrategy == EVEN_DISTRIBUTION) {
        LOG(NOTICE, "EVEN_SELECTION BackupStrategy selected");
        return {100, 100};
    } else {
        DIE("Bad BackupStrategy selected");
    }
}

// --- BackupStorage::Handle ---

int32_t BackupStorage::Handle::allocatedHandlesCount = 0;

// --- SingleFileStorage ---

// - public -

/**
 * Create a SingleFileStorage.
 *
 * \param segmentSize
 *      The size in bytes of the segments this storage will deal with.
 * \param segmentFrames
 *      The number of segments this storage can store simultaneously.
 * \param filePath
 *      A filesystem path to the device or file where segments will be stored.
 * \param openFlags
 *      Extra flags for use while opening filePath (default to 0, O_DIRECT may
 *      be used to disable the OS buffer cache.
 */
SingleFileStorage::SingleFileStorage(uint32_t segmentSize,
                                     uint32_t segmentFrames,
                                     const char* filePath,
                                     int openFlags)
    : BackupStorage(segmentSize, Type::DISK)
    , freeMap(segmentFrames)
    , openFlags(openFlags)
    , fd(-1)
    , killMessage()
    , killMessageLen()
    , lastAllocatedFrame(FreeMap::npos)
    , segmentFrames(segmentFrames)
{
    const char* killMessageStr = "\0DIE";
    // Must write block size of larger when O_DIRECT.
    killMessageLen = 512;
    int r = posix_memalign(&killMessage, killMessageLen, killMessageLen);
    if (r != 0)
        throw std::bad_alloc();
    memset(killMessage, 0, killMessageLen);
    memcpy(killMessage, killMessageStr, 4);

    freeMap.set();

    fd = open(filePath,
              O_CREAT | O_RDWR | openFlags,
              0666);
    if (fd == -1) {
        auto e = errno;
        LOG(ERROR, "Failed to open backup storage file %s: %s",
            filePath, strerror(e));
        throw BackupStorageException(HERE,
              format("Failed to open backup storage file %s", filePath), e);
    }

    // If its a regular file reserve space, otherwise
    // assume its a device and we don't need to bother.
    struct stat st;
    r = stat(filePath, &st);
    if (r == -1)
        return;
    if (st.st_mode & S_IFREG)
        reserveSpace();
}

/// Close the file.
SingleFileStorage::~SingleFileStorage()
{
    int r = close(fd);
    if (r == -1)
        LOG(ERROR, "Couldn't close backup log");
    std::free(killMessage);
}

// See BackupStorage::allocate().
BackupStorage::Handle*
SingleFileStorage::allocate()
{
    FreeMap::size_type next = freeMap.find_next(lastAllocatedFrame);
    if (next == FreeMap::npos) {
        next = freeMap.find_first();
        if (next == FreeMap::npos)
            throw BackupStorageException(HERE, "Out of free segment frames.");
    }
    lastAllocatedFrame = next;
    uint32_t targetSegmentFrame = static_cast<uint32_t>(next);
    return associate(targetSegmentFrame);
}

BackupStorage::Handle*
SingleFileStorage::associate(uint32_t segmentFrame)
{
    assert(freeMap[segmentFrame] == 1);
    freeMap[segmentFrame] = 0;
    return new Handle(segmentFrame);
}

/**
 * Same as BackupStorage::benchmark() except it resets the storage to reuse
 * the segment frames that may have been used during benchmarking.
 * This allows benchmark to be called without
 * wasting early segment frames on the disk which may be faster.
 */
pair<uint32_t, uint32_t>
SingleFileStorage::benchmark(BackupStrategy backupStrategy)
{
    auto r = BackupStorage::benchmark(backupStrategy);
    lastAllocatedFrame = FreeMap::npos;
    return r;
}

// See BackupStorage::free().
void
SingleFileStorage::free(BackupStorage::Handle* handle)
{
    uint32_t segmentFrame =
        static_cast<const Handle*>(handle)->getSegmentFrame();

    off_t offset = lseek(fd,
                         offsetOfSegmentFrame(segmentFrame),
                         SEEK_SET);
    if (offset == -1)
        throw BackupStorageException(HERE,
                "Failed to seek to segment frame to free storage", errno);
    ssize_t r = write(fd, killMessage, killMessageLen);
    if (r != killMessageLen)
        throw BackupStorageException(HERE,
                "Couldn't overwrite stored segment header to free", errno);

    freeMap[segmentFrame] = 1;
    delete handle;
}

// See BackupStorage::getSegment().
// NOTE: This must remain thread-safe, so be careful about adding
// access to other resources.
void
SingleFileStorage::getSegment(const BackupStorage::Handle* handle,
                              char* segment) const
{
    uint32_t sourceSegmentFrame =
        static_cast<const Handle*>(handle)->getSegmentFrame();
    off_t offset = offsetOfSegmentFrame(sourceSegmentFrame);
    ssize_t r = pread(fd, segment, segmentSize, offset);
    if (r != static_cast<ssize_t>(segmentSize))
        throw BackupStorageException(HERE, errno);
}

// See BackupStorage::putSegment().
// NOTE: This must remain thread-safe, so be careful about adding
// access to other resources.
void
SingleFileStorage::putSegment(const BackupStorage::Handle* handle,
                              const char* segment) const
{
    uint32_t targetSegmentFrame =
        static_cast<const Handle*>(handle)->getSegmentFrame();
    off_t offset = offsetOfSegmentFrame(targetSegmentFrame);
    ssize_t r = pwrite(fd, segment, segmentSize, offset);
    if (r != static_cast<ssize_t>(segmentSize))
        throw BackupStorageException(HERE, errno);
}

// - private -

/**
 * Pure function.
 *
 * \param segmentFrame
 *      The segmentFrame to find the offset of in the file.
 * \return
 *      The offset into the file a particular segmentFrame starts at.
 */
uint64_t
SingleFileStorage::offsetOfSegmentFrame(uint32_t segmentFrame) const
{
    // Watch out for overflow; multiplying to 32-bit values.
    return uint64_t(segmentFrame) * segmentSize;
}

/**
 * Fix the size of the logfile to ensure that the OS doesn't tell us
 * the filesystem is out of space later.
 *
 * \throw BackupStorageException
 *      If space for segmentFrames segments of segmentSize cannot be reserved.
 */
void
SingleFileStorage::reserveSpace()
{
    uint64_t logSpace = segmentSize;
    logSpace *= segmentFrames;

    LOG(DEBUG, "Reserving %lu bytes of log space", logSpace);
    int r = ftruncate(fd, logSpace);
    if (r == -1)
        throw BackupStorageException(HERE,
                "Couldn't reserve storage space for backup", errno);
}

/**
 * Fetch the starting and ending bytes from each segment frame.
 * Since segments align their header and footer entries to the
 * beginning and end this method can be used to fetch all the
 * locations where the entries would reside (note returned locations
 * don't necessarily contain valid segment data).
 *
 * \param headerSize
 *      Bytes from the beginning of each segment frame to return; the
 *      starting \a headerSize bytes from each segment frame start at
 *      f * (headerSize + footerSize) for f = 0 to #segmentFrames - 1
 *      in the returned result.
 * \param footerSize
 *      Bytes from the end of each segment frame to return; the
 *      ending \a footerSize bytes from each segment frame start at
 *      f * (headerSize + footerSize) + headerSize for f = 0 to
 *      #segmentFrames - 1 in the returned result.
 * \return
 *      An array of bytes of back-to-back entries of alternating
 *      size (first \a headerSize, then \a footerSize, repeated
 *      #segmentFrames times).
 */
std::unique_ptr<char[]>
SingleFileStorage::getAllHeadersAndFooters(size_t headerSize,
                                           size_t footerSize)
{
    typedef char* bytePtr;

    size_t headerBlockSize;
    size_t footerBlockSize;
    if (openFlags & O_DIRECT) {
        headerBlockSize = ((headerSize + 511) / 512) * 512;
        footerBlockSize = ((footerSize + 511) / 512) * 512;
    } else {
        headerBlockSize = headerSize;
        footerBlockSize = footerSize;
    }
    const size_t totalBytes = (footerSize + headerSize) * segmentFrames;
    Memory::unique_ptr_free blocks(
        Memory::xmemalign(HERE, getpagesize(),
                          headerBlockSize + footerBlockSize),
        std::free);

    std::unique_ptr<char[]> results(new char[totalBytes]);
    char* nextResult = results.get();

    off_t offset;
    ssize_t r;

    // Read the first header.
    offset = offsetOfSegmentFrame(0);
    r = pread(fd, blocks.get(), headerBlockSize, offset);
    if (r != ssize_t(headerBlockSize)) {
        LOG(ERROR, "Couldn't read the first header of storage.");
        throw BackupStorageException(HERE, errno);
    }
    memcpy(nextResult, blocks.get(), headerSize);
    nextResult += headerSize;

    // Read the footer and the following header in 1 combined IO.
    const void* startOfHeader = bytePtr(blocks.get()) + footerBlockSize;
    for (uint32_t frame = 1; frame < segmentFrames; ++frame) {
        off_t offset = offsetOfSegmentFrame(frame) - footerBlockSize;
        ssize_t r = pread(fd, blocks.get(),
                          footerBlockSize + headerBlockSize, offset);
        if (r != ssize_t(footerBlockSize + headerBlockSize)) {
            LOG(ERROR, "Couldn't read header from frame %u and the footer from "
                "frame %u of storage.", frame, frame + 1);
            throw BackupStorageException(HERE, errno);
        }
        memcpy(nextResult,
               bytePtr(startOfHeader) - footerSize,
               footerSize + headerSize);
        nextResult += footerSize + headerSize;
    }

    // Read the last footer.
    offset = offsetOfSegmentFrame(segmentFrames) - footerBlockSize;
    r = pread(fd, blocks.get(), footerBlockSize, offset);
    if (r != ssize_t(footerBlockSize)) {
        LOG(ERROR, "Couldn't read the last footer of storage.");
        throw BackupStorageException(HERE, errno);
    }
    memcpy(nextResult,
           bytePtr(blocks.get()) + footerBlockSize - footerSize,
           footerSize);
    nextResult += footerSize;

    return results;
}

// --- InMemoryStorage ---

// - public -

/**
 * Create an in-memory storage area.
 *
 * \param segmentSize
 *      The size of segments this storage will house.
 * \param segmentFrames
 *      The number of segments this storage can house.
 */
InMemoryStorage::InMemoryStorage(uint32_t segmentSize,
                                 uint32_t segmentFrames)
    : BackupStorage(segmentSize, Type::MEMORY)
    , pool(segmentSize)
    , segmentFrames(segmentFrames)
{
}

/// Free up all storage.
InMemoryStorage::~InMemoryStorage()
{
}

// See BackupStorage::allocate().
BackupStorage::Handle*
InMemoryStorage::allocate()
{
    if (!segmentFrames)
        throw BackupStorageException(HERE, "Out of free segment frames.");
    segmentFrames--;
    char* address = static_cast<char *>(pool.malloc());
    return new Handle(address);
}

BackupStorage::Handle*
InMemoryStorage::associate(uint32_t segmentFrame)
{
    return NULL;
}

// See BackupStorage::free().
void
InMemoryStorage::free(BackupStorage::Handle* handle)
{
    TEST_LOG("called");
    char* address =
        static_cast<const Handle*>(handle)->getAddress();
    memcpy(address, "\0DIE", 4);
    pool.free(address);
    delete handle;
}

/**
 * This operation is not supported by InMemoryStorage; just returns NULL.
 *
 * \param headerSize
 *      Ignored.
 * \param footerSize
 *      Ignored.
 * \return
 *      NULL.
 */
std::unique_ptr<char[]>
InMemoryStorage::getAllHeadersAndFooters(size_t headerSize,
                                         size_t footerSize)
{
    return {};
}

// See BackupStorage::getSegment().
// NOTE: This must remain thread-safe, so be careful about adding
// access to other resources.
void
InMemoryStorage::getSegment(const BackupStorage::Handle* handle,
                            char* segment) const
{
    char* address = static_cast<const Handle*>(handle)->getAddress();
    memcpy(segment, address, segmentSize);
}

// See BackupStorage::putSegment().
// NOTE: This must remain thread-safe, so be careful about adding
// access to other resources.
void
InMemoryStorage::putSegment(const BackupStorage::Handle* handle,
                            const char* segment) const
{
    char* address = static_cast<const Handle*>(handle)->getAddress();
    memcpy(address, segment, segmentSize);
}

} // namespace RAMCloud
