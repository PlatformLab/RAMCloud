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
    , superblock()
    , lastSuperblockFrame(1)
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
    killMessageLen = BLOCK_SIZE;
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
        headerBlockSize = ((headerSize + BLOCK_SIZE - 1) / BLOCK_SIZE) *
                                BLOCK_SIZE;
        footerBlockSize = ((footerSize + BLOCK_SIZE - 1) / BLOCK_SIZE) *
                                BLOCK_SIZE;
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

/**
 * Overwrite the on-storage superblock with new information that future
 * backups reusing this storage will need (in the case of this backup's
 * demise).
 * This is done safely so that a failure in the middle of the update
 * will leave either the old superblock or the new.
 *
 * \param serverId
 *      The server id of the process as assigned by the coordinator.
 *      It is persisted for the benefit of future processes reusing this
 *      storage.
 * \param clusterName
 *      Controls the reuse of replicas stored on this backup.  'Tags'
 *      replicas created on this backup with this cluster name.  This has
 *      two effects.  First, any replicas found in storage are discarded
 *      unless they are tagged with an identical cluster name. Second, any
 *      replicas created by the backup process will only be reused by future
 *      backup processes if the cluster name on the stored replica matches
 *      the cluster name of future process. The name '__unnamed__' is
 *      special and never matches any cluster name (even itself), so it
 *      guarantees all stored replicas are discarded on start and that all
 *      replicas created by this process are discarded by future backups.
 *      This is convenient for testing.
 * \param frameSkipMask
 *      Used for testing. This storage keeps two superblock images and
 *      overwrites the older first then the newer in the case a failure
 *      occurs in the middle of writing. Setting frameSkipMask to 0x1
 *      skips writing the first superblock image, 0x2 skips the second,
 *      and 0x3 skips both.
 */
void
SingleFileStorage::resetSuperblock(ServerId serverId,
                                   const string& clusterName,
                                   const uint32_t frameSkipMask)
{
    Superblock newSuperblock =
        Superblock(superblock.version + 1, serverId, clusterName.c_str());

    Memory::unique_ptr_free block(
        Memory::xmemalign(HERE, getpagesize(), BLOCK_SIZE), std::free);
    struct FileContents {
        explicit FileContents(const Superblock& newSuperblock)
            : superblock(newSuperblock)
            , checksum()
        {
            Crc32C crc;
            crc.update(&superblock, sizeof(superblock));
            checksum = crc.getResult();
        }
        Superblock superblock;
        Crc32C::ResultType checksum;
    } __attribute__((packed));
    new(block.get()) FileContents(newSuperblock);

    // Overwrite the two superblock images starting with the older one.
    for (uint32_t i = 0; i < 2; ++i) {
        const uint32_t nextFrame = (lastSuperblockFrame + 1) % 2;
        if (!((frameSkipMask >> nextFrame) & 0x01)) {
            const uint64_t offset = offsetOfSuperblockFrame(nextFrame);
            ssize_t r = pwrite(fd, block.get(), BLOCK_SIZE, offset);
            // An accurate superblock is required to determine if replicas
            // should be preserved at startup.  Without it any replicas
            // written by this backup would be in jeopardy.
            if (r == -1) {
                DIE("Failed to write the backup superblock; "
                    "cannot continue safely: %s", strerror(errno));
            } else if (r < BLOCK_SIZE) {
                DIE("Short write while writing the backup superblock; "
                    "cannot continue safely");
            }
            int s = fdatasync(fd);
            if (s == -1) {
                DIE("Failed to flush the backup superblock; "
                    "cannot continue safely: %s", strerror(errno));
            }
            LOG(DEBUG, "Superblock frame %u written", nextFrame);
        }
        lastSuperblockFrame = nextFrame;
    }

    superblock = newSuperblock;
}

/**
 * Read both on-storage superblock locations and return the most up-to-date
 * and complete superblock since the last resetSuperblock().
 *
 * \return
 *      The most up-to-date complete superblock found on storage.  If no
 *      superblock can be found a default superblock is returned which
 *      indicates no prior backup instance left behind intelligible
 *      traces of life on storage.
 */
BackupStorage::Superblock
SingleFileStorage::loadSuperblock()
{
    Tub<Superblock> left;
    Tub<Superblock> right;

    try {
        left = tryLoadSuperblock(0);
    } catch (Exception& e) {}
    try {
        right = tryLoadSuperblock(1);
    } catch (Exception& e) {}

    bool chooseLeft = false;
    if (left && right) {
        chooseLeft = left->version >= right->version;
    } else if (!left && !right) {
        LOG(WARNING,
            "Backup couldn't find existing superblock; "
            "starting as fresh backup.");
        right.construct();
        chooseLeft = false;
    } else {
        chooseLeft = left;
    }

    if (chooseLeft) {
        superblock = *left;
        lastSuperblockFrame = 0;
    } else {
        superblock = *right;
        lastSuperblockFrame = 1;
    }

    LOG(DEBUG,
        "Reloading backup superblock (version %lu, superblockFrame %u) "
        "from previous run", superblock.version, lastSuperblockFrame);
    LOG(DEBUG, "Prior backup had ServerId %lu", superblock.serverId);
    LOG(DEBUG, "Prior backup had cluster name '%s'", superblock.clusterName);

    return superblock;
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
    const uint64_t firstSegmentFrameStart = offsetOfSuperblockFrame(2);
    // Watch out for overflow; multiplying to 32-bit values.
    return firstSegmentFrameStart + uint64_t(segmentFrame) * segmentSize;
}

/**
 * Pure function.
 *
 * \param index
 *      The superblock can be stored at two locations in the file.
 *      Passing 0 returns the start of the first location,
 *      passing 1 returns the start of the second location,
 *      passing 2 returns the start of the first segment frame.
 * \return
 *      The offset into the file where a copy of the superblock may
 *      be located.
 */
uint64_t
SingleFileStorage::offsetOfSuperblockFrame(uint32_t index) const
{
    return index *
           ((sizeof(Superblock) + BLOCK_SIZE - 1) / BLOCK_SIZE) *
           BLOCK_SIZE;
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
    uint64_t logSpace = offsetOfSegmentFrame(segmentFrames);

    LOG(DEBUG, "Reserving %lu bytes of log space", logSpace);
    int r = ftruncate(fd, logSpace);
    if (r == -1)
        throw BackupStorageException(HERE,
                "Couldn't reserve storage space for backup", errno);
}

/**
 * Try to read one of the multiple storage locations which may contain
 * a superblock.
 *
 * \param superblockFrame
 *      Which of the multiple superblock storage locations to read.
 *      Currently, there are two in order to avoid corrupting an existing
 *      superblock if a failure occurs in the middle of an update.
 * \return
 *      The superblock stored at \a superblockFrame is returned if
 *      it was loaded the stored checksum was correct.  Otherwise,
 *      if there was a problem reading the file or the contents appears
 *      to be damaged or incomplete the returned value is empty.
 */
Tub<BackupStorage::Superblock>
SingleFileStorage::tryLoadSuperblock(uint32_t superblockFrame)
{
    Memory::unique_ptr_free block(
        Memory::xmemalign(HERE, getpagesize(), BLOCK_SIZE), std::free);
    struct FileContents {
        Superblock superblock;
        Crc32C::ResultType checksum;
    } __attribute__((packed));
    uint64_t offset = offsetOfSuperblockFrame(superblockFrame);
    ssize_t r = pread(fd, block.get(), BLOCK_SIZE, offset);
    if (r == -1) {
        LOG(NOTICE, "Couldn't read superblock from superblock frame %u: %s",
            superblockFrame, strerror(errno));
        return {};
    } else if (r < BLOCK_SIZE) {
        LOG(NOTICE, "Couldn't read superblock from superblock frame %u: "
            "read was short (read %ld bytes of %u)",
            superblockFrame, r, BLOCK_SIZE);
        return {};
    }
    FileContents* fileContents = reinterpret_cast<FileContents*>(block.get());
    Superblock& superblock = fileContents->superblock;

    Crc32C crc;
    crc.update(&superblock, sizeof(superblock));
    uint32_t checksum = crc.getResult();

    // Check stored checksum against the computed checksum for stored data.
    if (fileContents->checksum != checksum) {
        LOG(NOTICE, "Stored superblock had a bad checksum: "
            "stored checksum was %x, but stored data had checksum %x",
            fileContents->checksum, checksum);
        return {};
    }
    char& endOfName =
        superblock.clusterName[sizeof(superblock.clusterName) - 1];
    if (endOfName != '\0')
        DIE("Stored superblock's cluster name should end in \\0; "
            "this should never happen unless there is a software bug");

    return { superblock };
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

/**
 * No-op for InMemoryStorage since it can't actually persist a superblock.
 *
 * \param serverId
 *      Ignored.
 * \param clusterName
 *      Ignored.
 * \param frameSkipMask
 *      Ignored.
 */
void
InMemoryStorage::resetSuperblock(ServerId serverId,
                                 const string& clusterName,
                                 uint32_t frameSkipMask)
{
}

/**
 * Returns a default superblock since InMemoryStorage has no persistence.
 *
 * \return
 *      A default constructed superblock corresponding to no known prior
 *      server id and an unnamed cluster.
 */
BackupStorage::Superblock
InMemoryStorage::loadSuperblock()
{
    return {};
}

} // namespace RAMCloud
