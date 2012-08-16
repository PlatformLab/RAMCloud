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

#ifndef RAMCLOUD_ASYNCIOQUEUE_H
#define RAMCLOUD_ASYNCIOQUEUE_H

#include <condition_variable>
#include <deque>
#include <mutex>
#include <queue>
#include <thread>

#include "Common.h"
#include "Syscall.h"
#include "Tub.h"

namespace RAMCloud {

class AsyncIoQueue;

/**
 * Descriptor for a single IO operation on a file which can be performed
 * asynchronously using an AsyncIoQueue. Behavior is undefined if a single
 * AsyncIo is simultaneously enqueued with different AsyncIoQueues.
 */
class AsyncIo {
  PUBLIC:
    enum Operation { READ, WRITE };
    enum Priority { LOW, NORMAL, HIGH };

    /**
     * Create an IO descriptor which can be enqueued with an AsyncIoQueue for
     * asynchronous file IO. After construction IO service can be requested
     * with AsyncIoQueue::enqueueIo(). Once enqueued this descriptor must not
     * be destroyed before isDone() returns true. If this descriptor is
     * destroyed earlier its destructor will block until the IO completes.
     *
     * \param operation
     *      READ or WRITE.
     * \param priority
     *      LOW, NORMAL, or HIGH. Of all IOs enqueued IOs are performed in the
     *      order of priority and then in FIFO order within a single priority.
     * \param fd
     *      File descriptor of the file to perform \a operation on.
     * \param buffer
     *      Starting address of data to be written for WRITEs. Starting address
     *      of where data should be placed for READs. For a read the region
     *      starting at this address is invalid until isDone() returns true.
     *      For a write the region start at this address must remain stable
     *      until isDone() returns true.
     * \param length
     *      Bytes to be read or written transferred.
     * \param offset
     *      Byte offset into the file where data should be read from or be
     *      written to.
     */
    AsyncIo(Operation operation,
            Priority priority,
            int fd,
            void* buffer,
            size_t length,
            size_t offset)
        : operation(operation)
        , priority(priority)
        , fd(fd)
        , buffer(buffer)
        , length(length)
        , offset(offset)
        , done()
        , enqueuedTimestamp()
    {}

    /**
     * Blocks until the requested IO completes to prevent the AsyncIoQueue from
     * having a dangling pointer.
     */
    ~AsyncIo() {
        while (enqueuedTimestamp && !isDone());
    }

    /**
     * Returns true when the requested IO has completed; false otherwise.
     * Once isDone() returns true the caller is free use/reuse/free the memory
     * at #buffer.
     */
    bool isDone() {
        return done;
    }

  PRIVATE:
    /// Whether this IO should perform a READ or a WRITE.
    Operation operation;

    /**
     * LOW, NORMAL, or HIGH. Of all IOs enqueued IOs are performed in the
     * order of priority and then in FIFO order within a single priority.
     */
    Priority priority;

    /// File descriptor of the file to perform #operation on.
    int fd;

    /**
     * Starting address of data to be written for WRITEs; starting address
     * of where data should be placed for READs. For a read the region
     * starting at this address is invalid until isDone() returns true.
     * For a write the region start at this address must remain stable
     * until isDone() returns true.
     */
    void* buffer;

    /// Bytes to be read or written transferred.
    size_t length;

    /// Byte offset into #fd where data should be read from or be written to.
    off_t offset;

    /**
     * Whether this IO has been completed yet. Cleared initially and when the
     * IO descriptor is enqueued with the AsyncIoQueue; set by the AsyncIoQueue
     * once the IO has been performed.
     */
    bool done;

    /**
     * Logical timestamp that tracks "when" this IO was enqueued with an
     * AsyncIoQueue. Used to order operations within a single priority level.
     */
    uint64_t enqueuedTimestamp;

    friend class AsyncIoQueue;
    DISALLOW_COPY_AND_ASSIGN(AsyncIo);
};

/**
 * Dispatches file IO requests to the OS in the background one-at-a-time.
 * Users submit AsyncIos to the queue which asynchronously marks them as done
 * when the requested operation completes. Allows users to specify priority on 
 * each IO; performs IOs FIFO within each priority level. See AsyncIo to
 * create an IO descriptor and use enqueueIo() to submit it for IO.
 */
class AsyncIoQueue {
  PUBLIC:
    AsyncIoQueue();
    ~AsyncIoQueue();

    void main();
    void start();
    void halt();

    void enqueueIo(AsyncIo* io);

    void quiesce();

  PRIVATE:
    AsyncIo* popNextIo();
    void performIo(AsyncIo* io);
    static bool asyncIoPointerLess(const AsyncIo* left, const AsyncIo* right);

    typedef std::unique_lock<std::mutex> Lock;

    /// Protects all members for thread-safety.
    std::mutex mutex;

    /**
     * Used to wait on new work, completion of existing work, and exit
     * conditions.
     * Notified in three cases:
     * 1) An AsyncIo has been enqueued.
     * 2) An AsyncIo has finished reading/writing.
     * 3) #running becomes false.
     */
    std::condition_variable changes;

    /**
     * Drives main() waiting for new AsyncIo, dispatching them to the OS
     * one-at-a-time, and marking them as complete to notify waiters. Exits
     * if running is set to false.
     */
    Tub<std::thread> thread;

    /// When false exit after finishing the single AsyncIo it is performing.
    bool running;

    typedef std::priority_queue<AsyncIo*,
                                std::deque<AsyncIo*>,
                                bool (*)(const AsyncIo*, const AsyncIo*)>
            AioPriorityQueue;
    /**
     * Contains AsyncIo pointers which have been enqueued for IO. Orders IOs
     * first by priority and then by the order in which they were enqueued.
     * Only manipulated by enqueueIo() and popNextIo().
     */
    AioPriorityQueue queue;

    /**
     * Counts AsyncIos that have been enqueued.
     * Has two uses:
     * Allows quiesce to ensure no IOs are enqueued or underway.
     * And, acts as a logical timestamp to give total order to IOs within the
     * same priority (see AsyncIo::enqueuedTimestamp).
     */
    uint64_t enqueuedCount;

    /**
     * Counts AsyncIos which have completed.
     * Used by quiesce to determine when all work enqueued is completed.
     */
    uint64_t doneCount;

    /**
     * Used by to make all system calls. In normal production use sys passes
     * calls through to the OS provided implementation. During testing calls
     * can be mocked out.
     */
    static Syscall* sys;

    DISALLOW_COPY_AND_ASSIGN(AsyncIoQueue);
};

} // namespace RAMCloud

#endif
