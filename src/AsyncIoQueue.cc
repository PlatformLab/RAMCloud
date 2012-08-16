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

#include "AsyncIoQueue.h"
#include "Logger.h"
#include "ShortMacros.h"

namespace RAMCloud {

/// Default object used to make system calls.
static Syscall defaultSyscall;

/**
 * Used by this class to make all system calls. In normal production
 * use it points to defaultSyscall; for testing it points to a mock
 * object.
 */
Syscall* AsyncIoQueue::sys = &defaultSyscall;

/**
 * Create an AsyncIoQueue; start() must be called before enqueued AsyncIo
 * requests will be processed.
 */
AsyncIoQueue::AsyncIoQueue()
    : mutex()
    , changes()
    , thread()
    , running()
    , queue(AsyncIoQueue::asyncIoPointerLess)
    , enqueuedCount()
    , doneCount()
{
} 

/**
 * Halts outstanding IO and destroys the thread used by this instance to
 * perform IO in the background.
 */
AsyncIoQueue::~AsyncIoQueue()
{
    halt();
}

/**
 * Waits for new AsyncIo requests, dispatches them to the OS one-at-a-time,
 * and marks them as complete to notify waiters.
 * Exits when halt() is called; may wait for a single IO to complete before
 * returning.
 */
void
AsyncIoQueue::main()
try {
    while (AsyncIo* io = popNextIo())
        performIo(io);
} catch (const std::exception& e) {
    LOG(ERROR, "Fatal error in AsyncIoQueue: %s", e.what());
    throw;
} catch (...) {
    LOG(ERROR, "Unknown fatal error in AsyncIoQueue.");
    throw;
}

/**
 * Start performing enqueued IOs in the background.
 * Calling start() on an instance that is already started has no effect.
 */
void
AsyncIoQueue::start()
{
    Lock lock(mutex);
    if (running)
        return;
    running = true;
    thread.construct(&AsyncIoQueue::main, this);
}

/**
 * Stop performing enqueued IOs. Calling halt() on an instance that is
 * already halted or has never been started has no effect.
 */
void
AsyncIoQueue::halt()
{
    Lock lock(mutex);
    if (!running)
        return;
    running = false;
    changes.notify_all();
    lock.unlock();
    thread->join();
    thread.destroy();
}

/**
 * Enqueue an IO request that should be performed asynchronously. After return
 * the caller can begin polling AsyncIo::isDone() to determine when the
 * requested IO has completed. Until the \a io->length bytes at \a io->buffer
 * must remain allocated. Those bytes may not be modified on a WRITE until
 * isDone() returns true. Those bytes may be invalid on a READ until isDone()
 * returns true.
 *
 * AsyncIos will be performed first according to priority order and then
 * in-order of enqueuing within a single priority.
 *
 * \param io
 *      IO descriptor that provides the details of the operation. See AsyncIo
 *      for details on the meaning of each of the fields. Behavior is undefined
 *      if \a io is already enqueued with a different AsyncIoQueue.
 */
void
AsyncIoQueue::enqueueIo(AsyncIo* io)
{
    Lock lock(mutex);
    io->done = false;
    io->enqueuedTimestamp = ++enqueuedCount;
    queue.push(io);
    changes.notify_all();
}

/**
 * Wait until all IO has completed; NOTICE this call DOES NOT block out new
 * operations and is prone to being starved out. Used primarily by high-level
 * RAMCloud system tests to ensure all started backup operations have made it
 * to storage.
 */
void
AsyncIoQueue::quiesce()
{
    Lock lock(mutex);
    while (running && !queue.empty() && enqueuedCount != doneCount)
        changes.wait(lock);
}

// - private -

/**
 * Returns the next AsyncIo that should be performed, obeying the
 * readers-before-writers priority constraint. If no operations are
 * enqueued for IO then this call blocks. Returns NULL when the
 * AsyncIoQueue has been asked to halt.
 */
AsyncIo*
AsyncIoQueue::popNextIo()
{
    Lock lock(mutex);
    while (running && queue.empty())
        changes.wait(lock);
    if (!running)
        return NULL;

    AsyncIo* io = queue.top();
    queue.pop();
    return io;
}

/**
 * Synchronously do a single read or write IO. If the IO fails in any
 * way a message is logged and FatalException is thrown; it is expected
 * this will crash the async io thread and the entire RAMCloud process.
 * 
 * \param io
 *      AsyncIo to be performed. Marked as done when the io completes.
 */
void
AsyncIoQueue::performIo(AsyncIo* io)
{
    ssize_t r;
    if (io->operation == AsyncIo::READ)
        r = sys->pread(io->fd, io->buffer, io->length, io->offset);
    else
        r = sys->pwrite(io->fd, io->buffer, io->length, io->offset);
    if (r != static_cast<ssize_t>(io->length))
        DIE("Failure performing asynchronous IO: %s", strerror(errno));
    io->done = true;
    ++doneCount;
    changes.notify_all();
}

/**
 * Returns true if \a left should be loaded from disk LATER THAN \a right.
 * Based on the priority of the IOs and then using the order the ios
 * were enqueued to break ties.
 */
bool
AsyncIoQueue::asyncIoPointerLess(const AsyncIo* left, const AsyncIo* right) {
    if (!left && !right)
        return false;
    else if (!left)
        return true;
    else if (!right)
        return false;
    else if (left->priority == right->priority)
        return left->enqueuedTimestamp > right->enqueuedTimestamp;
    else
        return left->priority < right->priority;
}

} // namespace RAMCloud
