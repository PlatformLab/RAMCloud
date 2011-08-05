/* Copyright (c) 2011 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <sys/epoll.h>

#include "ShortMacros.h"
#include "Common.h"
#include "Cycles.h"
#include "Dispatch.h"
#include "Fence.h"
#include "Initialize.h"
#include "Metrics.h"
#include "NoOp.h"

namespace RAMCloud {

Dispatch *dispatch = NULL;
static Initialize _(Dispatch::init);

/**
 * Default object used to make system calls.
 */
static Syscall defaultSyscall;

/**
 * Used by this class to make all system calls.  In normal production
 * use it points to defaultSyscall; for testing it points to a mock
 * object.
 */
Syscall* Dispatch::sys = &defaultSyscall;

// The following is a debugging assertion used in many methods to make sure
// that either (a) the method was invoked in the dispatch thread or (b) the
// dispatcher was been locked before calling the method.
#define CHECK_LOCK assert((owner->locked.load() != 0) || \
        owner->isDispatchThread())

/**
 * Perform once-only overall initialization for the Dispatch class, such
 * as creating the global #dispatch object.  This method is invoked
 * automatically during initialization, but it may be invoked explicitly
 * by other modules to ensure that initialization occurs before those modules
 * initialize themselves.
 */
void
Dispatch::init() {
    if (dispatch == NULL) {
        dispatch = new Dispatch();
    }
}

/**
 * Constructor for Dispatch objects.
 */
Dispatch::Dispatch()
    : currentTime(Cycles::rdtsc())
    , pollers()
    , files()
    , epollFd(-1)
    , epollThread()
    , readyFd(-1)
    , readyEvents(0)
    , fileInvocationSerial(0)
    , timers()
    , earliestTriggerTime(0)
    , ownerId(ThreadId::get())
    , mutex()
    , lockNeeded(0)
    , locked(0)
{
    exitPipeFds[0] = exitPipeFds[1] = -1;
}

/**
 * Destructor for Dispatch objects.  All existing Pollers, Timers, and Files
 * become useless.
 */
Dispatch::~Dispatch()
{
    if (epollThread) {
        // Writing data to the pipe below signals the epoll thread that it
        // should exit.
        sys->write(exitPipeFds[1], "x", 1);
        epollThread->join();
        epollThread.destroy();
        sys->close(exitPipeFds[0]);
        sys->close(exitPipeFds[1]);
        exitPipeFds[1] = exitPipeFds[0] = -1;
    }
    if (epollFd >= 0) {
        sys->close(epollFd);
        epollFd = -1;
    }
    for (uint32_t i = 0; i < pollers.size(); i++) {
        pollers[i]->owner = NULL;
        pollers[i]->slot = -1;
    }
    pollers.clear();
    for (uint32_t i = 0; i < files.size(); i++) {
        if (files[i] != NULL) {
            files[i]->owner = NULL;
            files[i]->active = false;
            files[i]->events = 0;
            files[i] = NULL;
        }
    }
    readyFd = -1;
    while (timers.size() > 0) {
        Timer* t = timers.back();
        t->stop();
        t->owner = NULL;
    }
}

/**
 * Check to see if any events need handling.
 *
 * \return
 *      True means that we found some work to do; false means we looked
 *      around but there was nothing to do.
 */
void
Dispatch::poll()
{
    assert(isDispatchThread());
    currentTime = Cycles::rdtsc();
    if (lockNeeded.load() != 0) {
        // Someone wants us locked. Indicate that we are locked,
        // then wait for the lock to be released.
        Fence::leave();
        locked.store(1);
        while (lockNeeded.load() != 0) {
            // Empty loop body.
        }
        locked.store(0);
        Fence::enter();
    }
    for (uint32_t i = 0; i < pollers.size(); i++) {
        pollers[i]->poll();
    }
    if (readyFd >= 0) {
        int fd = readyFd;

        // Make sure that the read of readyEvents doesn't get reordered either
        // before we see readyFd or after we change it (otherwise could
        // read the wrong value).
        __asm__ __volatile__("lfence");
        int events = readyEvents;
        __asm__ __volatile__("lfence");
        readyFd = -1;
        File* file = files[fd];
        if (file) {
            int id = fileInvocationSerial + 1;
            if (id == 0) {
                id++;
            }
            fileInvocationSerial = id;
            file->invocationId = id;
            file->handleFileEvent(events);

            // Must reenable the event for this file, since it was automatically
            // disabled by epoll.  However, it's possible that the handler
            // deleted the File; don't do anything if that appears to have
            // happened.  By using a unique invocation id instead of a simple
            // boolean we can detect if the old handler was deleted and a new
            // handler was created for the same fd.
            if ((files[fd] == file) && (file->invocationId == id)) {
                file->invocationId = 0;
                file->setEvents(file->events);
            }
        }
    }
    if (currentTime >= earliestTriggerTime) {
        // Looks like a timer may have triggered. Check all the timers and
        // invoke any that have triggered.
        for (uint32_t i = 0; i < timers.size(); ) {
            Timer* timer = timers[i];
            if (timer->triggerTime <= currentTime) {
                timer->stop();
                timer->handleTimerEvent();
            } else {
                // Only increment i if the current timer did not trigger.
                // If it did trigger, it got removed from the vector and
                // we need to process the timer that got moved its place.
                i++;
            }
        }

        // Compute a new value for earliestTriggerTime. Can't do this
        // in the loop above, because one timer handler could delete
        // another, which can rearrange the list and cause us to miss
        // a trigger time.
        earliestTriggerTime = ~(0ull);
        for (uint32_t i = 0; i < timers.size(); i++) {
            Timer* timer = timers[i];
            if (timer->triggerTime < earliestTriggerTime) {
                earliestTriggerTime = timer->triggerTime;
            }
        }
    }
}

/**
 * Construct a Poller.
 *
 * \param dispatch
 *      Dispatch object through which the poller will be invoked (defaults
 *      to the global #RAMCloud::dispatch object).
 */
Dispatch::Poller::Poller(Dispatch *dispatch)
    : owner(dispatch), slot(downCast<unsigned>(owner->pollers.size()))
{
    CHECK_LOCK;
    owner->pollers.push_back(this);
}

/**
 * Destroy a Poller.
 */
Dispatch::Poller::~Poller()
{
    if (slot < 0) {
        return;
    }
    CHECK_LOCK;

    // Erase this Poller from the vector by overwriting it with the
    // poller that used to be the last one in the vector.
    //
    // Note: this approach is reentrant (it is safe to delete a
    // poller from a poller callback, which means that the poll
    // method is in the middle of scanning the list of all pollers;
    // the worst that will happen is that the poller that got moved
    // may not be invoked in the current scan).
    owner->pollers[slot] = owner->pollers.back();
    owner->pollers[slot]->slot = slot;
    owner->pollers.pop_back();
    slot = -1;
}

/**
 * Construct a file handler.
 *
 * \param fd
 *      File descriptor of interest. Note: at most one Dispatch::File
 *      may be created for a single file descriptor.
 * \param events
 *      Invoke the object when any of the events specified by this
 *      parameter occur (OR-ed combination of FileEvent values). If this
 *      is 0 then the file handler starts off inactive; it will not
 *      trigger until setEvents has been called.
 * \param dispatch
 *      Dispatch object that will manage this file handler (defaults
 *      to the global #RAMCloud::dispatch object).
 */
Dispatch::File::File(int fd, int events, Dispatch* dispatch)
        : owner(dispatch)
        , fd(fd)
        , events(0)
        , active(false)
        , invocationId(0)
{
    CHECK_LOCK;

    // Start the polling thread if it doesn't already exist (and also create
    // the epoll file descriptor and the exit pipe).
    if (!owner->epollThread) {
        owner->epollFd = sys->epoll_create(10);
        if (owner->epollFd < 0) {
            throw FatalError(HERE, "epoll_create failed in Dispatch", errno);
        }
        if (sys->pipe(owner->exitPipeFds) != 0) {
            throw FatalError(HERE,
                    "Dispatch couldn't create exit pipe for epoll thread",
                    errno);
        }
        epoll_event epollEvent;
        // The following statement is not needed, but without it valgrind
        // will generate false errors about uninitialized data.
        epollEvent.data.u64 = 0;
        epollEvent.events = EPOLLIN|EPOLLONESHOT;

        // -1 fd signals to epoll thread to exit.
        epollEvent.data.fd = -1;
        if (sys->epoll_ctl(owner->epollFd, EPOLL_CTL_ADD,
                owner->exitPipeFds[0], &epollEvent) != 0) {
            throw FatalError(HERE,
                    "Dispatch couldn't set epoll event for exit pipe",
                    errno);
        }
        owner->epollThread.construct(Dispatch::epollThreadMain, owner);
    }

    if (owner->files.size() <= static_cast<uint32_t>(fd)) {
        owner->files.resize(2*fd);
    }
    if (owner->files[fd] != NULL) {
            throw FatalError(HERE, "can't have more than 1 Dispatch::File "
                    "for a file descriptor");
    }
    owner->files[fd] = this;
    if (events != 0) {
        setEvents(events);
    }
}

/**
 * Destroy a file handler.
 */
Dispatch::File::~File()
{
    if (owner == NULL) {
        // Dispatch object has already been deleted; don't do anything.
        return;
    }
    CHECK_LOCK;

    if (active) {
        // Note: don't worry about errors here. For example, it's
        // possible that the file was closed before this destructor
        // was invoked, in which case EBADF will occur.
        sys->epoll_ctl(owner->epollFd, EPOLL_CTL_DEL, fd, NULL);
    }
    owner->files[fd] = NULL;
}

/**
 * Specify the events of interest for this file handler.
 *
 * \param events
 *      Indicates the conditions under which this object should be invoked
 *      (OR-ed combination of FileEvent values).
 */
void Dispatch::File::setEvents(int events)
{
    if (owner == NULL) {
        // Dispatch object has already been deleted; don't do anything.
        return;
    }
    CHECK_LOCK;

    epoll_event epollEvent;
    // The following statement is not needed, but without it valgrind
    // will generate false errors about uninitialized data.
    epollEvent.data.u64 = 0;
    this->events = events;
    if (invocationId != 0) {
        // Don't communicate anything to epoll while a call to
        // operator() is in progress (don't want another instance of
        // the handler to be invoked until the first completes): we
        // will get another chance to update epoll state when the handler
        // completes.
        return;
    }
    epollEvent.events = 0;
    if (events & READABLE) {
        epollEvent.events |= EPOLLIN|EPOLLONESHOT;
    }
    if (events & WRITABLE) {
        epollEvent.events |= EPOLLOUT|EPOLLONESHOT;
    }
    epollEvent.data.fd = fd;
    if (sys->epoll_ctl(owner->epollFd,
            active ? EPOLL_CTL_MOD : EPOLL_CTL_ADD, fd, &epollEvent) != 0) {
        throw FatalError(HERE, format("Dispatch couldn't set epoll event "
                "for fd %d", fd), errno);
    }
    active = true;
}

/**
 * This function is invoked in a separate thread; its job is to invoke
 * epoll and report back whenever epoll returns information about an
 * event.  By putting this functionality in a separate thread the main
 * poll loop never needs to incur the overhead of a kernel call.
 *
 * \param owner
 *      The Dispatch object on whose behalf this thread is working.
 */
void Dispatch::epollThreadMain(Dispatch* owner) {
#define MAX_EVENTS 10
    struct epoll_event events[MAX_EVENTS];
    while (true) {
        int count = sys->epoll_wait(owner->epollFd, events, MAX_EVENTS, -1);
        if (count <= 0) {
            if (count == 0) {
                LOG(WARNING, "epoll_wait returned no events in "
                    "Dispatch::epollThread");
                continue;
            } else {
                if (errno == EINTR) continue;
                LOG(ERROR, "epoll_wait failed in Dispatch::epollThread: %s",
                        strerror(errno));
                return;
            }
        }

        // Signal all of the ready file descriptors back to the main
        // polling loop using a shared memory location.
        for (int i = 0; i < count; i++) {
            int fd = events[i].data.fd;
            int readyEvents = 0;
            if (events[i].events & EPOLLIN) {
                readyEvents |= READABLE;
            }
            if (events[i].events & EPOLLOUT) {
                readyEvents |= WRITABLE;
            }
            if (fd == -1) {
                // This is a special value associated with exitPipeFd[0],
                // and indicates that this thread should exit.
                TEST_LOG("done");
                return;
            }
            while (owner->readyFd >= 0) {
                // The main polling loop hasn't yet noticed the last file
                // that became ready; wait for the shared memory location
                // to clear again.  This loop busy-waits but yields the
                // CPU to any other runnable threads.
                sched_yield();
            }
            owner->readyEvents = readyEvents;
            // The following line guarantees that the modification of
            // owner->readyEvents will be visible in memory before the
            // modification of readyFd.
            __asm__ __volatile__("sfence");
            owner->readyFd = events[i].data.fd;
        }
    }
}

/**
 * Construct a timer but do not start it: it will not fire until #start
 * is invoked.
 *
 * \param dispatch
 *      Dispatch object that will manage this timer (defaults to the
 *      global #RAMCloud::dispatch object).
 */
Dispatch::Timer::Timer(Dispatch* dispatch)
    : owner(dispatch), triggerTime(0), slot(-1)
{
}

/**
 * Construct a timer and start it running.
 *
 * \param dispatch
 *      Dispatch object that will manage this timer (defaults to the
 *      global #RAMCloud::dispatch object).
 * \param cycles
 *      Time at which the timer should trigger, measured in cycles (the units
 *      returned by #Cycles::rdtsc).
 */
Dispatch::Timer::Timer(uint64_t cycles, Dispatch* dispatch)
        : owner(dispatch), triggerTime(0), slot(-1)
{
    start(cycles);
}

/**
 * Destructor for Timers.
 */
Dispatch::Timer::~Timer()
{
    if (owner != NULL) stop();
}

/**
 * Returns true if the timer is currently running, false if it isn't.
 */
bool Dispatch::Timer::isRunning()
{
    return slot >= 0;
}

/**
 * Start this timer running.
 *
 * \param rdtscTime
 *      The timer will trigger when #Cycles::rdtsc() returns a value at least this
 *      large.  If the timer was already running, the old trigger time is
 *      forgotten.
 */
void Dispatch::Timer::start(uint64_t rdtscTime)
{
    if (owner == NULL) {
        // Our Dispatch no longer exists, so there's nothing to do here.
        return;
    }
    CHECK_LOCK;

    triggerTime = rdtscTime;
    if (slot < 0) {
        slot = downCast<unsigned>(owner->timers.size());
        owner->timers.push_back(this);
    }
    if (triggerTime < owner->earliestTriggerTime) {
        owner->earliestTriggerTime = triggerTime;
    }
}

/**
 * Stop this timer, if it was running. After this call, the timer won't
 * trigger until #start is invoked again.
 */
void Dispatch::Timer::stop()
{
    if (slot < 0) {
        return;
    }
    CHECK_LOCK;

    // Remove this Timer from the timers vector by overwriting its slot
    // with the timer that used to be the last one in the vector.
    //
    // Note: this approach using a vector is reentrant.  It is safe
    // to delete a Timer while executing a timer callback, which means
    // that Dispatch::poll is in the middle of scanning the list of all
    // Timers; the worst that will happen is that the Timer that got
    // moved may not be invoked in the current scan).
    owner->timers[slot] = owner->timers.back();
    owner->timers[slot]->slot = slot;
    owner->timers.pop_back();
    slot = -1;
}

#if TESTING
/**
 * A thread-local flag that says whether this thread is currently executing
 * within a Dispatch::Lock. It is used to throw an assertion failure if a
 * thread ever tries to acquire a second Dispatch::Lock, which is not allowed.
 */
static __thread bool thisThreadHasDispatchLock = false;
#else
static NoOp<int> thisThreadHasDispatchLock;
#endif

/**
 * Construct a Lock object, which means we must lock the dispatch
 * thread unless we are currently executing in the dispatch thread.
 * These are not recursive (you can't safely create a Lock object if
 * someone up the stack already has one).
 *
 * \param dispatch
 *      Dispatch object to lock (defaults to the global #RAMCloud::dispatch
 *      object).
 */
Dispatch::Lock::Lock(Dispatch* dispatch)
    : dispatch(dispatch), lock()
{
    if (dispatch->isDispatchThread()) {
        return;
    }

    assert(!thisThreadHasDispatchLock);
    thisThreadHasDispatchLock = true;
    lock.construct(dispatch->mutex);

    // It's possible that when we arrive here the dispatch thread hasn't
    // finished unlocking itself after the previous lock-unlock cycle.
    // We need to make sure for this to complete; otherwise we could
    // get confused below and return before the dispatch thread has
    // re-locked itself.
    while (dispatch->locked.load() != 0) {
        // Empty loop.
    }

    // The following statements ensure that the preceding load completes
    // before the following store (reordering could cause deadlock).
    Fence::sfence();
    Fence::lfence();
    dispatch->lockNeeded.store(1);
    while (dispatch->locked.load() == 0) {
        // Empty loop: spin-wait for the dispatch thread to lock itself.
    }
    Fence::enter();
}

/**
 * Destructor for Lock objects: unlock the dispatch thread if we
 * locked it.
 */
Dispatch::Lock::~Lock()
{
    if (!lock) {
        // We never acquired the mutex; this means we're running in the
        // dispatch thread so there's nothing for us to do here.
        return;
    }

    Fence::leave();
    dispatch->lockNeeded.store(0);
    thisThreadHasDispatchLock = false;
}

} // namespace RAMCloud
