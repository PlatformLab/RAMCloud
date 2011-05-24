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
#include "BenchUtil.h"
#include "Common.h"
#include "Dispatch.h"
#include "Initialize.h"
#include "Metrics.h"

namespace RAMCloud {

Dispatch *dispatch = NULL;
Initialize _(dispatch);

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
 * Constructor for Dispatch objects.
 */
Dispatch::Dispatch()
    : currentTime(rdtsc())
    , pollers()
    , files()
    , epollFd(-1)
    , epollThread()
    , readyFd(-1)
    , fileInvocationSerial(0)
    , timers()
    , earliestTriggerTime(0)
    , dispatchThreadToken()
    , mutex()
    , lockNeeded(0)
    , locked(0)
{
    exitPipeFds[0] = exitPipeFds[1] = -1;
    dispatchThreadToken.reset(new int);
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
            files[i]->event = FileEvent::NONE;
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
    currentTime = rdtsc();
    if (lockNeeded.load() != 0) {
        // Someone wants us locked. Indicate that we are locked,
        // then wait for the lock to be released.
        locked.store(1);
        while (lockNeeded.load() != 0) {
            // Empty loop body.
        }
        locked.store(0);
    }
    for (uint32_t i = 0; i < pollers.size(); i++) {
        pollers[i]->poll();
    }
    if (readyFd >= 0) {
        int fd = readyFd;
        readyFd = -1;
        File* file = files[fd];
        if (file) {
            int id = fileInvocationSerial + 1;
            if (id == 0) {
                id++;
            }
            fileInvocationSerial = id;
            file->invocationId = id;
            file->handleFileEvent();

            // Must reenable the event for this file, since it was automatically
            // disabled by epoll.  However, it's possible that the handler
            // deleted the File; don't do anything if that appears to have
            // happened.  By using a unique invocation id instead of a simple
            // boolean we can detect if the old handler was deleted and a new
            // handler was created for the same fd.
            if ((files[fd] == file) && (file->invocationId == id)) {
                file->invocationId = 0;
                file->setEvent(file->event);
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
 * Returns true if this thread is the one in which the object was created.
 */
bool
Dispatch::isDispatchThread()
{
    return dispatchThreadToken.get() != NULL;
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
 * \param event
 *      Invoke the object when any of the events specified by this
 *      parameter occur. If this is NONE then the file handler starts
 *      off inactive; it will not trigger until setEvent has been
 *      called.
 * \param dispatch
 *      Dispatch object that will manage this file handler (defaults
 *      to the global #RAMCloud::dispatch object).
 */
Dispatch::File::File(int fd, Dispatch::FileEvent event, Dispatch* dispatch)
        : owner(dispatch)
        , fd(fd)
        , event(NONE)
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
    if (event != NONE) {
        setEvent(event);
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
 * \param event
 *      Indicates the conditions under which this object should be invoked.
 */
void Dispatch::File::setEvent(FileEvent event)
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
    this->event = event;
    if (invocationId != 0) {
        // Don't communicate anything to epoll while a call to
        // operator() is in progress (don't want another instance of
        // the handler to be invoked until the first completes): we
        // will get another chance to update epoll state when the handler
        // completes.
        return;
    }
    if (event == READABLE) {
        epollEvent.events = EPOLLIN|EPOLLONESHOT;
    } else if (event == WRITABLE) {
        epollEvent.events = EPOLLOUT|EPOLLONESHOT;
    } else if (event == READABLE_OR_WRITABLE) {
        epollEvent.events = EPOLLIN|EPOLLOUT|EPOLLONESHOT;
    } else  {
        epollEvent.events = 0;
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
 * \param dispatch
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
            owner->readyFd = events[i].data.fd;
        }
    }
}

/**
 * Construct a timer but do not start it: it will not fire until one of
 * the #startXXX methods is invoked.
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
 *      returned by #rdtsc) from Dispatch::currentTime.
 */
Dispatch::Timer::Timer(uint64_t cycles, Dispatch* dispatch)
        : owner(dispatch), triggerTime(0), slot(-1)
{
    startCycles(cycles);
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
 * \param cycles
 *      Time at which the timer should trigger, measured in cycles (the units
 *      returned by #rdtsc) from Dispatch::currentTime.  If the timer was
 *      already running the old trigger time is forgotten
 */
void Dispatch::Timer::startCycles(uint64_t cycles)
{
    if (owner == NULL) {
        // Our Dispatch no longer exists, so there's nothing to do here.
        return;
    }
    CHECK_LOCK;

    triggerTime = owner->currentTime + cycles;
    if (slot < 0) {
        slot = downCast<unsigned>(owner->timers.size());
        owner->timers.push_back(this);
    }
    if (triggerTime < owner->earliestTriggerTime) {
        owner->earliestTriggerTime = triggerTime;
    }
}

/**
 * Start this timer running.
 *
 * \param micros
 *      Time at which the timer should trigger, measured in microseconds from
 *      Dispatch::currentTime.  If the timer was already running the old
 *      trigger time is forgotten
 */
void Dispatch::Timer::startMicros(uint64_t micros)
{
    startCycles(nanosecondsToCycles(1000*micros));
}

/**
 * Start this timer running.
 *
 * \param millis
 *      Time at which the timer should trigger, measured in milliseconds from
 *      Dispatch::currentTime.  If the timer was already running the old
 *      trigger time is forgotten
 */
void Dispatch::Timer::startMillis(uint64_t millis)
{
    startCycles(nanosecondsToCycles(1000*1000*millis));
}

/**
 * Start this timer running.
 *
 * \param seconds
 *      Time at which the timer should trigger, measured in seconds from
 *      Dispatch::currentTime.  If the timer was already running the old
 *      trigger time is forgotten
 */
void Dispatch::Timer::startSeconds(uint64_t seconds)
{
    startCycles(nanosecondsToCycles(1000*1000*1000*seconds));
}

/**
 * Stop this timer, if it was running. After this call, the timer won't
 * trigger until a #startXXX method is invoked again.
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

/**
 * Construct a Lock object, which means we must lock the dispatch
 * thread unless we are currently executing in the dispatch thread.
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
    lock.construct(dispatch->mutex);
    dispatch->lockNeeded.store(1);
    while (dispatch->locked.load() == 0) {
        // Empty loop: spin-wait for the dispatch thread to lock itself.
    }
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

    dispatch->lockNeeded.store(0);

    // We must not return (which will release the mutex) until the
    // dispatch thread has cleared the #locked variable. Otherwise
    // there is a race where another thread could think the dispatch
    // thread is locked when it is really about to unlock itself.
    while (dispatch->locked.load() != 0) {
        // Empty loop: spin-wait for the dispatch thread to unlock.
    }
}

} // namespace RAMCloud
