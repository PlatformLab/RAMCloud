/* Copyright (c) 2014-2016 Stanford University
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

#include "TimeTrace.h"

namespace RAMCloud {
__thread TimeTrace::Buffer* TimeTrace::threadBuffer = NULL;
std::vector<TimeTrace::Buffer*> TimeTrace::threadBuffers;
TimeTrace::TraceLogger* TimeTrace::backgroundLogger = NULL;
SpinLock TimeTrace::mutex("TimeTrace::mutex");
Atomic<int> TimeTrace::activeReaders(0);

/**
 * Creates a thread-private TimeTrace::Buffer object for the current thread,
 * if one doesn't already exist.
 */
void
TimeTrace::createThreadBuffer()
{
    SpinLock::Guard guard(mutex);
    if (threadBuffer == NULL) {
        threadBuffer = new Buffer;
        threadBuffers.push_back(threadBuffer);
    }
}

/**
 * Return a string containing all of the trace records from all of the
 * thread-local buffers.
 */
string
TimeTrace::getTrace()
{
    std::vector<TimeTrace::Buffer*> buffers;
    string s;

    // Make a copy of the list of traces, so we can do the actual tracing
    // without holding a lock and without fear of the list changing.
    {
        SpinLock::Guard guard(mutex);
        buffers = threadBuffers;
    }
    TimeTrace::printInternal(&buffers, &s);
    return s;
}

/**
 * This private method does most of the work for both printToLog and
 * getTrace.
 *
 * \param buffers
 *      Contains one or more TimeTrace::Buffers, whose contents will be merged
 *      in the resulting output. Note: some of the buffers may extend
 *      farther back in time than others. The output will cover only the
 *      time period covered by *all* of the traces, ignoring older entries
 *      from some traces.
 * \param s
 *      If non-NULL, refers to a string that will hold a printout of the
 *      time trace. If NULL, the trace will be printed on the system log.
 */
void
TimeTrace::printInternal(std::vector<TimeTrace::Buffer*>* buffers, string* s)
{
    bool printedAnything = false;

    // Holds the index of the next event to consider from each trace.
    std::vector<int> current;

    // Find the first (oldest) event in each trace. This will be events[0]
    // if we never completely filled the buffer, otherwise events[nextIndex+1].
    // This means we don't print the entry at nextIndex; this is convenient
    // because it simplifies boundary conditions in the code below.
    for (uint32_t i = 0; i < buffers->size(); i++) {
        TimeTrace::Buffer* buffer = buffers->at(i);
        int index = (buffer->nextIndex + 1) % Buffer::BUFFER_SIZE;
        if (buffer->events[index].format != NULL) {
            current.push_back(index);
        } else {
            current.push_back(0);
        }
    }

    // Decide on the time of the first event to be included in the output.
    // This is most recent of the oldest times in all the traces (an empty
    // trace has an "oldest time" of 0). The idea here is to make sure
    // that there's no missing data in what we print (if trace A goes back
    // farther than trace B, skip the older events in trace A, since there
    // might have been related events that were once in trace B but have since
    // been overwritten).
    uint64_t startTime = 0;
    for (uint32_t i = 0; i < buffers->size(); i++) {
        Event* event = &buffers->at(i)->events[current[i]];
        if ((event->format != NULL) && (event->timestamp > startTime)) {
            startTime = event->timestamp;
        }
    }
    RAMCLOUD_LOG(NOTICE, "Starting TSC %lu, cyclesPerSec %.0f", startTime,
            Cycles::perSecond());

    // Skip all events before the starting time.
    for (uint32_t i = 0; i < buffers->size(); i++) {
        TimeTrace::Buffer* buffer = buffers->at(i);
        while ((buffer->events[current[i]].format != NULL) &&
                (buffer->events[current[i]].timestamp < startTime) &&
                (current[i] != buffer->nextIndex)) {
            current[i] = (current[i] + 1) % Buffer::BUFFER_SIZE;
        }
    }

    // Each iteration through this loop processes one event (the one with
    // the earliest timestamp).
    double prevTime = 0.0;
    int eventsSinceCongestionCheck = 0;
    while (1) {
        TimeTrace::Buffer* buffer;
        Event* event;

        // Check all the traces to find the earliest available event.
        int currentBuffer = -1;
        uint64_t earliestTime = ~0;
        for (uint32_t i = 0; i < buffers->size(); i++) {
            buffer = buffers->at(i);
            event = &buffer->events[current[i]];
            if ((current[i] != buffer->nextIndex) && (event->format != NULL)
                    && (event->timestamp < earliestTime)) {
                currentBuffer = downCast<int>(i);
                earliestTime = event->timestamp;
            }
        }
        if (currentBuffer < 0) {
            // None of the traces have any more events to process.
            break;
        }
        printedAnything = true;
        buffer = buffers->at(currentBuffer);
        event = &buffer->events[current[currentBuffer]];
        current[currentBuffer] = (current[currentBuffer] + 1)
                % Buffer::BUFFER_SIZE;

        char message[1000];
        double ns = Cycles::toSeconds(event->timestamp - startTime) * 1e09;
        if (s != NULL) {
            if (s->length() != 0) {
                s->append("\n");
            }
#if TESTING
            snprintf(message, sizeof(message), "%8.1f ns (+%6.1f ns): ",
                    ns, ns - prevTime);
#else
            snprintf(message, sizeof(message), "T%d %8.1f ns (+%6.1f ns): ",
                    buffer->threadId, ns, ns - prevTime);
#endif
            s->append(message);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
            snprintf(message, sizeof(message), event->format, event->args[0],
                     event->args[1], event->args[2], event->args[3]);
#pragma GCC diagnostic pop
            s->append(message);
        } else {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
            snprintf(message, sizeof(message), event->format, event->args[0],
                     event->args[1], event->args[2], event->args[3]);
#pragma GCC diagnostic pop
#if TESTING
            RAMCLOUD_LOG(NOTICE, "%8.1f ns (+%6.1f ns): %s",
                    ns, ns - prevTime, message);
#else
            RAMCLOUD_LOG(NOTICE, "T%d %8.1f ns (+%6.1f ns): %s",
                    buffer->threadId, ns, ns - prevTime, message);
#endif

            // Make sure we're not monopolizing all of the buffer space
            // in the logger.
            eventsSinceCongestionCheck++;
            if (eventsSinceCongestionCheck > 100) {
                Logger::get().waitIfCongested();
                eventsSinceCongestionCheck = 0;
            }
        }
        prevTime = ns;
    }

    if (!printedAnything) {
        if (s != NULL) {
            s->append("No time trace events to print");
        } else {
            RAMCLOUD_LOG(NOTICE, "No time trace events to print");
        }
    }
}

/**
 * Combine all of the thread-local buffers and print them to the system log.
 */
void
TimeTrace::printToLog()
{
    std::vector<Buffer*> buffers;
    activeReaders.add(1);

    // Make a copy of the list of buffers, so we can do the actual tracing
    // without holding a lock and without fear of the list changing.
    {
        SpinLock::Guard guard(mutex);
        buffers = threadBuffers;
    }
    TimeTrace::printInternal(&buffers, NULL);
    activeReaders.add(-1);
}

/**
 * Merge all of the thread-local buffers and print them to the system
 * log. Do it in the background and return immediately, before it is done.
 *
 * \param dispatch
 *      Dispatch that can be used to schedule a WorkerTimer to
 *      log the time trace.
 */
void
TimeTrace::printToLogBackground(Dispatch* dispatch)

{
    SpinLock::Guard guard(mutex);
    // Don't do anything here if a previous background print is still
    // running.
    if (backgroundLogger) {
        if (!backgroundLogger->isFinished) {
            // A previous background print is still running, so don't
            // do anything here.
            return;
        }
        delete backgroundLogger;
    }

    // Stop all recording immediately (even before the WorkerTimer starts);
    // otherwise, new traces might overwrite the information we want to
    // print.
    activeReaders.add(1);
    backgroundLogger = new TraceLogger(dispatch);
}

/**
 * Discards all records in all of the thread-local buffers. Intended
 * primarily for unit testing.
 */
void
TimeTrace::reset()
{
    SpinLock::Guard guard(mutex);
    for (uint32_t i = 0; i < TimeTrace::threadBuffers.size(); i++) {
        TimeTrace::threadBuffers[i]->reset();
    }
}

/**
 * This method is invoked as a WorkerTimer to log all of the thread-local
 * buffers in the background.
 */
void
TimeTrace::TraceLogger::handleTimerEvent()
{
    std::vector<TimeTrace::Buffer*> buffers;

    // Make a copy of the list of traces, so we can do the actual tracing
    // without holding a lock and without fear of the list changing.
    {
        SpinLock::Guard guard(TimeTrace::mutex);
        buffers = threadBuffers;
    }
    TimeTrace::printInternal(&buffers, NULL);
    isFinished = true;
    TimeTrace::activeReaders.add(-1);
}

/**
 * Construct a TimeTrace::Buffer.
 */
TimeTrace::Buffer::Buffer()
    : nextIndex(0)
    , events()
    , threadId(ThreadId::get())
{
    // Mark all of the events invalid.
    for (uint32_t i = 0; i < BUFFER_SIZE; i++) {
        events[i].format = NULL;
    }
}

/**
 * Destructor for TimeTrace::Buffer.
 */
TimeTrace::Buffer::~Buffer()
{
}

/**
 * Return a string containing a printout of the records in the buffer.
 */
string TimeTrace::Buffer::getTrace()
{
    string s;
    std::vector<TimeTrace::Buffer*> buffers;
    buffers.push_back(this);
    printInternal(&buffers, &s);
    return s;
}

/**
 * Print all existing trace records to the system log.
 */
void TimeTrace::Buffer::printToLog()
{
    std::vector<TimeTrace::Buffer*> buffers;
    buffers.push_back(this);
    printInternal(&buffers, NULL);
}

/**
 * Discard any existing trace records.
 */
void TimeTrace::Buffer::reset()
{
    for (uint32_t i = 0; i < BUFFER_SIZE; i++) {
        if (events[i].format == NULL) {
            break;
        }
        events[i].format = NULL;
    }
    nextIndex = 0;
}

} // namespace RAMCloud
