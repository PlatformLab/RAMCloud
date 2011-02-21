/* Copyright (c) 2011 Stanford University
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

#ifndef RAMCLOUD_FAILUREDETECTOR_H
#define RAMCLOUD_FAILUREDETECTOR_H

#include <list>

#include "Common.h"
#include "Rpc.h"
#include "ServiceLocator.h"
#include "ServerList.pb.h"
#include "Syscall.h"
#include "Tub.h"

namespace RAMCloud {

class FailureDetector {
  public:
    FailureDetector(string coordinatorLocatorString,
        string listeningLocatorsString, ServerType type);
    ~FailureDetector();
    void mainLoop();

  private:
    /// The TimeoutQueue contains a list of previously-issued pings,
    /// in non-descending cycle count order of transmission. It abstracts
    /// out the tracking of outstanding requests, their timing out, how
    /// long we need to wait for the next one, and other piddly little
    /// details that are hard to do in-line.
    class TimeoutQueue {
      public:
        /// Each entry in our queue uses the following structure. It's simply
        /// a container for a single probe.
        class TimeoutEntry {
          public:
            TimeoutEntry(uint64_t startUsec, string locator, uint64_t nonce)
                : startUsec(startUsec),
                  locator(locator),
                  nonce(nonce)
            {
            }
            uint64_t startUsec;
            string   locator;
            uint64_t nonce;
        };

        explicit TimeoutQueue(uint64_t timeoutUsec);
        void enqueue(string locator, uint64_t nonce);
        Tub<TimeoutEntry> dequeue();
        Tub<TimeoutEntry> dequeue(uint64_t nonce);
        uint64_t microsUntilNextTimeout();

      private:
        std::list<TimeoutEntry> entries;    /// Timeouts in non-descending order
        uint64_t timeoutUsecs;              /// Common timeout for all entries

        friend class FailureDetectorTest;

        DISALLOW_COPY_AND_ASSIGN(TimeoutQueue);
    };

    /// Maximum payload in any datagram. This should be enough to get 40
    /// machines worth of ServiceLocators for our cluster. Try to temper
    /// your disgust with the fact that this whole class is a temporary
    /// hack.
    static const int MAXIMUM_MTU_BYTES = 9000;

    /// Number of microseconds between probes.
    static const int PROBE_INTERVAL_USECS = 10 * 1000;

    /// Number of microseconds before a probe is considered to have timed out.
    static const int TIMEOUT_USECS = 50 * 1000;

    /// Socket used for outbound pings and their incoming responses, i.e.
    /// what we use to ping out and hear back.
    int clientFd;

    /// Socket used for incoming ping requests and their outgoing responses,
    /// i.e. what others use to ping us and for us to respond on.
    int serverFd;

    /// Socket used for coordinator "rpcs", since the Transport isn't
    /// thread-safe.
    int coordFd;

    ServerType           type;            /// Type of servers we're to probe.
    string               coordinator;     /// Coordinator's serviceLocator str.
    string               localLocator;    /// Our local ServiceLocator string.
    ProtoBuf::ServerList serverList;      /// List of servers to probe.
    bool                 terminate;       /// Way to abort mainLoop for testing.
    TimeoutQueue         queue;           /// Queue of previous probes.

    /// Only complain once when we go to ping a random server and there
    /// are none available in our list.
    bool                 haveLoggedNoServers;

    /// System calls used for socket operations. When testing, replaced with
    /// special stubs.
    static Syscall*      sys;

    sockaddr_in serviceLocatorStringToSockaddrIn(string sl);
    void handleIncomingRequest(char* buf, ssize_t bytes,
        sockaddr_in* sourceAddress);
    void handleIncomingResponse(char* buf, ssize_t bytes,
        sockaddr_in* sourceAddress);
    void handleCoordinatorResponse(char* buf, ssize_t bytes,
        sockaddr_in* sourceAddress);
    void pingRandomServer();
    void alertCoordinator(TimeoutQueue::TimeoutEntry* te);
    void processPacket(int fd);
    void requestServerList();

    friend class FailureDetectorTest;

    DISALLOW_COPY_AND_ASSIGN(FailureDetector);
};

} // namespace

#endif // !RAMCLOUD_FAILUREDETECTOR_H
