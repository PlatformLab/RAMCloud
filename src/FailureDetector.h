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
#include "IpAddress.h"
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
    explicit FailureDetector(string coordinatorLocatorString);
    ~FailureDetector();
    void mainLoop();

    /**
     * Given a ServiceLocator for a server (master, backup, or coordinator),
     * generate the IP and UDP port they should be listening on for incoming
     * pings, and return the appropriate sockaddr_in struct.
     *
     * Since there may be multiple protocols used with different ports (and,
     * perhaps, IPs), we need to establish an order of precedence. It's
     * currently:
     *      - infrc
     *      - fast+udp
     *      - tcp
     *
     * Once we have the ip and port values, we simply add 2111 to the port
     * number. Voila.
     *
     * To support testing, if the protocol is "mock", we will return a sin
     * with INADDR_ANY.
     */
    static sockaddr_in
    serviceLocatorStringToSockaddrIn(string sl)
    {
        auto locators = ServiceLocator::parseServiceLocators(sl);
        ServiceLocator* useSl = NULL;
        string order[3] = { "infrc", "fast+udp", "tcp" };
        foreach (auto& s, order) {
            foreach (auto& l, locators) {
                if (l.getProtocol() == "mock") {
                    sockaddr_in sin;
                    memset(&sin, 0, sizeof(sin));
                    sin.sin_addr.s_addr = INADDR_ANY;
                    sin.sin_port = htons(0);
                    return sin;
                } else if (l.getProtocol() == s) {
                    useSl = &l;
                    break;
                }
            }
            if (useSl != NULL)
                break;
        }

        if (useSl == NULL) {
            throw Exception(HERE, format("could not determine IP/port for sl "
                "string [%s]", sl.c_str()));
        }

        IpAddress addr(*useSl);
        sockaddr_in sin;
        memcpy(&sin, &addr.address, sizeof(sin));
        sin.sin_family = PF_INET;
        sin.sin_port = htons(ntohs(sin.sin_port) + 2111);
        return sin;
    }

    /**
     * Synchronously ping the given server and return true if a response
     * was received. If no response is received within #TIMEOUT_USECS,
     * return false.
     *
     * \param[in] locatorString
     *      ServiceLocator string of the server to ping.
     */
    static bool
    pingServer(string locatorString)
    {
        LOG(DEBUG, "issuing request for [%s]\n", locatorString.c_str());

        ssize_t r = write(internalClientSocket, locatorString.c_str(),
            locatorString.length() + 1);
        if (r != static_cast<ssize_t>(locatorString.length() + 1))
            throw Exception(HERE, format("write failed with %zd", r));

        uint8_t gotResponse;
        r = read(internalClientSocket, &gotResponse, sizeof(gotResponse));
        if (r != sizeof(gotResponse))
            throw Exception(HERE, format("read failed with %zd", r));

        return gotResponse == 1;
    }

    /// Maximum payload in any datagram. This should be enough to get 40
    /// machines worth of ServiceLocators for our cluster. Try to temper
    /// your disgust with the fact that this whole class is a temporary
    /// hack.
    static const uint32_t MAXIMUM_MTU_BYTES = 9000;

    /// Number of microseconds between probes.
    static const uint32_t PROBE_INTERVAL_USECS = 10 * 1000;

    /// Number of microseconds before a probe is considered to have timed out.
    static const uint32_t TIMEOUT_USECS = 50 * 1000;

    /// Number of microseconds between refreshes of the server list.
    static const uint32_t REFRESH_INTERVAL_USECS = 5 * 1000 * 1000;

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
            TimeoutEntry(uint64_t startUsec, string locator,
              uint64_t nonce, int type)
                : startUsec(startUsec),
                  locator(locator),
                  nonce(nonce),
                  type(type)
            {
            }
            uint64_t startUsec;
            string   locator;
            uint64_t nonce;
            int      type;
        };

        explicit TimeoutQueue(uint64_t timeoutUsec);
        void enqueue(string locator, uint64_t nonce, int type);
        Tub<TimeoutEntry> dequeue();
        Tub<TimeoutEntry> dequeue(uint64_t nonce);
        uint64_t microsUntilNextTimeout();

      private:
        std::list<TimeoutEntry> entries;    /// Timeouts in non-descending order
        uint64_t timeoutUsecs;              /// Common timeout for all entries

        friend class FailureDetectorTest;

        DISALLOW_COPY_AND_ASSIGN(TimeoutQueue);
    };

    /**
     * The following are types of probes that we passed in to the TimeoutEntry
     * constructor. This lets us differentiate our random pings, coordinator-
     * initiated pings, and ones that were initiated internally via the
     * #pingServer method.
     */
    static const int RANDOM_PROBE   = 0;
    static const int COORD_PROBE    = 1;
    static const int INTERNAL_PROBE = 2;

    /// Socket used for outbound pings and their incoming responses, i.e.
    /// what we use to ping out and hear back.
    int clientFd;

    /// Socket used for incoming ping requests and their outgoing responses,
    /// i.e. what others use to ping us and for us to respond on.
    int serverFd;

    /// Socket used for coordinator "rpcs", since the Transport isn't
    /// thread-safe.
    int coordFd;

    /// The socket used internally to issue ping requests to the FailureDetector
    /// thread. Transports can employ this to determine whether or not RPCs have
    /// likely timed out.
    static int internalClientSocket;

    /// The other end of #internalClientSocket. This is where requests are
    /// received and processed in FailureDetector's #mainLoop.
    static int internalServerSocket;

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

    void handleIncomingRequest(char* buf, ssize_t bytes,
        sockaddr_in* sourceAddress);
    void handleIncomingResponse(char* buf, ssize_t bytes,
        sockaddr_in* sourceAddress);
    void handleCoordinatorResponse(char* buf, ssize_t bytes,
        sockaddr_in* sourceAddress);
    void pingRandomServer();
    void handleTimeout(TimeoutQueue::TimeoutEntry* te);
    void processPacket(int fd);
    void requestServerList();
    void handleInternalPingRequest();
    void clientMainLoop();
    void serverMainLoop();

    friend class FailureDetectorTest;

    DISALLOW_COPY_AND_ASSIGN(FailureDetector);
};

} // namespace

#endif // !RAMCLOUD_FAILUREDETECTOR_H
