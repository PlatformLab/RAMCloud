/* Copyright (c) 2010-2015 Stanford University
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

#include <gtest/gtest.h>

#include <queue>

#include "ServiceLocator.h"
#include "Transport.h"

#ifndef RAMCLOUD_MOCKTRANSPORT_H
#define RAMCLOUD_MOCKTRANSPORT_H

namespace RAMCloud {

/**
 * This class defines an implementation of Transport that allows unit
 * tests to run without a network or a remote counterpart (it logs
 * output packets and provides a mechanism for prespecifying input
 * packets).
 */
class MockTransport : public Transport {
  public:
    explicit MockTransport(Context* context,
                           const ServiceLocator *serviceLocator = NULL);
    virtual ~MockTransport() { }
    virtual string getServiceLocator();

    virtual Transport::SessionRef
    getSession(const ServiceLocator* serviceLocator, uint32_t timeoutMs = 0);

    virtual Transport::SessionRef
    getSession();

    void registerMemory(void* base, size_t bytes) {
        RAMCLOUD_TEST_LOG("register %d bytes at %lu for %s",
                          static_cast<int>(bytes),
                          reinterpret_cast<uint64_t>(base),
                          locatorString.c_str());
    }

    void clearInput();
    void setInput(const char* message);
    void clearOutput();

    enum Event { CLIENT_SEND, ABORT, CANCEL, SEND_REQUEST, SERVER_REPLY };

    /**
     * Compare a transmission buffer to some expected value and return the
     * differences, if any; for use with gtest.
     *
     * \param index
     *      Which recorded transmission to compare against. Each transmission
     *      made is stored in order starting at index 0. If this exceeds the
     *      number of recorded messages then AssertionFailure will be returned
     *      with an appropriate debugging message.
     * \param event
     *      Which event the transmission at \a index should have been enqueued
     *      by (see Event).
     * \param header
     *      The expected rpc head the transmission at \a index should have.
     * \param payload
     *      Optional byte string to compare against the data that trails the
     *      header in the enqueued message buffer.
     * \param payloadBytes
     *      Number of bytes starting at \a payload to compare against the
     *      byte string that follows the header in the buffer.
     * \return
     *      A gtest AssertionResult that contains useful debugging information
     *      when the output doesn't match what was expected.
     */
    template <typename T>
    ::testing::AssertionResult
    outputMatches(size_t index, Event event, const T& header,
                  const char* payload = NULL, size_t payloadBytes = 0)
    {
        try {
            auto& pair = output.at(index);
            if (pair.first != event) {
                return ::testing::AssertionFailure() <<
                    format("Expected event %u, actual event %u",
                           event, pair.first);
            }
            Buffer& actualBuffer = pair.second;
            if (actualBuffer.size() < sizeof(header)) {
                return ::testing::AssertionFailure() <<
                    format("Actual output was only %u bytes but header "
                           "header length was %lu",
                           actualBuffer.size(), sizeof(header));
            }
            typedef const char* b;
            auto* actualHeader = actualBuffer.getRange(0, sizeof(T));
            if (memcmp(&header, actualHeader, sizeof(header))) {
                return ::testing::AssertionFailure() <<
                    format("Expected header doesn't match actual header:\n"
                           "  Actual: \"%s\"\nExpected: \"%s\"\n",
                           toStringHack(b(actualHeader),
                                        sizeof(header)).c_str(),
                           toStringHack(b(&header),
                                        sizeof(header)).c_str());
            }
            uint32_t actualPayloadBytes = actualBuffer.size() -
                                          uint32_t(sizeof(header));
            auto* actualPayload = actualBuffer.getRange(sizeof(header),
                                                        actualPayloadBytes);
            if (payloadBytes != actualPayloadBytes ||
                memcmp(payload, actualPayload, payloadBytes) != 0)
            {
                return ::testing::AssertionFailure() <<
                    format("Headers matched, payloads don't match:\n"
                           "  Actual: \"%s\" (length %u)\n"
                           "Expected: \"%s\" (length %lu)\n",
                           toStringHack(b(actualPayload),
                                        actualPayloadBytes).c_str(),
                            actualPayloadBytes,
                           toStringHack(b(payload),
                                        uint32_t(payloadBytes)).c_str(),
                           payloadBytes);
            }
        } catch (const std::out_of_range& e) {
            return ::testing::AssertionFailure() <<
                format("No output message in MockTransport at index %lu",
                       index);
        }
        return ::testing::AssertionSuccess();
    }

    class MockServerRpc : public ServerRpc {
        public:
            explicit MockServerRpc(MockTransport* transport,
                                   const char* message);
            void sendReply();
            string getClientServiceLocator();
        private:
            MockTransport* transport;
            DISALLOW_COPY_AND_ASSIGN(MockServerRpc);
    };

    class MockSession : public Session {
        public:
            explicit MockSession(MockTransport* transport)
                : transport(transport),
                serviceLocator(ServiceLocator("mock: anonymous=1")) {}
            MockSession(MockTransport* transport,
                        const ServiceLocator* serviceLocator)
                : transport(transport), serviceLocator(*serviceLocator) {}
            virtual ~MockSession();
            void abort();
            virtual void cancelRequest(RpcNotifier* notifier);
            virtual string getRpcInfo();
            virtual void sendRequest(Buffer* request, Buffer* response,
                    RpcNotifier* notifier);
        private:
            MockTransport* transport;
            const ServiceLocator serviceLocator;
            DISALLOW_COPY_AND_ASSIGN(MockSession);
    };

    /**
     * Shared RAMCloud information.
     */
    Context* context;

    /**
     * Records information from each call to sendRequest and other methods.
     */
    string outputLog;

    /**
     * Records the transmitted buffers along with the event which caused
     * their transmission (see Event). For use with outputMatches(); can
     * be cleared with clearOutput().
     */
    std::deque<pair<Event, Buffer>> output;

    /*
     * Status from the most recent call to sendReply (STATUS_MAX_VALUE+1 means
     * response was too short to hold a status, or we haven't yet received
     * any responses).
     */
    Status status;

    /**
     * Used as the next input message required by wait.
     */
    std::queue<const char*> inputMessages;

    // Notifier from the last call to sendRequest.
    RpcNotifier *lastNotifier;

    // The following variables count calls to various methods, for use
    // by tests.
    uint32_t serverSendCount;
    uint32_t clientRecvCount;

    // Count of number of sessions created.
    uint32_t sessionCreateCount;

    // The following variable must be static: sessions can get deleted
    // *after* their transport, so can't reference anything in a particular
    // transport.
    static uint32_t sessionDeleteCount;

    // ServiceLocator string passed to constructor, or "mock:" if the
    // constructor argument was NULL.
    string locatorString;

  private:
    void appendToOutput(Event event, const string& message);
    void appendToOutput(Event event, Buffer& payload);
    string toStringHack(const char* buf, uint32_t length);

    DISALLOW_COPY_AND_ASSIGN(MockTransport);
};

}  // namespace RAMCloud

#endif
