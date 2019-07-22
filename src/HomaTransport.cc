/* Copyright (c) 2015-2017 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
#include <algorithm>
#include <fstream>

#include "HomaTransport.h"
#include "Service.h"
#include "TimeTrace.h"
#include "WorkerManager.h"
#include "OptionParser.h"

namespace RAMCloud {

// Change 0 -> 1 in the following line to compile detailed time tracing in
// this transport.
#define TIME_TRACE 0

// Provides a cleaner way of invoking TimeTrace::record, with the code
// conditionally compiled in or out by the TIME_TRACE #ifdef. Arguments
// are made uint64_t (as opposed to uin32_t) so the caller doesn't have to
// frequently cast their 64-bit arguments into uint32_t explicitly: we will
// help perform the casting internally.
namespace {
    inline void
    timeTrace(const char* format,
            uint64_t arg0 = 0, uint64_t arg1 = 0, uint64_t arg2 = 0,
            uint64_t arg3 = 0)
    {
#if TIME_TRACE
        TimeTrace::record(format, uint32_t(arg0), uint32_t(arg1),
                uint32_t(arg2), uint32_t(arg3));
#endif
    }
}

/**
 * Construct a new HomaTransport.
 * 
 * \param context
 *      Shared state about various RAMCloud modules.
 * \param locator
 *      Service locator that contains parameters for this transport.
 *      NULL means this transport is created on the client-side to handle
 *      outgoing requests.
 * \param driver
 *      Used to send and receive packets.
 * \param driverOwner
 *      True if this transport becomes owner of the driver and will free it
 *      when this object is deleted.
 * \param clientId
 *      Identifier that identifies us in outgoing RPCs: must be unique across
 *      all servers and clients.
 */
HomaTransport::HomaTransport(Context* context, const ServiceLocator* locator,
        Driver* driver, bool driverOwner, uint64_t clientId)
    : context(context)
    , driver(driver)
    , driverOwner(driverOwner)
    , locatorString("homa+"+driver->getServiceLocator())
    , poller(context, this)
    , maxDataPerPacket(driver->getMaxPacketSize() - sizeof32(DataHeader))

    // As of 09/2017, we consider messages less than 300 bytes as small (which
    // takes at most 240 ns to transmit on a 10Gbps network). This value is
    // chosen experimentally so that we can run W3 in Homa paper at 80% load on
    // a 10Gbps network and that no significant queueing delay at the TX queue
    // is observed.
    , smallMessageThreshold(300)
    , clientId(clientId)
    , highestAvailPriority()
    , lowestUnschedPrio()
    , highestSchedPriority()
    , nextClientSequenceNumber(1)
    , nextServerSequenceNumber(1)
    , receivedPackets()
    , messagesToGrant()
    , serverRpcPool()
    , clientRpcPool()
    , outgoingRpcs()
    , outgoingRequests()
    , activeOutgoingMessages()
    , incomingRpcs()
    , outgoingResponses()
    , serverTimerList()
    , roundTripBytes()
    , grantIncrement(maxDataPerPacket)
    , timerInterval(0)
    , nextTimeoutCheck(0)
    , timeoutCheckDeadline(0)

    // As of 7/2016, the value for timeoutIntervals is set relatively high.
    // This is needed to handle issues on some machines (such as the NEC
    // Atom cluster) where threads can get descheduled by the kernel for
    // 10-30ms. This can result in delays in handling network packets, and
    // we don't want those delays to result in RPC timeouts.
    , timeoutIntervals(40)
    , pingIntervals(3)
    , unschedPrioCutoffs()
    , activeMessages()
    , inactiveMessages()
    , maxGrantedMessages()
{
    // Set up the timer to trigger at 2 ms intervals. We use this choice
    // (as of 11/2015) because the Linux kernel appears to buffer packets
    // for up to about 1 ms before delivering them to applications. Shorter
    // intervals result in unnecessary retransmissions.
    timerInterval = Cycles::fromMicroseconds(2000);
    nextTimeoutCheck = Cycles::rdtsc() + timerInterval;

    // Read Homa configuration from the transport configuration file
    // (See config/transport.conf for documentation and examples).
    string configDir = "config";
    if (context->options) {
        configDir = context->options->getConfigDir();
    }
    std::ifstream configFile(configDir + "/transport.conf");
    Tub<ServiceLocator> config;
    ServiceLocator* params = NULL;
    if (configFile.is_open()) {
        std::string line;
        string protocol = ServiceLocator(locatorString).getProtocol();
        try {
            while (std::getline(configFile, line)) {
                if ((line.find('#') == 0) ||
                        (line.find(protocol) == string::npos)) {
                    // Skip comments and irrelevant lines.
                    continue;
                }
                params = config.construct(line);
                break;
            }
        } catch (ServiceLocator::BadServiceLocatorException&) {
            LOG(ERROR, "Ignored bad transport configuration: '%s'",
                    line.c_str());
        }
    }

    // Compute and set Homa parameters.
    roundTripBytes = getRoundTripBytes(params);
    getUnschedPriorities(params, &lowestUnschedPrio, &highestAvailPriority);
    highestSchedPriority = std::max(lowestUnschedPrio-1, 0);
    maxGrantedMessages = getOvercommitmentDegree(params,
            highestSchedPriority + 1);
    unschedPrioCutoffs = getUnschedPrioCutoffs(params,
            highestAvailPriority - lowestUnschedPrio + 1);

    LOG(NOTICE, "HomaTransport parameters: clientId %lu, maxDataPerPacket %u, "
            "roundTripBytes %u, grantIncrement %u, "
            "pingIntervals %d, timeoutIntervals %d, timerInterval %.2f ms, "
            "maxGrantedMessages %u, highestAvailPriority %d, "
            "lowestUnschedPriority %d, highestSchedPriority %d",
            clientId, maxDataPerPacket, roundTripBytes, grantIncrement,
            pingIntervals, timeoutIntervals,
            Cycles::toSeconds(timerInterval)*1e3, maxGrantedMessages,
            highestAvailPriority, lowestUnschedPrio, highestSchedPriority);
}

/**
 * Destructor for HomaTransports.
 */
HomaTransport::~HomaTransport()
{
    // This cleanup is mostly for the benefit of unit tests: in production,
    // this destructor is unlikely ever to get called.

    // Reclaim all of the RPC objects.
    for (ServerRpcMap::iterator it = incomingRpcs.begin();
            it != incomingRpcs.end(); ) {
        ServerRpc* serverRpc = it->second;

        // Advance iterator; otherwise it will get invalidated by
        // deleteServerRpc.
        it++;
        deleteServerRpc(serverRpc);
    }
    for (ClientRpcMap::iterator it = outgoingRpcs.begin();
            it != outgoingRpcs.end(); ) {
        ClientRpc* clientRpc = it->second;

        // Advance iterator; otherwise it will get invalidated by
        // deleteClientRpc.
        it++;
        deleteClientRpc(clientRpc);
    }

    driver->release();
    if (driverOwner) {
        delete driver;
    }
}

// See Transport::getServiceLocator().
string
HomaTransport::getServiceLocator()
{
    return locatorString;
}

/**
 * When we are finished processing an outgoing RPC, this method is
 * invoked to delete the ClientRpc object and remove it from all
 * existing data structures.
 *
 * \param clientRpc
 *      An RPC that has either completed normally or is being
 *      aborted.
 */
void
HomaTransport::deleteClientRpc(ClientRpc* clientRpc)
{
    uint64_t sequence = clientRpc->rpcId.sequence;
    TEST_LOG("RpcId %lu", sequence);
    outgoingRpcs.erase(sequence);
    if (clientRpc->transmitPending) {
        erase(outgoingRequests, *clientRpc);
    }
    if (clientRpc->request.active) {
        erase(activeOutgoingMessages, clientRpc->request);
    }
    clientRpcPool.destroy(clientRpc);
    timeTrace("deleted client RPC, clientId %u, sequence %u, %u outgoing RPCs",
            clientId, sequence, outgoingRpcs.size());
}

/**
 * When we are finished processing an incoming RPC, this method is
 * invoked to delete the RPC object and remove it from all existing
 * data structures.
 *
 * \param serverRpc
 *      An RPC that has either completed normally or should be
 *      aborted.
 */
void
HomaTransport::deleteServerRpc(ServerRpc* serverRpc)
{
    uint64_t sequence = serverRpc->rpcId.sequence;
    TEST_LOG("RpcId (%lu, %lu)", serverRpc->rpcId.clientId,
            sequence);
    incomingRpcs.erase(serverRpc->rpcId);
    if (serverRpc->sendingResponse) {
        erase(outgoingResponses, *serverRpc);
    }
    if (serverRpc->sendingResponse || !serverRpc->requestComplete) {
        erase(serverTimerList, *serverRpc);
    }
    if (serverRpc->response.active) {
        erase(activeOutgoingMessages, serverRpc->response);
    }
    serverRpcPool.destroy(serverRpc);
    timeTrace("deleted server RPC, clientId %u, sequence %u, %u incoming RPCs",
            serverRpc->rpcId.clientId, sequence, incomingRpcs.size());
}

/**
 * Parse option values in a service locator to determine how many bytes
 * of data must be sent to cover the round-trip latency of a connection.
 * The result is rounded up to the next multiple of the packet size.
 *
 * \param locator
 *      Service locator that may contain "gbs" and "rttMicros" options.
 *      If NULL, or if any of the options  are missing, then defaults
 *      are supplied.
 */
uint32_t
HomaTransport::getRoundTripBytes(const ServiceLocator* locator)
{
    // Set the round-trip time (RTT) between two servers in the cluster.
    // To be precise, the RTT includes the one-way delay of a full-size data
    // packet, the server processing time and the one-way delay of a grant
    // packet. As of 11/17, the RTT on CloudLab m510 nodes is ~8us (5 us of
    // one-way delay of a full-size data packet plus 1 us of server processing
    // time plus 2 us of grant packet one-way delay). Note: it's hacky to
    // hardcode this number in the code; a proper implementation would need to
    // measure and set the RTT dynamically.
    uint32_t roundTripMicros = 8;
    if (locator && locator->hasOption("rttMicros")) {
        char* end;
        uint32_t value = downCast<uint32_t>(strtoul(
                locator->getOption("rttMicros").c_str(), &end, 10));
        if ((*end == 0) && (value != 0)) {
            roundTripMicros = value;
        } else {
            LOG(ERROR, "Bad HomaTransport rttMicros option value '%s' "
                    "(expected positive integer); ignoring option",
                    locator->getOption("rttMicros").c_str());
        }
    }

    // Set the network bandwidth of this machine.
    uint32_t gBitsPerSec = 0;
    if (locator != NULL) {
        if (locator->hasOption("gbs")) {
            char* end;
            uint32_t value = downCast<uint32_t>(strtoul(
                    locator->getOption("gbs").c_str(), &end, 10));
            if ((*end == 0) && (value != 0)) {
                gBitsPerSec = value;
            } else {
                LOG(ERROR, "Bad HomaTransport gbs option value '%s' "
                        "(expected positive integer); ignoring option",
                        locator->getOption("gbs").c_str());
            }
        }
    }

    uint32_t mBitsPerSec = gBitsPerSec * 1000;
    if (mBitsPerSec == 0) {
        mBitsPerSec = driver->getBandwidth();
        if (mBitsPerSec == 0) {
            mBitsPerSec = 10 * 1000;
        }
    }

    // Compute round-trip time in terms of full packets (round up).
    uint32_t roundTripBytes = (roundTripMicros*mBitsPerSec)/8;
    roundTripBytes = ((roundTripBytes+maxDataPerPacket-1)/maxDataPerPacket)
            * maxDataPerPacket;
    LOG(NOTICE, "roundTripMicros %u, mBitsPerSec %u, roundTripBytes %u",
            roundTripMicros, mBitsPerSec, roundTripBytes);
    return roundTripBytes;
}

/**
 * Parse option values in a service locator to determine how many messages can
 * be granted concurrently.
 *
 * \param locator
 *      Service locator that may contain "degreeOC" options. If NULL, or if the
 *      option is missing, then defaults are supplied.
 * \param numSchedPrio
 *      # scheduled priorities. Acts as the default value of "degreeOC".
 */
uint32_t
HomaTransport::getOvercommitmentDegree(const ServiceLocator *locator,
        int numSchedPrio)
{
    // By default, set the degree of over-commitment to # scheduled priorities
    // (if it's not too small).
    uint32_t degreeOC = std::max(4u, downCast<uint32_t>(numSchedPrio));
    if (locator && locator->hasOption("degreeOC")) {
        int value = locator->getOption<uint32_t>("degreeOC");
        if (value > 0) {
            degreeOC = downCast<uint32_t>(value);
        } else {
            LOG(ERROR, "Bad HomaTransport degreeOC option value '%d' "
                    "(expected positive integer); ignoring option", value);
        }
    }
    return degreeOC;
}

/**
 * Parse option values in a service locator to determine what priorities to use
 * for unscheduled traffic.
 *
 * \param locator
 *      Service locator that may contain "numPrio" and "unschedPrio" options.
 *      If NULL, or if any of the options are missing, then defaults are
 *      supplied.
 * \param[out] lowest
 *      When this method returns, it will hold the lowest priority level to use
 *      for unscheduled traffic.
 * \param[out] highest
 *      When this method returns, it will hold the highest priority level to use
 *      for unscheduled traffic (i.e. the highest priority level we have).
 */
void
HomaTransport::getUnschedPriorities(const ServiceLocator *locator, int* lowest,
        int* highest)
{
    // By default, we use all priorities supported by the underlying driver but
    // allocate only the highest priority to unscheduled traffic.
    int numPrio = driver->getHighestPacketPriority() + 1;
    if (locator && locator->hasOption("numPrio")) {
        int value = locator->getOption<int>("numPrio");
        if ((0 < value) && (value <= numPrio)) {
            numPrio = value;
        } else {
            LOG(ERROR, "Bad HomaTransport numPrio option value '%d' "
                     "(expected (0, %d]); ignoring option", value, numPrio);
        }
    }
    *highest = numPrio - 1;
    *lowest = *highest;
    if (locator && locator->hasOption("unschedPrio")) {
        int value = locator->getOption<int>("unschedPrio");
        if ((0 < value) && (value <= numPrio)) {
            *lowest = numPrio - value;
        } else {
            LOG(ERROR, "Bad HomaTransport unschedPrio option value '%d' "
                    "(expected (0, %d]); ignoring option", value, numPrio);
        }
    }
}

/**
 * Parse option values in a service locator to determine the message size
 * cutoff for each unscheduled priority level.
 *
 * \param locator
 *      Service locator that may contain "unschedPrioCutoffs" options.
 *      If NULL, or if the option is missing, then defaults are supplied.
 * \param numUnschedPrio
 *      # priorities allocated to unscheduled traffic.
 */
vector<uint32_t>
HomaTransport::getUnschedPrioCutoffs(const ServiceLocator *locator,
        int numUnschedPrio) {
    string prioBrackets = "[0";
    vector<uint32_t> result;
    if (locator && locator->hasOption("unschedPrioCutoffs")) {
        std::stringstream sstream(locator->getOption("unschedPrioCutoffs"));
        std::string cutoff;
        int value;
        while (std::getline(sstream, cutoff, '.')) {
            try {
                value = stoi(cutoff);
            } catch (std::exception& e) {
                LOG(ERROR, "Bad priority cutoff value: '%s'; ignoring cutoff",
                        cutoff.c_str());
                continue;
            }
            if (!result.empty() && (value <= int(result.back()))) {
                LOG(ERROR, "Bad priority cutoff bracket: [%u, %d); "
                        "ignoring cutoff", result.back(), value);
                continue;
            }
            result.push_back(downCast<uint32_t>(value));
            prioBrackets += format(", %d] [%d", value-1, value);
        }
    }
    if (result.size() + 1 != downCast<uint32_t>(numUnschedPrio)) {
        LOG(ERROR, "Bad unscheduled priority cutoffs, %lu brackets specified "
                "(expecting %d)", result.size()+1, numUnschedPrio);
    }
    result.push_back(~0u);
    prioBrackets += format(", %u]", ~0u);
    LOG(NOTICE, "Priority brackets for unscheduled messages: %s",
            prioBrackets.c_str());
    return result;
}

/**
 * Decides which packet priority should be used to transmit the unscheduled
 * portion of a message.
 *
 * \param messageSize
 *      The size of the message to be transmitted.
 * \return
 *      The packet priority to use.
 */
uint8_t
HomaTransport::getUnschedTrafficPrio(uint32_t messageSize) {
    int priority = highestAvailPriority;
    for (uint32_t cutoff : unschedPrioCutoffs) {
        if (messageSize < cutoff) {
            return downCast<uint8_t>(priority);
        }
        priority--;
    }
    return downCast<uint8_t>(priority);
}

/**
 * Return a printable symbol for the opcode field from a packet.
 * \param opcode
 *     Opcode field from a packet.
 * \return
 *     The result is a static string, which may change on the next
 *
 */
string
HomaTransport::opcodeSymbol(uint8_t opcode) {
    switch (opcode) {
        case HomaTransport::PacketOpcode::ALL_DATA:
            return "ALL_DATA";
        case HomaTransport::PacketOpcode::DATA:
            return "DATA";
        case HomaTransport::PacketOpcode::GRANT:
            return "GRANT";
        case HomaTransport::PacketOpcode::LOG_TIME_TRACE:
            return "LOG_TIME_TRACE";
        case HomaTransport::PacketOpcode::RESEND:
            return "RESEND";
        case HomaTransport::PacketOpcode::BUSY:
            return "BUSY";
        case HomaTransport::PacketOpcode::ABORT:
            return "ABORT";
    }

    return format("%d", opcode);
}

/**
 * This method takes care of packet sizing and transmitting message data,
 * both for requests and for responses. When a method returns, the given
 * range of data will have been queued for the NIC but may not actually
 * have been transmitted yet.
 *
 * \param address
 *      Identifies the destination for the message.
 * \param rpcId
 *      Unique identifier for the RPC.
 * \param message
 *      Contains the entire message.
 * \param offset
 *      Offset in bytes of the first byte to be transmitted.
 * \param maxBytes
 *      Maximum number of bytes to transmit. If offset + maxBytes exceeds
 *      the message length, then all of the remaining bytes in message,
 *      will be transmitted.
 * \param unscheduledBytes
 *      Unscheduled bytes that will be sent unilaterally in this message.
 * \param priority
 *      Priority used to send the packets.
 * \param flags
 *      Extra flags to set in packet headers, such as FROM_CLIENT or
 *      RETRANSMISSION. Must at least specify either FROM_CLIENT or
 *      FROM_SERVER.
 * \return
 *      The number of bytes of data actually transmitted (may be 0 in
 *      some situations).
 */
uint32_t
HomaTransport::sendBytes(const Driver::Address* address, RpcId rpcId,
        Buffer* message, uint32_t offset, uint32_t maxBytes,
        uint32_t unscheduledBytes, uint8_t priority, uint8_t flags)
{
    uint32_t curOffset = offset;
    uint32_t transmitLimit = std::min(message->size(), curOffset + maxBytes);
    uint32_t bytesSent = 0;
    while (curOffset < transmitLimit) {
        // Don't send less-than-full-size packets except for the last packet
        // of the message (unless the caller explicitly requested it).
        uint32_t bytesThisPacket = transmitLimit - curOffset;
        if (bytesThisPacket >= maxDataPerPacket) {
            bytesThisPacket = maxDataPerPacket;
        } else if (transmitLimit < message->size()) {
            break;
        }

        QueueEstimator::TransmitQueueState txQueueState;
        if (bytesThisPacket == message->size()) {
            // Entire message fits in a single packet.
            AllDataHeader header(rpcId, flags,
                    downCast<uint16_t>(message->size()));
            Buffer::Iterator iter(message);
            const char* fmt = (flags & FROM_CLIENT) ?
                    "client sending ALL_DATA, clientId %u, sequence %u, "
                    "priority %u" :
                    "server sending ALL_DATA, clientId %u, sequence %u, "
                    "priority %u";
            timeTrace(fmt, rpcId.clientId, rpcId.sequence, priority);
            driver->sendPacket(address, &header, &iter, priority,
                    &txQueueState);
        } else {
            DataHeader header(rpcId, message->size(), curOffset,
                    unscheduledBytes, flags);
            Buffer::Iterator iter(message, curOffset, bytesThisPacket);
            const char* fmt = (flags & FROM_CLIENT) ?
                    "client sending DATA, clientId %u, sequence %u, "
                    "offset %u, priority %u" :
                    "server sending DATA, clientId %u, sequence %u, "
                    "offset %u, priority %u";
            timeTrace(fmt, rpcId.clientId, rpcId.sequence, curOffset,
                    priority);
            driver->sendPacket(address, &header, &iter, priority,
                    &txQueueState);
        }
        if (txQueueState.outstandingBytes > 0) {
            timeTrace("sent data, %u bytes queued ahead",
                    txQueueState.outstandingBytes);
        } else {
            timeTrace("sent data, tx queue idle %u cyc", txQueueState.idleTime);
        }
        bytesSent += bytesThisPacket;
        curOffset += bytesThisPacket;
    }

    return bytesSent;
}

/**
 * Send a control packet.
 *
 * \param recipient
 *      Where to send the packet.
 * \param packet
 *      Address of the first byte of the control packet header.
 */
template<typename T>
void
HomaTransport::sendControlPacket(const Driver::Address* recipient,
        const T* packet)
{
    QueueEstimator::TransmitQueueState txQueueState;
    driver->sendPacket(recipient, packet, NULL, highestAvailPriority,
            &txQueueState);
    timeTrace("sent control packet, opcode %u, %u bytes queued ahead, "
            "tx queue idle time %u cyc", packet->common.opcode,
            txQueueState.outstandingBytes, txQueueState.idleTime);
}

/**
 * Given a pointer to a HomaTransport packet, return a human-readable
 * string describing the information in its header.
 *
 * \param packet
 *      Address of the first byte of the packet header, which must be
 *      contiguous in memory.
 * \param packetLength
 *      Size of the header, in bytes.
 */
string
HomaTransport::headerToString(const void* packet, uint32_t packetLength)
{
    string result;
    const HomaTransport::CommonHeader* common =
            static_cast<const HomaTransport::CommonHeader*>(packet);
    uint32_t headerLength = sizeof32(HomaTransport::CommonHeader);
    if (packetLength < headerLength) {
        goto packetTooShort;
    }
    result += HomaTransport::opcodeSymbol(common->opcode);
    if (common->flags & HomaTransport::FROM_CLIENT) {
        result += " FROM_CLIENT";
    } else {
        result += " FROM_SERVER";
    }
    result += format(", rpcId %lu.%lu",
            common->rpcId.clientId, common->rpcId.sequence);
    switch (common->opcode) {
        case HomaTransport::PacketOpcode::ALL_DATA:
            headerLength = sizeof32(HomaTransport::AllDataHeader);
            if (packetLength < headerLength) {
                goto packetTooShort;
            }
            break;
        case HomaTransport::PacketOpcode::DATA: {
            headerLength = sizeof32(HomaTransport::DataHeader);
            if (packetLength < headerLength) {
                goto packetTooShort;
            }
            const HomaTransport::DataHeader* data =
                    static_cast<const HomaTransport::DataHeader*>(packet);
            result += format(", totalLength %u, offset %u%s",
                    data->totalLength, data->offset,
                    common->flags & HomaTransport::RETRANSMISSION
                            ? ", RETRANSMISSION" : "");
            break;
        }
        case HomaTransport::PacketOpcode::GRANT: {
            headerLength = sizeof32(HomaTransport::GrantHeader);
            if (packetLength < headerLength) {
                goto packetTooShort;
            }
            const HomaTransport::GrantHeader* grant =
                    static_cast<const HomaTransport::GrantHeader*>(packet);
            result += format(", offset %u, priority %u", grant->offset,
                    grant->priority);
            break;
        }
        case HomaTransport::PacketOpcode::LOG_TIME_TRACE:
            headerLength = sizeof32(HomaTransport::LogTimeTraceHeader);
            if (packetLength < headerLength) {
                goto packetTooShort;
            }
            break;
        case HomaTransport::PacketOpcode::RESEND: {
            headerLength = sizeof32(HomaTransport::ResendHeader);
            if (packetLength < headerLength) {
                goto packetTooShort;
            }
            const HomaTransport::ResendHeader* resend =
                    static_cast<const HomaTransport::ResendHeader*>(
                    packet);
            result += format(", offset %u, length %u, priority %u%s",
                    resend->offset, resend->length, resend->priority,
                    common->flags & HomaTransport::RESTART
                            ? ", RESTART" : "");
            break;
        }
        case HomaTransport::PacketOpcode::BUSY: {
            headerLength = sizeof32(HomaTransport::BusyHeader);
            if (packetLength < headerLength) {
                goto packetTooShort;
            }
            break;
        }
        case HomaTransport::PacketOpcode::ABORT: {
            headerLength = sizeof32(HomaTransport::AbortHeader);
            if (packetLength < headerLength) {
                goto packetTooShort;
            }
            break;
        }
    }
    return result;

  packetTooShort:
    if (!result.empty()) {
        result += ", ";
    }
    result += format("packet too short (got %u bytes, need at least %u)",
            packetLength, headerLength);
    return result;
}

/**
 * This method queues one or more data packets for transmission, if (a) the
 * NIC queue isn't too long and (b) there is data that needs to be transmitted.
 * \return
 *      Total number of bytes transmitted.
 */
uint32_t
HomaTransport::tryToTransmitData()
{
    uint32_t totalBytesSent = 0;

    // Check to see if we can transmit any data packets. The overall goal
    // here is not to enqueue too many data packets at the NIC at once; this
    // allows us to preempt long messages with shorter ones, and data
    // packets with control packets. The code here only handles data packets;
    // control packets (and retransmitted data) are always passed to the
    // driver immediately.
    int transmitQueueSpace =
            driver->getTransmitQueueSpace(context->dispatch->currentTime);

    // Each iteration of the following loop transmits data packets for a single
    // request or response. The policy here is "shortest remaining processing
    // time" (SRPT) (i.e., choosing the message with the fewest bytes remaining
    // to be transmitted).
    while ((transmitQueueSpace > 0) && !activeOutgoingMessages.empty()) {
        OutgoingMessage* message = &activeOutgoingMessages.front();
        uint32_t maxBytes = std::min(downCast<uint32_t>(transmitQueueSpace),
                message->transmitLimit - message->transmitOffset);

        // Transmit one or more request DATA packets from the message,
        // if appropriate.
        ClientRpc* clientRpc = message->clientRpc;
        ServerRpc* serverRpc = message->serverRpc;
        RpcId rpcId = clientRpc ? clientRpc->rpcId : serverRpc->rpcId;
        uint8_t whoFrom = clientRpc ? FROM_CLIENT : FROM_SERVER;
        const Driver::Address* address = clientRpc ?
                clientRpc->session->serverAddress : serverRpc->clientAddress;
        uint32_t bytesSent = sendBytes(address, rpcId, message->buffer,
                message->transmitOffset, maxBytes,
                message->unscheduledBytes, message->transmitPriority,
                whoFrom);
        if (bytesSent > 0) {
            message->transmitOffset += bytesSent;
            message->lastTransmitTime = driver->getLastTransmitTime();
            transmitQueueSpace -= bytesSent;
            totalBytesSent += bytesSent;
        }

        if (message->transmitOffset == message->buffer->size()) {
            // We have transmitted the last byte of the message.
            if (clientRpc) {
                clientRpc->transmitPending = false;
                erase(outgoingRequests, *clientRpc);
                message->active = false;
                erase(activeOutgoingMessages, *message);
            } else {
                // Delete the ServerRpc object as soon as we have
                // transmitted the last byte. This has the disadvantage
                // that if some of this data is lost we won't be able to
                // retransmit it (the whole RPC will be retried). However,
                // this approach is simpler and faster in the common case
                // where data isn't lost.
                deleteServerRpc(serverRpc);
            }
        } else if (message->transmitOffset == message->transmitLimit) {
            // We have transmitted every byte up to the granted limit.
            message->active = false;
            erase(activeOutgoingMessages, *message);
        } else {
            // We don't have enough queue space to transmit all granted bytes
            // of the top outgoing message; exit to avoid infinite loop.
            break;
        }
    }

    return totalBytesSent;
}

/**
 * Construct a new client session.
 *
 * \throw TransportException
 *      The service locator couldn't be parsed (a log message will
 *      have been generated already).
 */
HomaTransport::Session::Session(HomaTransport* t,
        const ServiceLocator* locator, uint32_t timeoutMs)
    : Transport::Session(locator->getOriginalString())
    , t(t)
    , serverAddress(NULL)
    , aborted(false)
{
    try {
        serverAddress = t->driver->newAddress(locator);
    }
    catch (const Exception& e) {
        LOG(NOTICE, "%s", e.message.c_str());
        throw TransportException(HERE,
                "HomaTransport couldn't parse service locator");
    }
}

/**
 * Destructor for client sessions.
 */
HomaTransport::Session::~Session()
{
    abort();
    delete serverAddress;
}

// See Transport::Session::abort for docs.
void
HomaTransport::Session::abort()
{
    aborted = true;
    for (ClientRpcMap::iterator it = t->outgoingRpcs.begin();
            it != t->outgoingRpcs.end(); ) {
        ClientRpc* clientRpc = it->second;
        it++;
        if (clientRpc->session == this) {
            t->deleteClientRpc(clientRpc);
        }
    }
}

// See Transport::Session::cancelRequest for docs.
void
HomaTransport::Session::cancelRequest(RpcNotifier* notifier)
{
    for (ClientRpcMap::iterator it = t->outgoingRpcs.begin();
            it != t->outgoingRpcs.end(); it++) {
        ClientRpc* clientRpc = it->second;
        if (clientRpc->notifier == notifier) {
            AbortHeader abort(clientRpc->rpcId);
            t->sendControlPacket(this->serverAddress, &abort);
            t->deleteClientRpc(clientRpc);

            // It's no longer safe to use "it", but at this point we're
            // done (the RPC can't exist in the list twice).
            return;
        }
    }
}

// See Transport::Session::getRpcInfo for docs.
string
HomaTransport::Session::getRpcInfo()
{
    string result;
    for (ClientRpcMap::iterator it = t->outgoingRpcs.begin();
            it != t->outgoingRpcs.end(); it++) {
        ClientRpc* clientRpc = it->second;
        if (clientRpc->session != this) {
            continue;
        }
        if (result.size() != 0) {
            result += ", ";
        }
        result += WireFormat::opcodeSymbol(clientRpc->request.buffer);
    }
    if (result.empty())
        result = "no active RPCs";
    result += " to server at ";
    result += serviceLocator;
    return result;
}

// See Transport::Session::sendRequest for docs.
void
HomaTransport::Session::sendRequest(Buffer* request, Buffer* response,
                RpcNotifier* notifier)
{
    uint32_t length = request->size();
    timeTrace("sendRequest invoked, clientId %u, sequence %u, length %u, "
            "%u outgoing requests", t->clientId, t->nextClientSequenceNumber,
            length, t->outgoingRequests.size());
    if (aborted) {
        notifier->failed();
        return;
    }
    if (request->size() > MAX_RPC_LEN) {
        throw TransportException(HERE,
             format("client request exceeds maximum rpc size "
                    "(attempted %u bytes, maximum %u bytes)",
                    request->size(), MAX_RPC_LEN));
    }

    response->reset();
    ClientRpc *clientRpc = t->clientRpcPool.construct(this,
            t->nextClientSequenceNumber, request, response, notifier);
    clientRpc->request.transmitPriority = t->getUnschedTrafficPrio(length);
    clientRpc->request.transmitLimit = std::min(t->roundTripBytes, length);
    t->outgoingRpcs[t->nextClientSequenceNumber] = clientRpc;
    t->nextClientSequenceNumber++;

    uint32_t bytesSent;
    if (length < t->smallMessageThreshold) {
        RpcId rpcId = clientRpc->rpcId;
        assert(length <= t->maxDataPerPacket);
        AllDataHeader header(rpcId, FROM_CLIENT, uint16_t(length));
        Buffer::Iterator iter(request, 0, length);
        timeTrace("client sending ALL_DATA, clientId %u, sequence %u, "
                "priority %u", rpcId.clientId, rpcId.sequence,
                clientRpc->request.transmitPriority);
        t->driver->sendPacket(serverAddress, &header, &iter,
                clientRpc->request.transmitPriority);
        clientRpc->request.transmitOffset = length;
        clientRpc->transmitPending = false;
        bytesSent = length;
    } else {
        t->outgoingRequests.push_back(*clientRpc);
        clientRpc->request.activate(t);
        bytesSent = t->tryToTransmitData();
    }
    if (bytesSent > 0) {
        timeTrace("sendRequest transmitted %u bytes", bytesSent);
    }
}

/**
 * This method is invoked whenever a packet arrives. It is the top-level
 * dispatching method for dealing with incoming packets, both for requests
 * and responses.
 *
 * \param received
 *      Information about the new packet.
 */
void
HomaTransport::handlePacket(Driver::Received* received)
{
    // The following method retrieves a header from a packet
    CommonHeader* common = received->getOffset<CommonHeader>(0);
    if (common == NULL) {
        RAMCLOUD_CLOG(WARNING, "packet from %s too short (%u bytes)",
                received->sender->toString().c_str(), received->len);
        return;
    }

    if (!(common->flags & FROM_CLIENT)) {
        // This packet was sent by the server, and it pertains to an RPC
        // for which we are the client.
        ClientRpcMap::iterator it = outgoingRpcs.find(
                common->rpcId.sequence);
        if (it == outgoingRpcs.end()) {
            // We have no record of this RPC; most likely this packet
            // pertains to an earlier RPC that we've already finished
            // with (e.g., we might have sent a RESEND just before the
            // server since the response). Discard the packet.
            if (common->opcode == LOG_TIME_TRACE) {
                // For LOG_TIME_TRACE requests, dump the trace anyway.
                LOG(NOTICE, "Client received LOG_TIME_TRACE request from "
                        "server %s for (unknown) sequence %lu",
                        received->sender->toString().c_str(),
                        common->rpcId.sequence);
                TimeTrace::record("client received LOG_TIME_TRACE for "
                        "clientId %u, sequence %u",
                        (uint32_t)common->rpcId.clientId,
                        (uint32_t)common->rpcId.sequence);
                TimeTrace::printToLogBackground(context->dispatch);
            }
            TEST_LOG("Discarding unknown packet, sequence %lu",
                    common->rpcId.sequence);
            return;
        }
        ClientRpc* clientRpc = it->second;
        clientRpc->silentIntervals = 0;
        switch (common->opcode) {
            // ALL_DATA from server
            case PacketOpcode::ALL_DATA: {
                // This RPC is now finished.
                AllDataHeader* header = received->getOffset<AllDataHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                uint32_t length;
                char *payload = received->steal(&length);
                uint32_t requiredLength =
                        downCast<uint32_t>(header->messageLength) +
                        sizeof32(AllDataHeader);
                if (length < requiredLength) {
                    RAMCLOUD_CLOG(WARNING, "ALL_DATA response from %s too "
                            "short (got %u bytes, expected %u)",
                            received->sender->toString().c_str(),
                            length, requiredLength);
                    driver->returnPacket(payload);
                    return;
                }
                timeTrace("client received ALL_DATA, clientId %u, sequence %u, "
                        "length %u", header->common.rpcId.clientId,
                        header->common.rpcId.sequence, length);
                Driver::PayloadChunk::appendToBuffer(clientRpc->response,
                        payload + sizeof32(AllDataHeader),
                        header->messageLength, driver, payload);
                clientRpc->notifier->completed();
                deleteClientRpc(clientRpc);
                return;
            }

            // DATA from server
            case PacketOpcode::DATA: {
                DataHeader* header = received->getOffset<DataHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                timeTrace("client received DATA, clientId %u, sequence %u, "
                        "offset %u, length %u",
                        header->common.rpcId.clientId,
                        header->common.rpcId.sequence,
                        header->offset, received->len);
                if (!clientRpc->accumulator) {
                    uint32_t totalLength = header->totalLength;
                    clientRpc->accumulator.construct(this, clientRpc->response,
                            totalLength);
                    if (totalLength > header->unscheduledBytes) {
                        clientRpc->scheduledMessage.construct(
                                clientRpc->rpcId, clientRpc->accumulator.get(),
                                uint32_t(header->unscheduledBytes),
                                clientRpc->session->serverAddress, totalLength,
                                uint8_t(FROM_SERVER));
                    }
                }
                bool retainPacket = clientRpc->accumulator->addPacket(header,
                        received->len);
                dataPacketArrive(clientRpc->scheduledMessage.get());
                if (clientRpc->response->size() >= header->totalLength) {
                    // Response complete.
                    if (clientRpc->response->size() > header->totalLength) {
                        // We have more bytes than we want. This can happen
                        // if the last packet gets padded by the network
                        // layer to meet minimum size requirements. Just
                        // truncate the response.
                        clientRpc->response->truncate(header->totalLength);
                    }
                    clientRpc->notifier->completed();
                    deleteClientRpc(clientRpc);
                }

                if (retainPacket) {
                    uint32_t dummy;
                    received->steal(&dummy);
                } else {
                    LOG(DEBUG, "Redundant DATA from server, sequence %lu, "
                            "offset %u, totalLength %u",
                            header->common.rpcId.sequence, header->offset,
                            header->totalLength);
                }
                return;
            }

            // GRANT from server
            case PacketOpcode::GRANT: {
                GrantHeader* header = received->getOffset<GrantHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                timeTrace("client received GRANT, clientId %u, sequence %u, "
                        "offset %u",
                        header->common.rpcId.clientId,
                        header->common.rpcId.sequence,
                        header->offset);
                OutgoingMessage* request = &clientRpc->request;
                uint32_t grantOffset = std::min(header->offset,
                        request->buffer->size());
                if (grantOffset > request->transmitLimit) {
                    request->transmitLimit = grantOffset;
                    request->transmitPriority = header->priority;
                    if (!request->active) {
                        request->activate(this);
                    }
                }
                return;
            }

            // LOG_TIME_TRACE from server
            case PacketOpcode::LOG_TIME_TRACE: {
                LOG(NOTICE, "Client received LOG_TIME_TRACE request from "
                        "server %s for clientId %lu, sequence %lu",
                        received->sender->toString().c_str(),
                        common->rpcId.clientId, common->rpcId.sequence);
                TimeTrace::record("client received LOG_TIME_TRACE for "
                        "clientId %u, sequence %u",
                        (uint32_t)common->rpcId.clientId,
                        (uint32_t)common->rpcId.sequence);
                TimeTrace::printToLogBackground(context->dispatch);
                return;
            }

            // RESEND from server
            case PacketOpcode::RESEND: {
                ResendHeader* header = received->getOffset<ResendHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                timeTrace("client received RESEND, clientId %u, sequence %u, "
                        "offset %u, length %u",
                        header->common.rpcId.clientId,
                        header->common.rpcId.sequence,
                        header->offset, header->length);
                OutgoingMessage* request = &clientRpc->request;
                if (header->common.flags & RESTART) {
                    // Reset the RPC to its pristine state.
                    if (!clientRpc->transmitPending) {
                        clientRpc->transmitPending = true;
                        outgoingRequests.push_back(*clientRpc);
                    }
                    if (request->active) {
                        request->active = false;
                        erase(activeOutgoingMessages, *request);
                    }
                    request->transmitOffset = 0;
                    request->transmitPriority =
                            getUnschedTrafficPrio(request->buffer->size());
                    request->transmitLimit =
                            std::min(header->length, request->buffer->size());
                    request->activate(this);
                    clientRpc->response->reset();
                    clientRpc->accumulator.destroy();
                    clientRpc->scheduledMessage.destroy();
                    return;
                }
                uint32_t resendEnd = std::min(header->offset + header->length,
                        request->buffer->size());
                if (resendEnd > request->transmitLimit) {
                    // Needed in case a GRANT packet was lost.
                    request->transmitLimit = resendEnd;
                    if (!request->active) {
                        request->activate(this);
                    }
                }
                if ((header->offset >= request->transmitOffset)
                        || ((Cycles::rdtsc() - request->lastTransmitTime)
                        < timerInterval)) {
                    // One of two things has happened: either (a) we haven't
                    // yet sent the requested bytes for the first time (there
                    // must be other outgoing traffic with higher priority)
                    // or (b) we transmitted data recently. In either case,
                    // it's unlikely that bytes have been lost, so don't
                    // retransmit; just return an BUSY so the server knows
                    // we're still alive.
                    BusyHeader busy(header->common.rpcId, FROM_CLIENT);
                    sendControlPacket(clientRpc->session->serverAddress, &busy);
                    return;
                }
                double elapsedMicros = Cycles::toSeconds(Cycles::rdtsc()
                        - request->lastTransmitTime)*1e06;
                // As of 2017/12, running W4 and W5 with 8 priorities under
                // high load will generate some spurious retransmission
                // messages in the log. The spurious retransmission seems to
                // happen when some bytes of a large message sent with the
                // lowest priorities gets stuck at the TOR switch queue for
                // too long.
                RAMCLOUD_CLOG(WARNING, "Retransmitting to server %s: "
                        "sequence %lu, offset %u, length %u, priority %u, "
                        "elapsed time %.1f us",
                        received->sender->toString().c_str(),
                        header->common.rpcId.sequence, header->offset,
                        header->length, header->priority, elapsedMicros);
                timeTrace("Retransmitting to server clientId %u"
                        "sequence %u, offset %u, length %u",
                        header->common.rpcId.clientId,
                        header->common.rpcId.sequence, header->offset,
                        header->length);
                // Resent bytes are passed directly to the NIC for simplicity;
                // we expect retransmission to be rare enough so that this
                // won't affect even the tail latency of other messages.
                sendBytes(clientRpc->session->serverAddress,
                        header->common.rpcId, clientRpc->request.buffer,
                        header->offset, header->length,
                        request->unscheduledBytes, header->priority,
                        FROM_CLIENT|RETRANSMISSION);
                request->lastTransmitTime = driver->getLastTransmitTime();
                return;
            }

            // BUSY from server
            case PacketOpcode::BUSY: {
                // Nothing to do.
                timeTrace("client received BUSY, clientId %u, sequence %u",
                        common->rpcId.clientId, common->rpcId.sequence);
                return;
            }

            default:
            RAMCLOUD_CLOG(WARNING,
                    "unexpected opcode %s received from server %s",
                    opcodeSymbol(common->opcode).c_str(),
                    received->sender->toString().c_str());
            return;
        }
    } else {
        // This packet was sent by the client; it relates to an RPC
        // for which we are the server.

        // Find the record for this RPC, if one exists.
        ServerRpc* serverRpc = NULL;
        ServerRpcMap::iterator it = incomingRpcs.find(common->rpcId);
        if (it != incomingRpcs.end()) {
            serverRpc = it->second;
            serverRpc->silentIntervals = 0;
        }

        switch (common->opcode) {
            // ALL_DATA from client
            case PacketOpcode::ALL_DATA: {
                // Common case: the entire request fit in a single packet.

                AllDataHeader* header = received->getOffset<AllDataHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                if (serverRpc != NULL) {
                    // This shouldn't normally happen: it means this packet is
                    // a duplicate, so we can just discard it.
                    return;
                }
                uint32_t length;
                char *payload = received->steal(&length);
                uint32_t requiredLength =
                        downCast<uint32_t>(header->messageLength) +
                        sizeof32(AllDataHeader);
                if (length < requiredLength) {
                    RAMCLOUD_CLOG(WARNING, "ALL_DATA request from %s too "
                            "short (got %u bytes, expected %u)",
                            received->sender->toString().c_str(),
                            length, requiredLength);
                    driver->returnPacket(payload);
                    return;
                }
                timeTrace("server received ALL_DATA, clientId %u, sequence %u, "
                          "length %u", header->common.rpcId.clientId,
                          header->common.rpcId.sequence, length);
                serverRpc = serverRpcPool.construct(this,
                        nextServerSequenceNumber, received->sender,
                        header->common.rpcId);
                nextServerSequenceNumber++;
                incomingRpcs[header->common.rpcId] = serverRpc;
                Driver::PayloadChunk::appendToBuffer(&serverRpc->requestPayload,
                        payload + sizeof32(AllDataHeader),
                        header->messageLength, driver, payload);
                serverRpc->requestComplete = true;
                context->workerManager->handleRpc(serverRpc);
                return;
            }

            // DATA from client
            case PacketOpcode::DATA: {
                DataHeader* header = received->getOffset<DataHeader>(0);
                bool retainPacket = false;
                if (header == NULL)
                    goto packetLengthError;
                timeTrace("server received DATA, clientId %u, sequence %u, "
                        "offset %u, length %u",
                        header->common.rpcId.clientId,
                        header->common.rpcId.sequence,
                        header->offset, received->len);
                if (serverRpc == NULL) {
                    serverRpc = serverRpcPool.construct(this,
                            nextServerSequenceNumber, received->sender,
                            header->common.rpcId);
                    nextServerSequenceNumber++;
                    incomingRpcs[header->common.rpcId] = serverRpc;
                    uint32_t totalLength = header->totalLength;
                    serverRpc->accumulator.construct(this,
                            &serverRpc->requestPayload, totalLength);
                    if (totalLength > header->unscheduledBytes) {
                        serverRpc->scheduledMessage.construct(
                                serverRpc->rpcId, serverRpc->accumulator.get(),
                                uint32_t(header->unscheduledBytes),
                                serverRpc->clientAddress, totalLength,
                                uint8_t(FROM_CLIENT));
                    }
                    serverTimerList.push_back(*serverRpc);
                } else if (serverRpc->requestComplete) {
                    // We've already received the full message, so
                    // ignore this packet.
                    TEST_LOG("ignoring extraneous packet");
                    goto serverDataDone;
                }
                retainPacket = serverRpc->accumulator->addPacket(header,
                        received->len);
                if (header->offset == 0) {
                    timeTrace("server received opcode %u, totalLength %u",
                            serverRpc->requestPayload.getStart<
                            WireFormat::RequestCommon>()->opcode,
                            header->totalLength);
                }
                dataPacketArrive(serverRpc->scheduledMessage.get());
                if (serverRpc->requestPayload.size() >= header->totalLength) {
                    // Message complete; start servicing the RPC.
                    if (serverRpc->requestPayload.size()
                            > header->totalLength) {
                        // We have more bytes than we want. This can happen
                        // if the last packet gets padded by the network
                        // layer to meet minimum size requirements. Just
                        // truncate the request.
                        serverRpc->requestPayload.truncate(header->totalLength);
                    }
                    erase(serverTimerList, *serverRpc);
                    serverRpc->requestComplete = true;
                    context->workerManager->handleRpc(serverRpc);
                }

                serverDataDone:
                if (retainPacket) {
                    uint32_t dummy;
                    received->steal(&dummy);
                } else {
                    LOG(DEBUG, "Redundant DATA from client, clientId %lu, "
                            "sequence %lu, offset %u, totalLength %u",
                            header->common.rpcId.clientId,
                            header->common.rpcId.sequence,
                            header->offset, header->totalLength);
                }
                return;
            }

            // GRANT from client
            case PacketOpcode::GRANT: {
                GrantHeader* header = received->getOffset<GrantHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                timeTrace("server received GRANT, clientId %u, sequence %u, "
                        "offset %u",
                        header->common.rpcId.clientId,
                        header->common.rpcId.sequence,
                        header->offset);
                if ((serverRpc == NULL) || !serverRpc->sendingResponse) {
                    RAMCLOUD_LOG(WARNING, "unexpected GRANT from client %s, "
                            "id (%lu,%lu), grantOffset %u, serverRpc %s",
                            received->sender->toString().c_str(),
                            header->common.rpcId.clientId,
                            header->common.rpcId.sequence, header->offset,
                            serverRpc ? "not sending response" : "not found");
                    return;
                }
                OutgoingMessage* response = &serverRpc->response;
                uint32_t grantOffset = std::min(header->offset,
                        response->buffer->size());
                if (grantOffset > response->transmitLimit) {
                    response->transmitLimit = grantOffset;
                    response->transmitPriority = header->priority;
                    if (!response->active) {
                        response->activate(this);
                    }
                }
                return;
            }

            // LOG_TIME_TRACE from client
            case PacketOpcode::LOG_TIME_TRACE: {
                LOG(NOTICE, "Server received LOG_TIME_TRACE request from "
                        "client %s for sequence %lu",
                        received->sender->toString().c_str(),
                        common->rpcId.sequence);
                TimeTrace::record("server received LOG_TIME_TRACE for "
                        "clientId %u, sequence %u",
                        (uint32_t)common->rpcId.clientId,
                        (uint32_t)common->rpcId.sequence);
                TimeTrace::printToLogBackground(context->dispatch);
                return;
            }

            // RESEND from client
            case PacketOpcode::RESEND: {
                ResendHeader* header = received->getOffset<ResendHeader>(0);
                if (header == NULL)
                    goto packetLengthError;
                timeTrace("server received RESEND, clientId %u, sequence %u, "
                        "offset %u, length %u",
                        header->common.rpcId.clientId,
                        header->common.rpcId.sequence,
                        header->offset, header->length);

                if (serverRpc == NULL) {
                    // This situation can happen if we never received the
                    // request, or if a packet of the response got lost but
                    // we have already freed the ServerRpc. In either case,
                    // ask the client to restart the RPC from scratch.
                    timeTrace("server requesting restart, clientId %u, "
                            "sequence %u",
                            common->rpcId.clientId, common->rpcId.sequence);
                    ResendHeader resend(header->common.rpcId, 0,
                            roundTripBytes, 0, FROM_SERVER|RESTART);
                    sendControlPacket(received->sender, &resend);
                    return;
                }
                OutgoingMessage* response = &serverRpc->response;
                uint32_t resendEnd = std::min(header->offset + header->length,
                        response->buffer->size());
                if (resendEnd > response->transmitLimit) {
                    // Needed in case GRANT packet was lost.
                    response->transmitLimit = resendEnd;
                    if (!response->active && serverRpc->sendingResponse) {
                        response->activate(this);
                    }
                }
                if (!serverRpc->sendingResponse
                        || (header->offset >= response->transmitOffset)
                        || ((Cycles::rdtsc() - response->lastTransmitTime)
                        < timerInterval)) {
                    // One of two things has happened: either (a) we haven't
                    // yet sent the requested bytes for the first time (there
                    // must be other outgoing traffic with higher priority)
                    // or (b) we transmitted data recently, so it might have
                    // crossed paths with the RESEND request. In either case,
                    // it's unlikely that bytes have been lost, so don't
                    // retransmit; just return an BUSY so the client knows
                    // we're still alive.
                    timeTrace("server about to send BUSY, clientId %u, "
                            "sequence %u", serverRpc->rpcId.clientId,
                            serverRpc->rpcId.sequence);
                    BusyHeader busy(serverRpc->rpcId, FROM_SERVER);
                    sendControlPacket(serverRpc->clientAddress, &busy);
                    return;
                }
                double elapsedMicros = Cycles::toSeconds(Cycles::rdtsc()
                        - response->lastTransmitTime)*1e06;
                RAMCLOUD_CLOG(WARNING, "Retransmitting to client %s: "
                        "sequence %lu, offset %u, length %u, priority %u, "
                        "elapsed time %.1f us",
                        received->sender->toString().c_str(),
                        header->common.rpcId.sequence, header->offset,
                        header->length, header->priority, elapsedMicros);
                timeTrace("Retransmitting to clientId %u, "
                        "sequence %u, offset %u, length %u",
                        header->common.rpcId.clientId,
                        header->common.rpcId.sequence, header->offset,
                        header->length);
                sendBytes(serverRpc->clientAddress,
                        serverRpc->rpcId, &serverRpc->replyPayload,
                        header->offset, header->length,
                        response->unscheduledBytes, header->priority,
                        RETRANSMISSION|FROM_SERVER);
                response->lastTransmitTime = driver->getLastTransmitTime();
                return;
            }

            // BUSY from client
            case PacketOpcode::BUSY: {
                // Nothing to do.
                timeTrace("server received BUSY, clientId %u, sequence %u",
                        common->rpcId.clientId, common->rpcId.sequence);
                return;
            }

            // ABORT from client
            case PacketOpcode::ABORT: {
                timeTrace("server received ABORT, clientId %u, sequence %u",
                        common->rpcId.clientId, common->rpcId.sequence);
                if (serverRpc != NULL) {
                    // Delete the ServerRpc if it is not being processed.
                    // Otherwise, delay the deletion to sendReply().
                    if (!serverRpc->requestComplete ||
                            serverRpc->sendingResponse) {
                        deleteServerRpc(serverRpc);
                    } else {
                        serverRpc->cancelled = true;
                    }
                }
                return;
            }

            default:
                RAMCLOUD_CLOG(WARNING,
                        "unexpected opcode %s received from client %s",
                        opcodeSymbol(common->opcode).c_str(),
                        received->sender->toString().c_str());
                return;
        }
    }

    packetLengthError:
    RAMCLOUD_CLOG(WARNING, "packet of type %s from %s too short (%u bytes)",
            opcodeSymbol(common->opcode).c_str(),
            received->sender->toString().c_str(),
            received->len);

}

/**
 * Insert this outgoing message into the right place of the ordered active
 * outgoing message list.
 *
 * \pre
 *      This message must have grants available and cannot be in the list
 *      already.
 */
void
HomaTransport::OutgoingMessage::activate(HomaTransport* t)
{
    assert(!active && (transmitOffset < transmitLimit));
    active = true;
    uint32_t remainingBytes = buffer->size() - transmitOffset;
    for (OutgoingMessageList::iterator it = t->activeOutgoingMessages.begin();
            it != t->activeOutgoingMessages.end(); it++) {
        OutgoingMessage* insertHere = &(*it);
        if (remainingBytes <
                insertHere->buffer->size() - insertHere->transmitOffset) {
            insertBefore(t->activeOutgoingMessages, *this, *insertHere);
            return;
        }
    }
    t->activeOutgoingMessages.push_back(*this);
}

/**
 * Returns a string containing human-readable information about the client
 * that initiated this RPC. Right now this isn't formatted as a service
 * locator; it just describes a Driver::Address.
 */
string
HomaTransport::ServerRpc::getClientServiceLocator()
{
    return clientAddress->toString();
}

/**
 * This method is invoked when a server has finished processing an RPC.
 * It begins transmitting the response back to the client, but returns
 * before that process is complete.
 */
void
HomaTransport::ServerRpc::sendReply()
{
    uint32_t length = replyPayload.size();
    timeTrace("sendReply invoked, clientId %u, sequence %u, length %u, "
            "%u outgoing responses", rpcId.clientId, rpcId.sequence,
            length, t->outgoingResponses.size());
    if (cancelled) {
        t->deleteServerRpc(this);
        return;
    }
    if (replyPayload.size() > MAX_RPC_LEN) {
        throw TransportException(HERE,
             format("server response exceeds maximum rpc size "
                    "(attempted %u bytes, maximum %u bytes)",
                    replyPayload.size(), MAX_RPC_LEN));
    }

    uint32_t bytesSent;
    response.transmitPriority = t->getUnschedTrafficPrio(length);
    response.transmitLimit = std::min(t->roundTripBytes, length);
    if (length < t->smallMessageThreshold) {
        AllDataHeader header(rpcId, FROM_SERVER, uint16_t(length));
        Buffer::Iterator iter(&replyPayload, 0, length);
        timeTrace("server sending ALL_DATA, clientId %u, sequence %u, "
                "priority %u", rpcId.clientId, rpcId.sequence,
                response.transmitPriority);
        t->driver->sendPacket(clientAddress, &header, &iter,
                response.transmitPriority);
        t->deleteServerRpc(this);
        bytesSent = length;
    } else {
        sendingResponse = true;
        t->outgoingResponses.push_back(*this);
        t->serverTimerList.push_back(*this);
        response.activate(t);
        bytesSent = t->tryToTransmitData();
    }
    if (bytesSent > 0) {
        timeTrace("sendReply transmitted %u bytes", bytesSent);
    }
}

/**
 * Construct a MessageAccumulator.
 *
 * \param t
 *      Overall information about the transport.
 * \param buffer
 *      The complete message will be assembled here; caller should ensure
 *      that this is initially empty. The caller owns the storage for this
 *      and must ensure that it persists as long as this object persists.
 * \param totalLength
 *      Length of the message, in bytes.
 */
HomaTransport::MessageAccumulator::MessageAccumulator(HomaTransport* t,
        Buffer* buffer, uint32_t totalLength)
    : t(t)
    , buffer(buffer)
    , fragments()
    , totalLength(totalLength)
{
    assert(buffer->size() == 0);
    buffer->reserve(totalLength);
}

/**
 * Destructor for MessageAccumulators.
 */
HomaTransport::MessageAccumulator::~MessageAccumulator()
{
    // If there are any unassembled fragments, then we must release
    // them back to the driver.
    for (FragmentMap::iterator it = fragments.begin();
            it != fragments.end(); it++) {
        MessageFragment fragment = it->second;
        t->driver->returnPacket(fragment.header);
    }
    fragments.clear();
}

/**
 * This method is invoked whenever a new DATA packet arrives for a partially
 * complete message. It saves information about the new fragment and
 * (eventually) combines all of the fragments into a complete message.
 *
 * \param header
 *      Pointer to the first byte of the packet, which must be a valid
 *      DATA packet.
 * \param length
 *      Total number of bytes in the packet.
 * \return
 *      The return value is true if we have retained a pointer to the
 *      packet (meaning that the caller should "steal" the Received, if
 *      it hasn't already). False means that the data in this packet
 *      was all redundant; we didn't save anything, so the caller need
 *      not steal the Received.
 */
bool
HomaTransport::MessageAccumulator::addPacket(DataHeader *header,
        uint32_t length)
{
    length -= sizeof32(DataHeader);

    // We only allow a partial packet to appear at the end of a message.
    assert(header->offset % t->maxDataPerPacket == 0);
    assert((length == t->maxDataPerPacket) ||
            (header->offset + length == header->totalLength));

    bool retainPacket;
    if (header->offset > buffer->size()) {
        // Can't append this packet into the buffer because some prior
        // data is missing. Save the packet for later, if it's not redundant.
        FragmentMap::iterator iter;
        std::tie(iter, retainPacket) = fragments.emplace(
                uint32_t(header->offset), MessageFragment(header, length));
        return retainPacket;
    }

    // Append this fragment to the assembled message buffer, then see
    // if some of the unappended fragments can now be appended as well.
    if (header->offset == buffer->size()) {
        // Each iteration of the following loop appends one fragment to
        // the buffer.
        MessageFragment fragment(header, length);
        do {
            char* payload = reinterpret_cast<char*>(fragment.header);
            // Currently, the first packet of a multi-packet message must
            // be retained in the buffer because ServerRpc::clientAddress
            // is pointing to some memory owned by the packet buffer (see
            // docs of Driver::Received::sender). In contrast, other packets
            // are copied out to reduce the number of chunks in the buffer
            // and, thus, jitters caused by destroying the buffer.
            if (expect_false(header->offset == 0)) {
                Driver::PayloadChunk::appendToBuffer(buffer,
                        payload + sizeof32(DataHeader), fragment.length,
                        t->driver, payload);
            } else {
                buffer->appendCopy(payload + sizeof32(DataHeader),
                        fragment.length);
                if (fragment.header != header) {
                    // This packet was retained earlier due to out-of-order
                    // arrival and must be returned to driver now.
                    t->driver->returnPacket(payload);
                }
            }
            FragmentMap::iterator it = fragments.find(buffer->size());
            if (it == fragments.end()) {
                // Only the first packet will be retained.
                return (header->offset == 0);
            } else {
                fragment = it->second;
                fragments.erase(it);
            }
        } while (true);
    } else {
        // This packet is redundant.
        return false;
    }
}

/**
 * This method is invoked to issue a RESEND packet when it appears that
 * packets have been lost. It is used by both servers and clients.
 *
 * \param t
 *      Overall information about the transport.
 * \param address
 *      Network address to which the RESEND should be sent.
 * \param rpcId
 *      Unique identifier for the RPC in question.
 * \param grantOffset
 *      Largest grantOffset that we have sent for this message (i.e.
 *      this is how many total bytes we should have received already).
 *      May be 0 if the sender never requested a grant (meaning that it
 *      planned to transmit the entire message unilaterally).
 * \param priority
 *      The priority we request the sender to use to transmit the lost
 *      data. See docs for ResendHeader.
 * \param whoFrom
 *      Must be either FROM_CLIENT, indicating that we are the client, or
 *      FROM_SERVER, indicating that we are the server.
 *
 * \return
 *      The offset of the byte just after the last one whose retransmission
 *      was requested.
 */
uint32_t
HomaTransport::MessageAccumulator::requestRetransmission(HomaTransport *t,
        const Driver::Address* address, RpcId rpcId, uint32_t grantOffset,
        int priority, uint8_t whoFrom)
{
    if ((reinterpret_cast<uint64_t>(&fragments) < 0x1000lu)) {
        DIE("Bad fragment pointer: %p", &fragments);
    }
    uint32_t endOffset;

    // Compute the end of the retransmission range.
    if (!fragments.empty()) {
        // Retransmit the entire gap up to the first fragment.
        endOffset = ~0u;
        for (FragmentMap::iterator it = fragments.begin();
                it != fragments.end(); it++) {
            endOffset = std::min(endOffset, it->first);
        }
    } else if (grantOffset > 0) {
        // Retransmit everything that we've asked the sender to send:
        // we don't seem to have received any of it.
        endOffset = grantOffset;
    } else {
        // We haven't issued a GRANT for this message; just request
        // the first round-trip's worth of data. Once this data arrives,
        // the normal grant mechanism should kick in if it's still needed.
        endOffset = t->roundTripBytes;
    }
    if (endOffset <= buffer->size()) {
        DIE("Bad endOffset %u, offset %u", endOffset, buffer->size());
    }
    const char* fmt = (whoFrom == FROM_SERVER) ?
            "server requesting retransmission of bytes %u-%u, clientId %u, "
            "sequence %u" :
            "client requesting retransmission of bytes %u-%u, clientId %u, "
            "sequence %u";
    timeTrace(fmt, buffer->size(), endOffset, rpcId.clientId, rpcId.sequence);
    ResendHeader resend(rpcId, buffer->size(), endOffset - buffer->size(),
            downCast<uint8_t>(priority), whoFrom);
    t->sendControlPacket(address, &resend);
    return endOffset;
}

/**
 * Constructor for ScheduledMessages.
 *
 * \param rpcId
 *      Unique identifier for the RPC this message belongs to.
 * \param accumulator
 *      Overall information about this multi-packet message.
 * \param unscheduledBytes
 *      # bytes sent unilaterally.
 * \param senderAddress
 *      Network address of the message sender.
 * \param totalLength
 *      Total # bytes in the message.
 * \param whoFrom
 *      Must be either FROM_CLIENT, indicating that this is a request, or
 *      FROM_SERVER, indicating that this is a response.
 */
HomaTransport::ScheduledMessage::ScheduledMessage(RpcId rpcId,
        MessageAccumulator* accumulator, uint32_t unscheduledBytes,
        const Driver::Address* senderAddress, uint32_t totalLength,
        uint8_t whoFrom)
    : accumulator(accumulator)
    , activeMessageLinks()
    , inactiveMessageLinks()
    , grantOffset(unscheduledBytes)
    , grantPriority()
    , rpcId(rpcId)
    , senderAddress(senderAddress)
    , senderHash(senderAddress->getHash())
    , state(NEW)
    , totalLength(totalLength)
    , whoFrom(whoFrom)
{
    accumulator->t->tryToSchedule(this);
}

/**
 * Destructor for ScheduledMessages.
 */
HomaTransport::ScheduledMessage::~ScheduledMessage()
{
    if (state == ACTIVE) {
        // Set grantOffset to a large number so that this message will be
        // purged by replaceActiveMessage.
        grantOffset = ~0u;
        accumulator->t->replaceActiveMessage(this, NULL);
    } else if (state == INACTIVE) {
        erase(accumulator->t->inactiveMessages, *this);
    }
}

/**
 * Compare the relative precedence of two scheduled messages in the message
 * scheduler.
 *
 * \param other
 *      The other message to compare with.
 * \return
 *      Negative number if this message has higher precedence; positive
 *      number if the other message has higher precedence; 0 if the two
 *      messages have equal precedence.
 */
int
HomaTransport::ScheduledMessage::compareTo(ScheduledMessage& other) const
{
    // Implement the SRPT policy.
    int r0 = totalLength - accumulator->buffer->size();
    int r1 = other.totalLength - other.accumulator->buffer->size();
    return r0 - r1;
}

/**
 * This method is invoked in the inner polling loop of the dispatcher;
 * it drives the operation of the transport.
 * \return
 *      The return value is 1 if this method found something useful to do,
 *      0 otherwise.
 */
int
HomaTransport::Poller::poll()
{
    int result = 0;

#if TIME_TRACE
    uint64_t startTime = Cycles::rdtsc();
#endif

    // Try to receive MAX_PACKETS packets at a time (an optimized driver
    // implementation may prefetch the payloads for us). As of 07/2016,
    // MAX_PACKETS is set to 8 because our CPU can take at most 8 cache
    // misses at a time (although it's not clear 8 is the best value).
#define MAX_PACKETS 8
    t->driver->receivePackets(MAX_PACKETS, &t->receivedPackets);
    uint32_t numPackets = downCast<uint32_t>(t->receivedPackets.size());

    // Process any incoming packet.
    if (numPackets > 0) {
        result = 1;
#if TIME_TRACE
        // Log the beginning of poll() here so that timetrace entries do not
        // go back in time.
        uint32_t ns = static_cast<uint32_t>(
                Cycles::toNanoseconds(startTime - lastPollTime));
        TimeTrace::record(startTime, "start of polling iteration %u, "
                "last poll was %u ns ago", uint32_t(owner->iteration), ns);
#endif
        for (uint32_t i = 0; i < numPackets; i++) {
            t->handlePacket(&t->receivedPackets[i]);
        }
        t->receivedPackets.clear();

        // See if we should send out new GRANT packets. Grants are sent here as
        // opposed to inside #handlePacket because we would like to coalesse
        // GRANT packets to the same message whenever possible. Besides,
        // structuring code this way seems to improve the overall performance,
        // potentially by being more cache-friendly.
        while (!t->messagesToGrant.empty()) {
            ScheduledMessage* recipient = t->messagesToGrant.back();
            t->messagesToGrant.pop_back();
            uint8_t whoFrom = (recipient->whoFrom == FROM_CLIENT) ?
                    FROM_SERVER : FROM_CLIENT;
            GrantHeader grant(recipient->rpcId, recipient->grantOffset,
                    downCast<uint8_t>(recipient->grantPriority), whoFrom);
            const char* fmt = (whoFrom == FROM_CLIENT) ?
                    "client sending GRANT, clientId %u, sequence %u, offset %u,"
                    " priority %u" :
                    "server sending GRANT, clientId %u, sequence %u, offset %u,"
                    " priority %u";
            timeTrace(fmt, recipient->rpcId.clientId, recipient->rpcId.sequence,
                    grant.offset, grant.priority);
            t->sendControlPacket(recipient->senderAddress, &grant);
        }
    }

    // See if we should check for timeouts. Ideally, we'd like to do this
    // every timerInterval. However, it's better not to call checkTimeouts
    // when there are input packets pending, since checkTimeouts might then
    // request retransmission of a packet that's waiting in the NIC. Thus,
    // if necessary, we delay the call to checkTimeouts to find a time when
    // we are caught up on input packets. If a long time goes by without ever
    // catching up, then we invoke checkTimeouts anyway.
    //
    // Note: it isn't a disaster if we occasionally request an unnecessary
    // retransmission, since the protocol will handle this fine. However, if
    // too many of these happen, it will create noise in the logs, which will
    // make it harder to notice when a *real* problem happens. Thus, it's
    // best to eliminate spurious retransmissions as much as possible.
    uint64_t now = owner->currentTime;
    if (expect_false(now >= t->nextTimeoutCheck)) {
        if (t->timeoutCheckDeadline == 0) {
            t->timeoutCheckDeadline = now + t->timerInterval;
        }
        if ((numPackets < MAX_PACKETS)
                || (now >= t->timeoutCheckDeadline)) {
            if (numPackets == MAX_PACKETS) {
                RAMCLOUD_CLOG(NOTICE, "Deadline invocation of checkTimeouts");
                timeTrace("Deadline invocation of checkTimeouts");
            }
            t->checkTimeouts();
            result = 1;
            t->nextTimeoutCheck = now + t->timerInterval;
            t->timeoutCheckDeadline = 0;
        }
    }

    // Transmit data packets if possible.
    uint32_t totalBytesSent = t->tryToTransmitData();
    result += totalBytesSent;

    // Release packet buffers that have been returned to the driver.
    t->driver->release();

#if TIME_TRACE
    lastPollTime = startTime;
#endif
    if (result) {
        timeTrace("end of polling iteration %u, received %u packets, "
                "transmitted %u bytes", owner->iteration, numPackets,
                totalBytesSent);
    }
    return result;
}

/**
 * This method is invoked by poll at regular intervals to check for
 * unexpected lapses in communication. It implements all of the timer-related
 * functionality for both clients and servers, such as requesting packet
 * retransmission and aborting RPCs.
 */
void
HomaTransport::checkTimeouts()
{
    timeTrace("checkTimeouts invoked, %u client RPCs, %u server RPCs",
            outgoingRpcs.size(), serverTimerList.size());

    // Scan all of the ClientRpc objects.
    for (ClientRpcMap::iterator it = outgoingRpcs.begin();
            it != outgoingRpcs.end(); ) {
        uint64_t sequence = it->first;
        ClientRpc* clientRpc = it->second;
        OutgoingMessage* request = &clientRpc->request;
        if (request->transmitOffset < request->transmitLimit) {
            // We haven't finished transmitting every granted byte of the
            // request (our transmit queue is probably backed up), so no
            // need to worry about whether we have heard from the server.
            it++;
            continue;
        }
        clientRpc->silentIntervals++;

        // Advance the iterator here, so that it won't get invalidated if
        // we delete the ClientRpc below.
        it++;

        assert(timeoutIntervals > 2*pingIntervals);
        if (clientRpc->silentIntervals >= timeoutIntervals) {
            // A long time has elapsed with no communication whatsoever
            // from the server, so abort the RPC.
            RAMCLOUD_LOG(WARNING, "aborting %s RPC to server %s, "
                    "sequence %lu: timeout",
                    WireFormat::opcodeSymbol(clientRpc->request.buffer),
                    clientRpc->session->serverAddress->toString().c_str(),
                    sequence);
            timeTrace("aborting client RPC, clientId %u, sequence %u",
                    clientId, sequence);
            clientRpc->notifier->failed();
            deleteClientRpc(clientRpc);
            continue;
        }

        if (clientRpc->silentIntervals < 2) {
            // Make sure the clientRpc has experienced at least one full
            // timerInterval before we start to deal with it.
            continue;
        }

        if (clientRpc->response->size() == 0) {
            // We haven't received any part of the response message. Normally,
            // it's the server's responsibility to request retransmission.
            // However, in case the whole request was lost (so the server is
            // not aware of this RPC), the server had higher priority messages
            // to grant to or the server crashed, we need to send occasional
            // RESEND packets, which should produce some response from the
            // server, so that we know the server will take care of this
            // situation. Note: the wait time for this ping is longer than
            // the server's wait time to request retransmission (first give the
            // server a chance to handle the problem).
            if (clientRpc->silentIntervals % pingIntervals == 0) {
                timeTrace("client sending RESEND for clientId %u, "
                        "sequence %u", clientId, sequence);
                ResendHeader resend(RpcId(clientId, sequence), 0,
                        roundTripBytes, getUnschedTrafficPrio(roundTripBytes),
                        FROM_CLIENT);
                sendControlPacket(clientRpc->session->serverAddress,
                        &resend);
            }
        } else {
            // We have received part of the response.
            assert(clientRpc->accumulator);
            ScheduledMessage* scheduledMessage =
                    clientRpc->scheduledMessage.get();
            uint32_t grantOffset = scheduledMessage ?
                    scheduledMessage->grantOffset : 0;
            if (grantOffset == clientRpc->response->size()) {
                // The client has received every granted byte but hasn't got
                // around to grant more because there are higher priority
                // responses. Send BUSY to tell the server don't time out on
                // this RPC.
                clientRpc->silentIntervals = 0;
                BusyHeader busy(RpcId(clientId, sequence), FROM_CLIENT);
                sendControlPacket(clientRpc->session->serverAddress, &busy);
            } else {
                // The client expects to receive more but the server
                // has gone silent, this must mean packets were lost,
                // grants were lost, or the server has preempted this
                // response for higher priority messages, so request
                // retransmission anyway.
                if (clientRpc->silentIntervals % pingIntervals == 0) {
                    MessageAccumulator* accumulator =
                            clientRpc->accumulator.get();
                    int priority = scheduledMessage ?
                            scheduledMessage->grantPriority :
                            getUnschedTrafficPrio(accumulator->totalLength);
                    accumulator->requestRetransmission(this,
                            clientRpc->session->serverAddress,
                            RpcId(clientId, sequence), grantOffset,
                            priority, FROM_CLIENT);
                }
            }
        }
    }

    // Scan all of the ServerRpc objects for which network I/O is in
    // progress (either for the request or the response).
    for (ServerTimerList::iterator it = serverTimerList.begin();
            it != serverTimerList.end(); ) {
        ServerRpc* serverRpc = &(*it);
        OutgoingMessage* response = &serverRpc->response;
        if (serverRpc->sendingResponse &&
                (response->transmitOffset < response->transmitLimit)) {
            // Looks like the transmit queue has been too backed up to finish
            // transmitting every granted byte of the response, so no need to
            // check for a timeout.
            it++;
            continue;
        }
        serverRpc->silentIntervals++;

        // Advance the iterator now, so it won't get invalidated if we
        // delete the ServerRpc below.
        it++;

        // If a long time has elapsed with no communication whatsoever
        // from the client, then abort the RPC. Note: this code should
        // only be executed when we're waiting to transmit or receive
        // (never if we're waiting for the RPC to execute locally).
        // The most common reasons for getting here are:
        // (a) The client has crashed
        // (b) The client sent us data for an RPC after we processed it,
        //     returned the result, and deleted the RPC; as a result, we
        //     created a new RPC that the client no longer cares about.
        //     Such extraneous data can occur if we requested a
        //     retransmission but then the original data arrived, so we
        //     could process the RPC before the retransmitted data arrived.
        assert(serverRpc->sendingResponse || !serverRpc->requestComplete);
        if (serverRpc->silentIntervals >= timeoutIntervals) {
            RAMCLOUD_LOG(WARNING, "aborting %s RPC from client %s, "
                    "sequence %lu: timeout",
                    WireFormat::opcodeSymbol(&serverRpc->requestPayload),
                    serverRpc->clientAddress->toString().c_str(),
                    serverRpc->rpcId.sequence);
            timeTrace("aborting server RPC, clientId %u, sequence %u",
                    serverRpc->rpcId.clientId, serverRpc->rpcId.sequence);
            deleteServerRpc(serverRpc);
            continue;
        }

        if (serverRpc->silentIntervals < 2) {
            // Make sure the serverRpc has experienced at least one full
            // timerInterval before we start to deal with it.
            continue;
        }

        if (!serverRpc->requestComplete) {
            // See if we need to request retransmission for part of the request
            // message.
            ScheduledMessage* scheduledMessage =
                    serverRpc->scheduledMessage.get();
            uint32_t grantOffset =
                    scheduledMessage ? scheduledMessage->grantOffset : 0;
            if (scheduledMessage &&
                    (grantOffset == serverRpc->requestPayload.size())) {
                // The server has received every granted byte but hasn't got
                // around to grant more because there are higher priority
                // responses.
                serverRpc->silentIntervals = 0;
            } else {
                MessageAccumulator* accumulator = serverRpc->accumulator.get();
                int priority = scheduledMessage ?
                        scheduledMessage->grantPriority :
                        getUnschedTrafficPrio(accumulator->totalLength);
                accumulator->requestRetransmission(this,
                        serverRpc->clientAddress, serverRpc->rpcId, grantOffset,
                        priority, FROM_SERVER);
            }
        }
    }
}

/**
 * A message needs to be put in the right place of the (sorted) active message
 * list that reflects its scheduling precedence. This message can be either a
 * non-active (new or inactive) message that doesn't yet exist in the active
 * message list or an existing active message that needs to move forward in the
 * list.
 *
 * \param message
 *      A message that needs to be put at the right place in the active message
 *      list.
 */
void
HomaTransport::adjustSchedulingPrecedence(ScheduledMessage* message)
{
    assert(message->state != ScheduledMessage::FULLY_GRANTED);

    // The following loop iterates over the active message list to find the
    // right place to insert the given message.
    ScheduledMessage* insertHere = NULL;
    for (ScheduledMessage& m : activeMessages) {
        if (&m == message) {
            // This existing message is still in the right place:
            // all preceding messages are smaller.
            return;
        }

        if (message->compareTo(m) < 0) {
            insertHere = &m;
            break;
        }
    }

    // Relocate the message to its new place.
    if (message->state == ScheduledMessage::ACTIVE) {
        erase(activeMessages, *message);
    } else if (message->state == ScheduledMessage::INACTIVE) {
        erase(inactiveMessages, *message);
    }
    message->state = ScheduledMessage::ACTIVE;
    if (insertHere) {
        insertBefore(activeMessages, *message, *insertHere);
    } else {
        activeMessages.push_back(*message);
    }
}

/**
 * Replace an active message by a non-active (new or inactive) one because
 * either our scheduling policy (in tryToSchedule) says it's a better choice
 * or the the active message has been fully granted or cancelled. If the
 * active message has been fully granted, we will purge it from the scheduler;
 * otherwise, it will be moved to the inactive message list.
 *
 * \param oldMessage
 *      A currently active message that needs to be deactivated.
 * \param newMessage
 *      A non-active message that should be activated. If the value is NULL,
 *      it is the duty of this method to pick a replacement from the inactive
 *      message list (this can happen when oldMessage is purged or cancelled).
 */
void
HomaTransport::replaceActiveMessage(ScheduledMessage *oldMessage,
        ScheduledMessage *newMessage)
{
    // Remove oldMessage from the active message list; put it back to the
    // inactive message list if it has not been fully granted.
    bool purgeOK = (oldMessage->grantOffset >= oldMessage->totalLength);
    erase(activeMessages, *oldMessage);
    if (purgeOK) {
        oldMessage->state = ScheduledMessage::FULLY_GRANTED;
    } else {
        oldMessage->state = ScheduledMessage::INACTIVE;
        inactiveMessages.push_back(*oldMessage);
    }

    // Find an inactive message to activate if none has been designated.
    if (NULL == newMessage) {
        for (ScheduledMessage& message : inactiveMessages) {
            if (newMessage != NULL && newMessage->compareTo(message) <= 0) {
                continue;
            }

            // Make sure the candidate doesn't have the same sender as any of
            // the active messages.
            bool senderConflict = false;
            for (ScheduledMessage& active : activeMessages) {
                if (active.senderHash == message.senderHash) {
                    senderConflict = true;
                    break;
                }
            }
            if (!senderConflict) {
                newMessage = &message;
            }
        }
    }
    if (newMessage) {
        adjustSchedulingPrecedence(newMessage);
    }
}

/**
 * Attempts to schedule a message by placing it in the active message list.
 * This method is invoked when a new scheduled message arrives or an existing
 * inactive message tries to step up to the active message list. If the message
 * doesn't make it to the active message list, it will remain or get placed in
 * the inactive message list.
 *
 * \param message
 *      A message that cannot be sent unilaterally in unscheduled bytes.
 */
void
HomaTransport::tryToSchedule(ScheduledMessage* message)
{
    assert(message->state == ScheduledMessage::NEW ||
            message->state == ScheduledMessage::INACTIVE);
    bool newMessage = (message->state == ScheduledMessage::NEW);

    // The following loop handles the special case where some active message
    // comes from the same sender as the new message.
    for (ScheduledMessage &m : activeMessages) {
        if (m.senderHash != message->senderHash) {
            continue;
        }

        if (message->compareTo(m) < 0) {
            replaceActiveMessage(&m, message);
        } else if (newMessage) {
            message->state = ScheduledMessage::INACTIVE;
            inactiveMessages.push_back(*message);
        }
        return;
    }

    // From now on, we can assume that this message has a different sender
    // than the active messages.
    if (activeMessages.size() < maxGrantedMessages) {
        // We have not buffered enough messages. Note that this can only
        // happen if this message is new and that every inactive message
        // has the same sender as one of the active messages.
        assert(newMessage);
        adjustSchedulingPrecedence(message);
    } else if (message->compareTo(activeMessages.back()) < 0) {
        // This message should replace the "worst" active message.
        replaceActiveMessage(&activeMessages.back(), message);
    } else if (newMessage) {
        // Place the new message in the inactive message list.
        message->state = ScheduledMessage::INACTIVE;
        inactiveMessages.push_back(*message);
    } else {
        // This is an existing inactive message. Do nothing.
        assert(message->state == ScheduledMessage::INACTIVE);
    }
}

/**
 * When we receive a DATA packet, this method is invoked to see if the
 * scheduler needs to 1) update its active message list and 2) send out
 * a GRANT.
 *
 * \param scheduledMessage
 *      NULL means the data packet belongs to a unscheduled message;
 *      otherwise, it is the scheduled message that receives the data
 *      packet.
 */
void
HomaTransport::dataPacketArrive(ScheduledMessage* scheduledMessage)
{
    // If this DATA packet belongs to a scheduled message, see if we need to
    // adjust the scheduling precedence of this message (since the remaining
    // size of this message is decreased).
    if (scheduledMessage) {
        switch (scheduledMessage->state) {
            case ScheduledMessage::ACTIVE:
                adjustSchedulingPrecedence(scheduledMessage);
                break;
            case ScheduledMessage::INACTIVE:
                tryToSchedule(scheduledMessage);
                break;
            case ScheduledMessage::FULLY_GRANTED:
                // This can happen because a scheduled message will be purged
                // from the scheduler as soon as it is fully granted, but we
                // will continue to receive data from it for a while.
                break;
            default:
                LOG(ERROR, "unexpected message state %u",
                        scheduledMessage->state);
                return;
        }
    }

    // Find the first active message that could use a GRANT and compute the
    // priority to use for it.
    ScheduledMessage* messageToGrant = NULL;
    int p = std::min(int(activeMessages.size()) - 1, highestSchedPriority);
    for (ScheduledMessage& m : activeMessages) {
        if (m.grantOffset < m.accumulator->buffer->size() + roundTripBytes) {
            messageToGrant = &m;
            break;
        }
        p--;
    }
    if (messageToGrant == NULL) {
        return;
    }

    // If we have more active messages than # scheduled priorities, squash
    // the bottom few messages to the lowest priority level.
    int priority = std::max(p, 0);
    if (messageToGrant->totalLength - messageToGrant->grantOffset
            <= roundTripBytes) {
        // For the last 1 RTT remaining bytes of a scheduled message
        // with size (1+a)RTT, use the same priority as an unscheduled
        // message that has size min{1, a}*RTT.
        priority = getUnschedTrafficPrio(std::min(roundTripBytes,
                messageToGrant->totalLength - roundTripBytes));
    }

    messageToGrant->grantOffset += grantIncrement;
    messageToGrant->grantPriority = priority;
    if (messageToGrant->grantOffset >= messageToGrant->totalLength) {
        // A message has been fully granted. Purge it from the scheduler.
        messageToGrant->grantOffset = messageToGrant->totalLength;
        replaceActiveMessage(messageToGrant, NULL);
    }

    // Output a GRANT for the selected message.
    if (std::find(messagesToGrant.begin(), messagesToGrant.end(),
            messageToGrant) == messagesToGrant.end()) {
        messagesToGrant.push_back(messageToGrant);
    }
}

}  // namespace RAMCloud
