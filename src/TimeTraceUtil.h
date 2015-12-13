/* Copyright (c) 2015 Stanford University
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

#ifndef RAMCLOUD_TIMETRACEUTIL_H
#define RAMCLOUD_TIMETRACEUTIL_H

#include "WireFormat.h"

namespace RAMCloud {

/**
 * This class manages a collection of strings for use in time traces,
 * which make it easier to produce more detailed messages in time traces.
 * This class is needed because time trace messages must be static; this
 * class generates static messages that combine several different pieces
 * of information, such as the type of request being serviced and its
 * current status.
 *
 * There's no particular structure to this information, so feel free to
 * add more strings and methods as needed.
 */
class TimeTraceUtil {
  PUBLIC:
    // Request statuses supported by statusMsg.
    enum RequestStatus {
        HANDOFF = 0,
        WORKER_START,
        WORKER_DONE,
        POST_PROCESSING,
        REPLY_SENT,
        WORKER_SLEEP,
        LAST_STATUS           // Marks the end of the enum
    };

    static const char* queueLengthMsg(WireFormat::ServiceType service,
            size_t length);
    static const char* statusMsg(int thread, WireFormat::Opcode,
            RequestStatus status);

    static const uint32_t MAX_MESSAGE_SIZE = 50;
    static const int MAX_THREAD_ID = 20;
    static const uint32_t MAX_QUEUE_LENGTH = 100;

    PRIVATE:
    static void initQueueLengthMessages();
    static void initStatusMessages();

    // This class is not intended to be instantiated.
    TimeTraceUtil() {}

    // Request types supported by statusMsg.
    enum RequestOp {
        WRITE = 0,
        READ,
        BACKUP_WRITE,
        OTHER,
        LAST_OP               // Marks the end of the enum
    };

    static char* statusMessages[MAX_THREAD_ID+2][RequestOp::LAST_OP]
            [RequestStatus::LAST_STATUS];
    static char* queueLengthMessages
            [WireFormat::ServiceType::INVALID_SERVICE][MAX_QUEUE_LENGTH + 2];
    static RequestOp opcodeMap[WireFormat::ILLEGAL_RPC_TYPE];
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_TIMETRACEUTIL_H
