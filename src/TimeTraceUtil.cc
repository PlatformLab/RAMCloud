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

#include <stdio.h>

#include "TimeTraceUtil.h"

namespace RAMCloud {

// The arrays below hold generated messages, each combining multiple
// pieces of information.
char* TimeTraceUtil::statusMessages[MAX_THREAD_ID+2][RequestOp::LAST_OP]
        [RequestStatus::LAST_STATUS];
char* TimeTraceUtil::queueLengthMessages
        [WireFormat::ServiceType::INVALID_SERVICE][MAX_QUEUE_LENGTH + 2];

// The array below maps from WireFormat::Opcode values to RequestOp
// values.
TimeTraceUtil::RequestOp TimeTraceUtil::opcodeMap
        [WireFormat::Opcode::ILLEGAL_RPC_TYPE];

/**
 * Returns a static message that includes a thread id, an opcode
 * identifying a particular kind of request, and the current status
 * of that request.
 * 
 * \param thread
 *      Integer thread identifier; should be a small integer.
 * \param op
 *      RPC request being processed.
 * \param status
 *      Current state of request, or something that just happened
 *      to this request.
 * 
 */
const char*
TimeTraceUtil::statusMsg(int thread, WireFormat::Opcode op,
        RequestStatus status)
{
    if (statusMessages[0][0][0] == NULL) {
        initStatusMessages();
    }
    if (thread > MAX_THREAD_ID) {
        thread = MAX_THREAD_ID + 1;
    }
    return statusMessages[thread][opcodeMap[op]][status];
}

/*
 * Returns a static string indicating that the queue of requests for the
 * backup service has a particular length.
 *
 * \param length
 *      Number of requests currently waiting in the queue for the
 *      backup service.
 */
const char*
TimeTraceUtil::queueLengthMsg(WireFormat::ServiceType service, size_t length)
{
    if (queueLengthMessages[0][0] == NULL) {
        initQueueLengthMessages();
    }
    if (length > MAX_QUEUE_LENGTH) {
        length = MAX_QUEUE_LENGTH+1;
    }
    return queueLengthMessages[service][length];
}

/*
 * This method initializes all of the messages returned by statusMsg.
 */
void
TimeTraceUtil::initStatusMessages()
{
    // Initialize the opcode map.
    for (uint32_t op = 0; op < WireFormat::Opcode::ILLEGAL_RPC_TYPE; op++) {
        switch (op) {
            case WireFormat::Opcode::READ:
                opcodeMap[op] = RequestOp::READ;
                break;
            case WireFormat::Opcode::WRITE:
                opcodeMap[op] = RequestOp::WRITE;
                break;
            case WireFormat::Opcode::BACKUP_WRITE:
                opcodeMap[op] = RequestOp::BACKUP_WRITE;
                break;
            default:
                opcodeMap[op] = RequestOp::OTHER;
        }
    }

    // Initialize all of the messages.
    for (int thread = 0; thread <= MAX_THREAD_ID+1; thread++) {
        for (uint32_t op = 0; op < RequestOp::LAST_OP; op++) {
            const char* opString;
            switch (op) {
                case RequestOp::WRITE:
                    opString = "Write";
                    break;
                case RequestOp::READ:
                    opString = "Read";
                    break;
                case RequestOp::BACKUP_WRITE:
                    opString = "BackupWrite";
                    break;
                default:
                    opString = "UnknownRPC";
            }
            for (uint32_t status = 0; status < RequestStatus::LAST_STATUS;
                    status++) {
                const char* statusString;
                switch (status) {
                    case RequestStatus::HANDOFF:
                        statusString = "Handoff to worker for";
                        break;
                    case RequestStatus::WORKER_START:
                        statusString = "Worker starting";
                        break;
                    case RequestStatus::WORKER_DONE:
                        statusString = "Worker finished";
                        break;
                    case RequestStatus::POST_PROCESSING:
                        statusString = "Worker postprocessing";
                        break;
                    case RequestStatus::REPLY_SENT:
                        statusString = "Reply sent for";
                        break;
                    case RequestStatus::WORKER_SLEEP:
                        statusString = "Worker sleeping after";
                        break;
                }
                statusMessages[thread][op][status] = new char[MAX_MESSAGE_SIZE];
                if (thread <= MAX_THREAD_ID) {
                    snprintf(statusMessages[thread][op][status],
                            MAX_MESSAGE_SIZE, "%s %s in thread %d",
                            statusString, opString, thread);
                } else {
                    snprintf(statusMessages[thread][op][status],
                            MAX_MESSAGE_SIZE, "%s %s in unknown thread",
                            statusString, opString);
                }
            }
        }
    }
}

/**
 * This method initializes the values of all of the messages returned
 * by queueLengthMsg.
 */
void
TimeTraceUtil::initQueueLengthMessages()
{
    for (uint32_t type = 0; type < WireFormat::INVALID_SERVICE; type++) {
        const char *typeString;
        switch (type) {
            case WireFormat::ServiceType::MASTER_SERVICE:
                typeString = "Master";
                break;
            case WireFormat::ServiceType::BACKUP_SERVICE:
                typeString = "Backup";
                break;
            case WireFormat::ServiceType::COORDINATOR_SERVICE:
                typeString = "Coordinator";
                break;
            case WireFormat::ServiceType::PING_SERVICE:
                typeString = "Ping";
                break;
            case WireFormat::ServiceType::MEMBERSHIP_SERVICE:
                typeString = "Membership";
                break;
            default:
                typeString = "Unknown";
        }

        for (uint32_t i = 0; i <= MAX_QUEUE_LENGTH; i++) {
            queueLengthMessages[type][i] = new char[MAX_MESSAGE_SIZE];
            snprintf(queueLengthMessages[type][i],
                    MAX_MESSAGE_SIZE, "%s queue length is %d",
                    typeString, i);
        }
        queueLengthMessages[type][MAX_QUEUE_LENGTH+1] =
                new char[MAX_MESSAGE_SIZE];
        snprintf(queueLengthMessages[type][MAX_QUEUE_LENGTH+1],
                MAX_MESSAGE_SIZE, "%s queue length is > %d",
                typeString, MAX_QUEUE_LENGTH);
    }
}

}  // namespace RAMCloud
