/* Copyright (c) 2010 Stanford University
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

/**
 * \file
 * This file defines RAMCloud features available from C rather than C++.
 */

#ifndef RAMCLOUD_CRAMCLOUD_H
#define RAMCLOUD_CRAMCLOUD_H

#include "Common.h"
#include "RejectRules.h"
#include "Status.h"

#ifdef __cplusplus
extern "C" {
#endif

RAMCloud::Status    rc_connect(const char* serverLocator,
                            struct rc_client** newClient);
RAMCloud::Status    rc_connectWithClient(
                            struct RAMCloud::RamCloud* existingClient,
                            struct rc_client** newClient);
void                rc_disconnect(struct rc_client* client);

RAMCloud::Status    rc_createTable(struct rc_client* client, const char* name);
RAMCloud::Status    rc_dropTable(struct rc_client* client, const char* name);
RAMCloud::Status    rc_getStatus(struct rc_client* client);
RAMCloud::Status    rc_openTable(struct rc_client* client, const char* name,
                            uint32_t* tableId);
RAMCloud::Status    rc_ping(struct rc_client* client,
                            const char* serviceLocator,
                            uint64_t nonce, uint64_t timeoutNanoseconds,
                            uint64_t* result);

RAMCloud::Status    rc_read(struct rc_client* client, uint32_t tableId,
                            const char* key, uint16_t keyLength,
                            const struct RAMCloud::RejectRules* rejectRules,
                            uint64_t* version, void* buf, uint32_t maxLength,
                            uint32_t* actualLength);
RAMCloud::Status    rc_remove(struct rc_client* client, uint32_t tableId,
                              const char* key, uint16_t keyLength,
                              const struct RAMCloud::RejectRules* rejectRules,
                              uint64_t* version);
RAMCloud::Status    rc_write(struct rc_client* client, uint32_t tableId,
                             const char* key, uint16_t keyLength,
                             const void* buf, uint32_t length,
                             const struct RAMCloud::RejectRules* rejectRules,
                             uint64_t* version);


#ifdef __cplusplus
}
#endif

#endif // RAMCLOUD_CRAMCLOUD_H
