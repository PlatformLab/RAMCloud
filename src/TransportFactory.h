/* Copyright (c) 2010-2012 Stanford University
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

#ifndef RAMCLOUD_TRANSPORTFACTORY_H
#define RAMCLOUD_TRANSPORTFACTORY_H

#include "Common.h"
#include "Transport.h"

namespace RAMCloud {

/**
 * A factory for #Transport instances.
 * Used only in #TransportManager for now.
 */
class TransportFactory {
  public:
    /**
     * Constructor.
     * The arguments are protocols that transports of this type support.
     * They should be ordered from most specific to most general.
     */
    TransportFactory(const char* arg1 = NULL,
                     const char* arg2 = NULL,
                     const char* arg3 = NULL,
                     const char* arg4 = NULL,
                     const char* arg5 = NULL,
                     const char* arg6 = NULL,
                     const char* arg7 = NULL,
                     const char* arg8 = NULL,
                     const char* arg9 = NULL,
                     const char* arg10 = NULL,
                     const char* arg11 = NULL,
                     const char* arg12 = NULL,
                     const char* arg13 = NULL,
                     const char* arg14 = NULL,
                     const char* arg15 = NULL,
                     const char* arg16 = NULL) : protocols() {
        if (arg1 != NULL) protocols.push_back(arg1);
        if (arg2 != NULL) protocols.push_back(arg2);
        if (arg3 != NULL) protocols.push_back(arg3);
        if (arg4 != NULL) protocols.push_back(arg4);
        if (arg5 != NULL) protocols.push_back(arg5);
        if (arg6 != NULL) protocols.push_back(arg6);
        if (arg7 != NULL) protocols.push_back(arg7);
        if (arg8 != NULL) protocols.push_back(arg8);
        if (arg9 != NULL) protocols.push_back(arg9);
        if (arg10 != NULL) protocols.push_back(arg10);
        if (arg11 != NULL) protocols.push_back(arg11);
        if (arg12 != NULL) protocols.push_back(arg12);
        if (arg13 != NULL) protocols.push_back(arg13);
        if (arg14 != NULL) protocols.push_back(arg14);
        if (arg15 != NULL) protocols.push_back(arg15);
        if (arg16 != NULL) protocols.push_back(arg16);
    }
    virtual ~TransportFactory() {}

    /**
     * Create a concrete #Transport instance.
     * The Dispatch lock must be held by the caller for the duration of this
     * function.
     * \param context
     *      Overall information about the RAMCloud server or client.
     * \param localServiceLocator
     *      The local address on which the transport should receive RPCs.
     *      May be NULL, indicating the Transport should only behave as a
     *      client.
     */
    virtual Transport*
    createTransport(Context* context,
            const ServiceLocator* localServiceLocator = NULL) = 0;

    /**
     * Return the list of supported protocols as provided to the constructor.
     */
    const std::vector<const char*>&
    getProtocols() const { return protocols; }

    /**
     * Return whether \a protocol is one of the protocols provided to the
     * constructor.
     */
    bool supports(const char* protocol) const {
        foreach (const char* supportedProtocol, protocols) {
            if (strcmp(supportedProtocol, protocol) == 0)
                return true;
        }
        return false;
    }

  private:
    std::vector<const char*> protocols;
    DISALLOW_COPY_AND_ASSIGN(TransportFactory);
};

} // end RAMCloud

#endif  // RAMCLOUD_TRANSPORTFACTORY_H
