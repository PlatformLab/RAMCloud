/* Copyright (c) 2012 Stanford University
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

#include "ClientException.h"
#include "FailSession.h"

namespace RAMCloud {

/**
 * Constructor for FailSession.
 */
FailSession::FailSession()
    : Session("fail:")
{}

/**
 * Destructor for FailSession.
 */
FailSession::~FailSession()
{}

// See Transport::Session for documentation.
void FailSession::abort()
{
    // No need to do anything here: this session is already as dead as
    // a doornail.
}

// See Transport::Session for documentation.
void FailSession::cancelRequest(Transport::RpcNotifier* notifier)
{
    // No need to do anything here, since we don't do anything with
    // requests in the first place.
}

/**
 * Return a pointer to a single shared instance of FailSession.  We
 * use a single instance everywhere a FailSession is needed; this is
 * safe, because FailSession is thread-safe, and it's convenient for
 * testing because it is easy to see if a method returns the FailSession.
 */
FailSession*
FailSession::get()
{
    static FailSession sharedFailSession;
    return &sharedFailSession;
}

// See Transport::Session for documentation.
void FailSession::release()
{
    // Nothing to do here: we never release the shared instance;
}

// See Transport::Session for documentation.
void FailSession::sendRequest(Buffer* request, Buffer* response,
                Transport::RpcNotifier* notifier)
{
    // Immediately fail the request, then ignore it.
    notifier->failed();
}

} // namespace RAMCloud
