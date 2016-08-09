/* Copyright (c) 2012-2014 Stanford University
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

#include "TestUtil.h"
#include "MockTransport.h"
#include "Transport.h"

namespace RAMCloud {

class TransportTest : public ::testing::Test {
  public:
    Context context;
    MockTransport transport;

    TransportTest()
        : context()
        , transport(&context)
    {}
    DISALLOW_COPY_AND_ASSIGN(TransportTest);
};

// The following test makes sure that reference counts are thread-safe;
// it uses two threads incrementing and decrementing the reference count
// simultaneously.
static void contentionThread(Transport::SessionRef* ref, bool* done)
{
    while (!*done) {
        for (int i = 0; i < 100; i++) {
            Transport::SessionRef copy(*ref);
            copy = NULL;
        }
    }
}

TEST_F(TransportTest, sessionRef_contention) {
    Transport::SessionRef wrappedSession = new Transport::Session("");
    Transport::SessionRef copy = wrappedSession;
    bool done = false;
    std::thread child(contentionThread, &wrappedSession, &done);
    for (int i = 0; i < 100000; i++) {
        Transport::SessionRef copy2(wrappedSession);
        copy2 = NULL;
    }
    done = true;
    child.join();
    EXPECT_EQ(2, wrappedSession->refCount.load());
}


}  // namespace RAMCloud
