/* Copyright (c) 2010-2014 Stanford University
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
#include "DispatchExec.h"
#include "Cycles.h"


namespace RAMCloud {

class DispatchExecTest : public ::testing::Test {
    public:
        Dispatch dispatch;
        DispatchExec dispatchExec;
        TestLog::Enable logEnabler;

        DispatchExecTest() :
            dispatch(false),
            dispatchExec(&dispatch),
            logEnabler() { }

};

class PrintOne : public DispatchExec::Lambda {
  public:
    PrintOne() { }
    void invoke() {
        TEST_LOG("1");
    }

};

class PrintArg : public DispatchExec::Lambda {
  public:
    explicit PrintArg(const char* arg) : myArg(arg) {
    }
    void invoke() {
        TEST_LOG("%s", myArg);
    }
  private:
    const char* myArg;
    DISALLOW_COPY_AND_ASSIGN(PrintArg);
};

TEST_F(DispatchExecTest, basics) {
    EXPECT_EQ(0, dispatchExec.poll());
    EXPECT_EQ(dispatchExec.addIndex, 0);
    EXPECT_EQ(dispatchExec.removeIndex, 0);
    dispatchExec.addRequest<PrintOne>();
    EXPECT_EQ(dispatchExec.addIndex, 1);
    EXPECT_EQ(1, dispatchExec.poll());
    EXPECT_EQ(dispatchExec.removeIndex, 1);
    EXPECT_EQ(TestLog::get(), "invoke: 1");
}

TEST_F(DispatchExecTest, passingArgs) {
    dispatchExec.addRequest<PrintArg>("test_arg");
    dispatchExec.poll();
    EXPECT_EQ(TestLog::get(), "invoke: test_arg");
}

TEST_F(DispatchExecTest, wrapAround) {
    dispatchExec.addIndex = DispatchExec::NUM_WORKER_REQUESTS - 1;
    dispatchExec.removeIndex = DispatchExec::NUM_WORKER_REQUESTS - 1;
    dispatchExec.addRequest<PrintOne>();
    EXPECT_EQ(dispatchExec.addIndex, 0);
    dispatchExec.poll();
    EXPECT_EQ(dispatchExec.removeIndex, 0);
}
}
