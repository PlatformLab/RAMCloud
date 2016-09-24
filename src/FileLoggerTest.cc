/* Copyright (c) 2014-2016 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.xx
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "FileLogger.h"
#include "Logger.h"
#include "TimeTrace.h"

namespace RAMCloud {
class FileLoggerTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    FileLogger fileLogger;
    FILE* f;

    FileLoggerTest()
        : logEnabler()
        , fileLogger(NOTICE, "testLogger: ")
        , f(fileLogger.getFile())
    {
    }

    ~FileLoggerTest()
    {
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(FileLoggerTest);
};

TEST_F(FileLoggerTest, sanityCheck) {
    fprintf(f, "Sample log message\n");
    fprintf(f, "Message 2\n");
    fflush(f);
    EXPECT_EQ("write: testLogger: Sample log message | "
            "write: testLogger: Message 2", TestLog::get());
}

TEST_F(FileLoggerTest, destructor) {
    {
        FileLogger logger2(NOTICE, "alt: ");
        FileLogger::write(&logger2, "abcd", 4);
        EXPECT_EQ("", TestLog::get());
    }
    EXPECT_EQ("~FileLogger: alt: abcd", TestLog::get());
}

TEST_F(FileLoggerTest, write_multipleLines) {
    const char* msg = "line 1\nline 2\nline 3\n";
    FileLogger::write(&fileLogger, msg, strlen(msg));
    EXPECT_EQ("write: testLogger: line 1 | write: testLogger: line 2 | "
            "write: testLogger: line 3", TestLog::get());
}
TEST_F(FileLoggerTest, write_partialLines) {
    FileLogger::write(&fileLogger, "line 1\nabcd", 11);
    EXPECT_EQ("write: testLogger: line 1", TestLog::get());
    TestLog::reset();

    FileLogger::write(&fileLogger, " xyzzy", 6);
    EXPECT_EQ("", TestLog::get());

    FileLogger::write(&fileLogger, " 012345\nLine 4\nLine 5", 21);
    EXPECT_EQ("write: testLogger: abcd xyzzy 012345 | "
            "write: testLogger: Line 4", TestLog::get());
    EXPECT_EQ("Line 5", fileLogger.partialLine);
}

}  // namespace RAMCloud
