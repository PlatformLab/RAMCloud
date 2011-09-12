/* Copyright (c) 2010-2011 Stanford University
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

#include "TestUtil.h"
#include "Window.h"

namespace RAMCloud {

class WindowTest : public ::testing::Test {
  public:
    Window<int, 8> window;
    WindowTest() : window()
    {
        for (int i = 0; i < 8; i++)
            window.array[i] = i;
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(WindowTest);
};

TEST_F(WindowTest, constructor) {
    size_t approxSize = sizeof(int) * 8; // NOLINT
    EXPECT_TRUE(approxSize <= sizeof(window) &&
                    sizeof(window) <= approxSize + 128);

    Window<int, 8> r2;
    EXPECT_EQ(0U, r2.offset);
    EXPECT_EQ(0, r2.translation);
    EXPECT_EQ(0, r2.array[0]);
}

TEST_F(WindowTest, reset) {
    window.advance(20);
    window.reset();
    for (int i = 0; i < 8; i++)
        EXPECT_EQ(0, window.array[i]);
    EXPECT_EQ(0U, window.offset);
    EXPECT_EQ(0, window.translation);
}

TEST_F(WindowTest, index) {
    EXPECT_EQ(0, window[0]);
    EXPECT_EQ(7, window[7]);
    window.advance(13);
    for (int i = 0; i < 8; i++)
        window.array[i] = i;
    EXPECT_EQ(5, window[13]);
    EXPECT_EQ(7, window[15]);
    EXPECT_EQ(0, window[16]);
    EXPECT_EQ(4, window[20]);
}

TEST_F(WindowTest, advance) {
    // No advance at all.
    window.advance(0);
    EXPECT_EQ(0, window[0]);
    EXPECT_EQ(7, window[7]);

    // Advance a bit (not enough for a wrap-around)
    window.advance(4);
    EXPECT_EQ(0, window.translation);
    EXPECT_EQ(4, window[4]);
    EXPECT_EQ(5, window[5]);
    EXPECT_EQ(0, window[10]);
    EXPECT_EQ(0, window[11]);
    window[11] = 99;
    EXPECT_EQ(99, window.array[3]);

    // Advance enough to wrap around and change translation
    window.advance(13);
    EXPECT_EQ(-16, window.translation);
    for (int i = 17; i < 25; i++)
        EXPECT_EQ(0, window[i]);
    for (int i = 0; i < 8; i++)
        window.array[i] = i;
    EXPECT_EQ(1, window[17]);
    EXPECT_EQ(7, window[23]);
    EXPECT_EQ(0, window[24]);
}

TEST_F(WindowTest, getLength) {
    EXPECT_EQ(8U, window.getLength());
}

TEST_F(WindowTest, getOffset) {
    EXPECT_EQ(0U, window.getOffset());
    window.advance(1);
    EXPECT_EQ(1U, window.getOffset());
    window.advance(5);
    EXPECT_EQ(6U, window.getOffset());
}

}  // namespace RAMCloud
