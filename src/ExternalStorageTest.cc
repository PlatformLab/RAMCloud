/* Copyright (c) 2013 Stanford University
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
#include "MockExternalStorage.h"
#include "TableManager.pb.h"

namespace RAMCloud {
class ExternalStorageTest : public ::testing::Test {
  public:
    MockExternalStorage storage;

    ExternalStorageTest()
        : storage(true)
    {}

    ~ExternalStorageTest()
    {}

  private:
    DISALLOW_COPY_AND_ASSIGN(ExternalStorageTest);
};

TEST_F(ExternalStorageTest, getAndSetWorkspace) {
    // Check initial value.
    EXPECT_STREQ("/", storage.getWorkspace());
    EXPECT_EQ("/", storage.fullName);

    // Try changing the value.
    storage.setWorkspace("/a/b/c/");
    EXPECT_STREQ("/a/b/c/", storage.getWorkspace());
    EXPECT_EQ("/a/b/c/", storage.fullName);
}

TEST_F(ExternalStorageTest, getFullName) {
    EXPECT_STREQ("/abc", storage.getFullName("abc"));
    EXPECT_STREQ("/first/second/third",
            storage.getFullName("/first/second/third"));

    storage.setWorkspace("/a/b/");
    EXPECT_STREQ("/a/b/abc", storage.getFullName("abc"));
    EXPECT_STREQ("/first/second/third",
            storage.getFullName("/first/second/third"));
}

TEST_F(ExternalStorageTest, get_templated_basics) {
    ProtoBuf::TableManager info;
    info.set_next_table_id(123);
    string str;
    info.SerializeToString(&str);
    storage.getResults.push(str);

    ProtoBuf::TableManager info2;
    EXPECT_TRUE(storage.getProtoBuf("/node1", &info2));
    EXPECT_EQ(123u, info2.next_table_id());
}
TEST_F(ExternalStorageTest, get_templated_noSuchObject) {
    ProtoBuf::TableManager info2;
    EXPECT_FALSE(storage.getProtoBuf("/node1", &info2));
}
TEST_F(ExternalStorageTest, get_templated_formatError) {
    storage.getResults.push("abc");

    ProtoBuf::TableManager info2;
    string message = "no exception";
    try {
        storage.getProtoBuf("/node1", &info2);
    } catch (ExternalStorage::FormatError& e) {
        message = e.message;
    }
    EXPECT_EQ("couldn't parse '/node1' object in external storage as "
            "RAMCloud.ProtoBuf.TableManager", message);
}

TEST_F(ExternalStorageTest, open_unknown) {
    EXPECT_TRUE(ExternalStorage::open("bogus:", NULL) == NULL);
}
// Note: no tests for successful opens here; those tests are in the
// test files for specific subclasses, such as ZooStorage.

}  // namespace RAMCloud
