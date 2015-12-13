/* Copyright (c) 2013-2015 Stanford University
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

namespace RAMCloud {
class MockExternalStorageTest : public ::testing::Test {
  public:
    MockExternalStorage storage;

    MockExternalStorageTest()
        : storage(true)
    {}

    ~MockExternalStorageTest()
    {}

    // Pretty-print the results from a getChildren call (collect all names
    // and values into a single string).
    string toString(vector<ExternalStorage::Object>* children)
    {
        string result;
        for (size_t i = 0; i < children->size(); i++) {
            if (result.length() != 0) {
                result += ", ";
            }
            ExternalStorage::Object* o = &children->at(i);
            result.append(o->name);
            result.append(": ");
            if (o->value == NULL) {
                result.append("-");
            } else {
                result.append(o->value, static_cast<size_t>(o->length));
            }
        }
        return result;
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(MockExternalStorageTest);
};

TEST_F(MockExternalStorageTest, becomeLeader) {
    storage.becomeLeader("leaderName", "abcde");
    EXPECT_EQ("becomeLeader(leaderName, abcde)", storage.log);
}

TEST_F(MockExternalStorageTest, get) {
    storage.getResults.emplace("value1");
    storage.getResults.emplace("value2");
    Buffer result;
    EXPECT_TRUE(storage.get("/node1", &result));
    EXPECT_EQ("value1", TestUtil::toString(&result));
    EXPECT_EQ("get(/node1)", storage.log);
    EXPECT_TRUE(storage.get("/node2", &result));
    EXPECT_EQ("value2", TestUtil::toString(&result));
    EXPECT_EQ("get(/node1); get(/node2)", storage.log);
    EXPECT_FALSE(storage.get("/node3", &result));
    EXPECT_EQ("", TestUtil::toString(&result));
    EXPECT_EQ("get(/node1); get(/node2); get(/node3)", storage.log);
}

TEST_F(MockExternalStorageTest, getChildren) {
    storage.getChildrenNames.emplace("name1");
    storage.getChildrenValues.emplace("v1");
    storage.getChildrenNames.emplace("name2");
    storage.getChildrenValues.emplace("v2");
    vector<ExternalStorage::Object> children;
    storage.getChildren("/a/b", &children);
    EXPECT_EQ("name1: v1, name2: v2", toString(&children));
    EXPECT_EQ("getChildren(/a/b)", storage.log);
    storage.getChildren("/x/y/z", &children);
    EXPECT_EQ("", toString(&children));
    EXPECT_EQ("getChildren(/a/b); getChildren(/x/y/z)", storage.log);
}

TEST_F(MockExternalStorageTest, remove) {
    storage.remove("/a/b/c");
    EXPECT_EQ("remove(/a/b/c)", storage.log);
}

TEST_F(MockExternalStorageTest, set) {
    storage.set(ExternalStorage::CREATE, "/a/b/c", "xyzzy", 5);
    storage.set(ExternalStorage::UPDATE, "/x1", "99");
    EXPECT_EQ("set(CREATE, /a/b/c); set(UPDATE, /x1)", storage.log);
    EXPECT_EQ("99", storage.setData);
}

TEST_F(MockExternalStorageTest, logAppend) {
    MockExternalStorage::Lock lock(storage.mutex);
    storage.logAppend(lock, "x y z");
    storage.logAppend(lock, "a b");
    storage.logAppend(lock, "12345");
    EXPECT_EQ("x y z; a b; 12345", storage.log);
}

}  // namespace RAMCloud
