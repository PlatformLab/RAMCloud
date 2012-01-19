/* Copyright (c) 2011 Stanford University
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
#include "MockCluster.h"
#include "ShortMacros.h"
#include "StringKeyAdapter.h"

namespace RAMCloud {

class StringKeyAdapterTest : public ::testing::Test {
  public:
    MockCluster cluster;
    Tub<RamCloud> client;
    Tub<StringKeyAdapter> sk;
    uint32_t table;

    StringKeyAdapterTest()
        : cluster()
        , client()
        , sk()
        , table()
    {

        ServerConfig config = ServerConfig::forTesting();
        config.localLocator = "mock:host=master";
        cluster.addServer(config);

        client.construct(Context::get(), "mock:host=coordinator");
        sk.construct(*client);

        client->createTable("StringKeyAdapterTest");
        table = client->openTable("StringKeyAdapterTest");
    }

    DISALLOW_COPY_AND_ASSIGN(StringKeyAdapterTest);
};

static const char* key = "ramcloud/users/stutsman/friends";
static const StringKeyAdapter::KeyLength keyLength =
    downCast<StringKeyAdapter::KeyLength>(strlen(key));
static const char* value = "ongaro, rumble";
static const uint32_t length = downCast<uint32_t>(strlen(value));

TEST_F(StringKeyAdapterTest, read)
{
    sk->write(table, key, keyLength, value, length);
    Buffer storedValue;
    sk->read(table, key, keyLength, storedValue);
    const void* valueRange = storedValue.getRange(0, length);
    // Cannot use STREQ, stored strings are not nul-terminated.
    EXPECT_TRUE(!memcmp(value, valueRange, length));
    EXPECT_EQ(length, storedValue.getTotalLength());
}

TEST_F(StringKeyAdapterTest, read_keyLengthMismatch)
{
    const auto hashedKey = StringKeyAdapter::hash(key, keyLength);
    // Put a value in the slot where we expect to find "key" that
    // has a 0-length key field and no other data.
    StringKeyAdapter::KeyLength zero = 0;
    client->write(table, hashedKey, &zero, sizeof(StringKeyAdapter::KeyLength));
    Buffer storedValue;
    EXPECT_THROW(sk->read(table, key, keyLength, storedValue),
                 ObjectDoesntExistException);
}

TEST_F(StringKeyAdapterTest, read_keyMismatch)
{
    const auto hashedKey = StringKeyAdapter::hash(key, keyLength);
    // Put a value in the slot where we expect to find "key" that
    // has the corrent length key field but with the wrong byte
    // string for the key.
    char messedObject[sizeof(StringKeyAdapter::KeyLength) + keyLength];
    memcpy(messedObject, &keyLength, sizeof(StringKeyAdapter::KeyLength));
    memcpy(messedObject + sizeof(StringKeyAdapter::KeyLength), key, keyLength);
    messedObject[sizeof(StringKeyAdapter::KeyLength)] = 'h';
    const uint32_t writeLength =
        downCast<uint32_t>(sizeof(StringKeyAdapter::KeyLength) + keyLength);
    client->write(table, hashedKey, messedObject, writeLength);
    Buffer storedValue;
    EXPECT_THROW(sk->read(table, key, keyLength, storedValue),
                 ObjectDoesntExistException);
}

TEST_F(StringKeyAdapterTest, remove)
{
    sk->write(table, key, keyLength, value, length);
    Buffer storedValue;
    const auto hashedKey = StringKeyAdapter::hash(key, keyLength);
    client->read(table, hashedKey, &storedValue);
    sk->remove(table, key, keyLength);
    EXPECT_THROW(client->read(table, hashedKey, &storedValue),
                 ObjectDoesntExistException);
}

TEST_F(StringKeyAdapterTest, write)
{
    sk->write(table, key, keyLength, value, length);

    Buffer storedValue;
    const auto hashedKey = StringKeyAdapter::hash(key, keyLength);
    client->read(table, hashedKey, &storedValue);

    EXPECT_EQ(keyLength,
              *storedValue.getOffset<StringKeyAdapter::KeyLength>(0));
    const void* keyRange =
        storedValue.getRange(sizeof(StringKeyAdapter::KeyLength), keyLength);
    // Cannot use STREQ, stored strings are not nul-terminated.
    EXPECT_TRUE(!memcmp(key, keyRange, keyLength));

    const uint32_t start =
        downCast<uint32_t>(sizeof(StringKeyAdapter::KeyLength) + keyLength);
    const void* valueRange =
        storedValue.getRange(start, length);
    // Cannot use STREQ, stored strings are not nul-terminated.
    EXPECT_TRUE(!memcmp(value, valueRange, length));
}

} // namespace RAMCloud
