/* Copyright (c) 2013-2015 Stanford University
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

#ifndef RAMCLOUD_MOCKEXTERNALSTORAGE_H
#define RAMCLOUD_MOCKEXTERNALSTORAGE_H

#include <thread>
#include <mutex>
#include <string>
#include <queue>
#include "ExternalStorage.h"

namespace RAMCloud {

/**
 * This class provides a dummy implementation of the ExternalStorage interface.
 * It is used for unit tests, and also for RAMCloud clusters that wish to run
 * without any persistent storage of coordinator data (typically testing and
 * performance measurement). This class is thread-safe.
 */
class MockExternalStorage: public ExternalStorage {
  PUBLIC:
    explicit MockExternalStorage(bool enableLogging);
    virtual ~MockExternalStorage();
    virtual void becomeLeader(const char* name, const string& leaderInfo);
    virtual bool get(const char* name, Buffer* value);
    virtual void getChildren(const char* name, vector<Object>* children);
    virtual void remove(const char* name);
    virtual void set(Hint flavor, const char* name, const char* value,
            int valueLength = -1);

    /**
     * This method treats the most recent value from a "set" call as a
     * protocol buffer of a particular type and returns a human-readable
     * string representing its contents.
     */
    template<typename ProtoBufType>
    string
    getPbValue()
    {
        ProtoBufType value;
        if (!value.ParseFromString(setData)) {
            return "format error";
        }
        return value.ShortDebugString();
    }

  PRIVATE:
    /// Monitor-style lock: acquired by all externally visible methods.
    std::mutex mutex;
    typedef std::unique_lock<std::mutex> Lock;

    /// Copy of generateLog argument from constructor.
    bool generateLog;

    /// Copy of name argument from becomeLeader.
    string leaderObject;

    /// Copy of leaderInfo argument from last call to setLeaderInfo or
    /// becomeLeader.
    string leaderInfo;

    /// Accumulates information about methods that have been invoked on
    /// this object.
    std::string log;

    /// Queue of values to be returned by the get method. A unit test will
    /// typically push information here before running a test.
    std::queue<std::string> getResults;

    /// The following queues hold names and values to be returned by the
    /// getChildren method. A unit test will typically push information here
    /// before running a test. Note: if a value in getChildrenValues is an
    /// empty string, NULL will be returned in the ExternalStorage::Object.
    std::queue<std::string> getChildrenNames;
    std::queue<std::string> getChildrenValues;

    /// Holds the data from the last call to "set".
    std::string setData;

    void logAppend(Lock& lock, const std::string& record);

    DISALLOW_COPY_AND_ASSIGN(MockExternalStorage);
};

} // namespace RAMCloud

#endif // RAMCLOUD_MOCKEXTERNALSTORAGE_H

