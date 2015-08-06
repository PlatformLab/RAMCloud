/* Copyright (c) 2013-2015 Stanford University
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

#include "MockExternalStorage.h"
#include <google/protobuf/message.h>

namespace RAMCloud {

/**
 * Construct a MockExternalStorage object.
 *
 * \param generateLog
 *      True means that information should be logged about methods invoked
 *      on this class (typically used during unit tests).
 */
MockExternalStorage::MockExternalStorage(bool generateLog)
    : mutex()
    , generateLog(generateLog)
    , leaderObject()
    , leaderInfo()
    , log()
    , getResults()
    , getChildrenNames()
    , getChildrenValues()
    , setData()
{}

/**
 * Destructor for MockExternalStorage objects.
 */
MockExternalStorage::~MockExternalStorage()
{}


// See documentation for ExternalStorage::becomeLeader.
void
MockExternalStorage::becomeLeader(const char* name, const string& leaderInfo)
{
    Lock lock(mutex);
    if (generateLog) {
        logAppend(lock, format("becomeLeader(%s, %s)", name,
                leaderInfo.c_str()));
    }
}

// See documentation for ExternalStorage::get.
bool
MockExternalStorage::get(const char* name, Buffer* value)
{
    Lock lock(mutex);
    if (generateLog) {
        logAppend(lock, format("get(%s)", name));
    }
    value->reset();
    if (getResults.empty()) {
        return false;
    }
    value->appendCopy(getResults.front().c_str(),
            downCast<uint32_t>(getResults.front().length()));
    getResults.pop();
    return true;
}

// See documentation for ExternalStorage::getChildren.
void
MockExternalStorage::getChildren(const char* name, vector<Object>* children)
{
    Lock lock(mutex);
    if (generateLog) {
        logAppend(lock, format("getChildren(%s)", name));
    }
    children->clear();
    while (!getChildrenNames.empty()) {
        const char *value = getChildrenValues.front().c_str();
        if (*value == 0) {
            value = NULL;
        }
        children->emplace_back(getChildrenNames.front().c_str(),
                getChildrenValues.front().c_str(),
                downCast<int>(getChildrenValues.front().length()));
        getChildrenNames.pop();
        getChildrenValues.pop();
    }
}

// See documentation for ExternalStorage::remove.
void
MockExternalStorage::remove(const char* name)
{
    Lock lock(mutex);
    if (generateLog) {
        logAppend(lock, format("remove(%s)", name));
    }
}

// See documentation for ExternalStorage::update.
void
MockExternalStorage::set(Hint flavor, const char* name, const char* value,
        int valueLength)
{
    Lock lock(mutex);
    if (generateLog) {
        logAppend(lock, format("set(%s, %s)",
                (flavor == Hint::CREATE) ? "CREATE" : "UPDATE", name));
    }
    setData.assign(value, (valueLength < 0) ? strlen(value)
            : downCast<size_t>(valueLength));
}

/**
 * This method is invoked to add information to the internal log;
 * its main job is to insert separators between the information from
 * different calls to the method.
 *
  \param lock
 *      Ensures that caller has acquired mutex; not actually used here.
 * \param record
 *      A chunk of information to be added to the internal log; typically
 *      describes a method call and its arguments.
 */
void
MockExternalStorage::logAppend(Lock& lock, const std::string& record)
{
    if (log.length() != 0) {
        log.append("; ");
    }
    log.append(record);
}

} // namespace RAMCloud
