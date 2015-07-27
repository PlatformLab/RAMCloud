/* Copyright (c) 2013 Stanford University
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

#include "Common.h"
#include "ExternalStorage.h"
#include "LogCabinStorage.h"
#include "ZooStorage.h"

namespace RAMCloud {

ExternalStorage* ExternalStorage::storageOverride = NULL;

/**
 * Constructor for ExternalStorage objects.
 */
ExternalStorage::ExternalStorage()
    : workspace("/")
    , fullName(workspace)
{}

bool
ExternalStorage::getLeaderInfo(const char* name, Buffer* value)
{
    return get(name, value);
}

/**
 * Read an object from external storage, and parse it as a protocol
 * buffer of type ProtoBufType.
 *
 * \param name
 *      Name of the desired object; NULL-terminated hierarchical path
 *      containing one or more path elements separated by slashes,
 *      such as "foo" or "/foo/bar". Relative names (no leading slash)
 *      are concatenated to the current workspace.
 * \param value
 *      A protocol buffer into which the object value is parsed.
 *
 * \return
 *      If the specified object exists, then true is returned. If there
 *      is no such object, then false is returned and value is empty.
 *
 * \throws LostLeadershipException
 * \throws FormatError
 */
bool
ExternalStorage::getProtoBuf(const char* name,
                             google::protobuf::Message* value)
{
    Buffer externalData;
    if (!get(name, &externalData))
        return false;
    uint32_t length = externalData.size();
    string str(static_cast<const char*>(externalData.getRange(0, length)),
            length);
    if (!value->ParseFromString(str)) {
        throw FormatError(HERE, format("couldn't parse '%s' "
            "object in external storage as %s",
            name, value->GetTypeName().c_str()));
    }
    return true;
}


// See header file for documentation.
const char*
ExternalStorage::getWorkspace()
{
    return workspace.c_str();
}

// See header file for documentation.
void
ExternalStorage::setWorkspace(const char* pathPrefix)
{
    workspace = pathPrefix;
    fullName = pathPrefix;
    assert(pathPrefix[0] == '/');
    assert(pathPrefix[workspace.size()-1] == '/');
}

/**
 * Return the absolute node name (i.e., one that begins with "/") that
 * corresponds to the \c name argument. It is provided as a convenience for
 * subclasses.
 * 
 * \param name
 *      Name of a node; may be either relative or absolute.
 * 
 * \return
 *      If \c name starts with "/", then it is returned. Otherwise, an
 *      absolute node name is formed by concatenating the workspace name
 *      with \c name, and this is returned. Note: the return value is stored
 *      in a string in the ExternalStorage object, and will be overwritten
 *      the next time this method is invoked. If you need the result to
 *      last a long time, you better copy it. This method is not thread-safe:
 *      it assumes the caller has acquired a lock, so that no one else can
 *      invoke this method concurrently.
 */
const char*
ExternalStorage::getFullName(const char* name)
{
    if (name[0] == '/') {
        return name;
    }
    fullName.resize(workspace.size());
    fullName.append(name);
    return fullName.c_str();
}

/**
 * Given a locator string, see if there is an external storage system
 * corresponding to the locator. If so, open a connection to that
 * system.
 *
 * \param locator
 *      Currently two forms of external storage are supported: LogCabin and
 *      ZooKeeper.
 *      For LogCabin the string starts with "lc:" and the rest of the string is
 *      handed off to the LogCabin::Client::Cluster constructor (which
 *      accepts comma-separated host:port pairs for the LogCabin servers, such
 *      as "rc03:5254,rc04:5254"; each hostname may resolve to multiple
 *      addresses).
 *      For ZooKeeper the string starts with "zk:" and the
 *      rest of the string contains a comma-separated list of host:port
 *      pairs for the ZooKeeper servers, such as "rc03:2109,rc04:2109".
 *      In the future, other forms of external storage may be supported;
 *      each one will have its own distinctive prefix.
 * \param context
 *      Overall server information; needed by some forms of external storage.
 * @return
 *      If locator was recognized as an external storage locator, then
 *      the return value refers to an open connection to that system.
 *      Is locator was not recognized, then NULL is returned.
 */
ExternalStorage*
ExternalStorage::open(string locator, Context* context)
{
    if (storageOverride != NULL) {
        return storageOverride;
    }
    if (locator.find("lc:") == 0) {
        string cluster = locator.substr(3);
#if ENABLE_LOGCABIN
        return new LogCabinStorage(cluster);
#else
        RAMCLOUD_DIE("Could not open connection to LogCabin: support for "
                     "LogCabin was disabled at compile time");
#endif
    } else if (locator.find("zk:") == 0) {
        string cluster = locator.substr(3);
#if ENABLE_ZOOKEEPER
        return new ZooStorage(cluster, context->dispatch);
#else
        RAMCLOUD_DIE("Could not open connection to ZooKeeper: support for "
                     "ZooKeeper was disabled at compile time");
#endif
    }
    return NULL;
}

/**
 * Construct an Object.
 *
 * \param name
 *      Name of the object; NULL-terminated string. A local copy will
 *      be made in this Object.
 * \param value
 *      Value of the object, or NULL if none. A local copy will
 *      be made in this Object.
 * \param length
 *      Length of value, in bytes.
 */
ExternalStorage::Object::Object(const char* name, const char* value, int length)
    : name(NULL)
    , value(NULL)
    , length(0)
{
    size_t nameLength = strlen(name) + 1;
    this->name = static_cast<char*>(malloc(nameLength));
    memcpy(this->name, name, nameLength);
    if ((value != NULL) && (length > 0)) {
        this->value = static_cast<char*>(malloc(length));
        memcpy(this->value, value, length);
        this->length = length;
    }
}

/**
 * Destructor for Objects (must free storage).
 */
ExternalStorage::Object::~Object()
{
    free(name);
    free(value);
}

} // namespace RAMCloud
