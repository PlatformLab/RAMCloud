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

#ifndef RAMCLOUD_EXTERNALSTORAGE_H
#define RAMCLOUD_EXTERNALSTORAGE_H

#include <google/protobuf/message.h>

#include "Buffer.h"
#include "Exception.h"

namespace RAMCloud {
class Context;

/**
 * This class defines an interface for storing data durably in a separate
 * key-value store with a hierarchical name space.  An object of this class
 * represents a connection to a particular storage system, such as ZooKeeper.
 * This class is used by the coordinator to store configuration data that
 * must survive coordinator crashes, such as information about cluster
 * membership and tablet locations. This class is also used for leader
 * election (ensuring that there is exactly one server acting as coordinator
 * at a time).
 *
 * The storage provided by this class is expected to be highly durable
 * and available (for example, it might be implemented using a consensus
 * approach with multiple storage servers). No recoverable exceptions or
 * errors should be thrown by this class; the only exceptions thrown should
 * be unrecoverable ones such as LostLeadershipException or internal
 * errors that should result in coordinator crashes. The storage system is
 * expected to be totally reliable; the worst that should happen is for
 * calls to this class to block while waiting for the storage system to
 * come back online.
 *
 * The goal of this interface is to support multiple implementations using
 * different storage systems; however, the interface was designed with
 * ZooKeeper in mind as the storage system.
 */
class ExternalStorage {
  PUBLIC:
    /**
     * The following structure holds information about a single object
     * including both its name and its value (if it has one). Its primary use
     * is for enumerating all of the children of a node.
     */
    struct Object {
        /// Name of the object within its parent (not a full path name).
        /// NULL-terminated. Dynamically-allocated, freed on destruction.
        char* name;

        /// Value of the object (not necessarily NULL-terminated). NULL
        /// means there is no value for this name (it is a pure node).
        /// If non-NULL, will be freed on destruction.
        char* value;

        /// Length of value, in bytes.
        int length;

        Object(const char* name, const char* value, int length);
        ~Object();

        // The following constructors are provided so that Objects can
        // be used in std::vectors.
        Object()
            : name(NULL)
            , value(NULL)
            , length(0)
        {}
        // move constructor
        Object(Object&& other)
            : name(other.name)
            , value(other.value)
            , length(other.length)
        {
            other.name = NULL;
            other.value = NULL;
            other.length = 0;
        }
        // move assignment
        Object& operator=(Object&& other)
        {
            if (this != &other) {
                std::swap(name, other.name);
                std::swap(value, other.value);
                std::swap(length, other.length);
            }
            return *this;
        }
        DISALLOW_COPY_AND_ASSIGN(Object);
    };

    /**
     * This exception is thrown if we lose leadership (i.e. some other
     * server decided that we are dead, so it took over as coordinator).
     * When this exception is thrown, the coordinator must either exit
     * or reset all of its state and do nothing until it becomes leader
     * again. Note: it's OK to access external storage if we have never had
     * leadership (presumably the caller knows how to do this safely, such
     * as just read-only access), but once we have obtained leadership,
     * we must always have it in the future.
     */
    struct LostLeadershipException : public Exception {
        explicit LostLeadershipException(const CodeLocation& where)
            : Exception(where) {}
    };

    /**
     * This exception is thrown if an object cannot be parsed properly
     * as a protocol buffer of the given type
     */
    struct FormatError : public Exception {
        explicit FormatError(const CodeLocation& where, std::string msg)
            : Exception(where, msg) {}
    };

    /**
     * This enum is passed as an argument to the "set" method as a hint
     * to indicate whether or not the object already exists.
     */
    enum Hint {
        CREATE,                    // A new object is being created.
        UPDATE                     // An existing object is being overwritten.
    };

    ExternalStorage();
    virtual ~ExternalStorage() {}

    /**
     * This method is invoked when a server first starts up.  At this point
     * the server is in "backup" mode, under the assumption that some other
     * server acting as leader. This method waits until there is no longer
     * an active leader; at that point it claims leadership and returns.
     * Once this method returns, the caller can begin acting as coordinator.
     * After this method returns, a background thread may crash this process at
     * any time if the lease cannot be maintained.
     *
     * (It used to not be the case that the process could crash as soon as a
     * lease was lost. A background thread would have to set a flag, and
     * another thread would eventually call back into the ExternalStorage,
     * notice the flag, and throw LostLeadershipException. The problem was that
     * we don't know how long it'll take for another thread to call back in,
     * and meanwhile the coordinator could engage in unsafe activity. Directly
     * crashing the process as soon as a lease is lost is simpler and safer.)
     *
     *  \param name
     *      Name of an object that is used to synchronize leader election
     *      (and which will hold information identifying the current
     *      leader); NULL-terminated hierarchical path containing one or
     *      more path elements separated by slashes, such as "foo" or
     *      "/foo/bar". Relative names (no leading slash) are concatenated
     *      to the current workspace.
     *  \param leaderInfo
     *      Information about this server (e.g., service locator that clients
     *      can use to connect), which will be stored in the object given
     *      by "name" once we become leader.
     */
    virtual void becomeLeader(const char* name, const string& leaderInfo) = 0;

    /**
     * Read a single non-lease object from external storage.
     *
     * See getLeaderInfo() to read the owner of a lease object.
     *
     * \param name
     *      Name of the desired object; NULL-terminated hierarchical path
     *      containing one or more path elements separated by slashes,
     *      such as "foo" or "/foo/bar". Relative names (no leading slash)
     *      are concatenated to the current workspace.
     * \param value
     *      The contents of the object are returned in this buffer.
     *      If the object is a pure node (directory), value is empty (and true
     *      is returned).
     *
     * \return
     *      If the specified object exists, then true is returned. If there
     *      is no such object, then false is returned and value is empty.
     *
     * \throws LostLeadershipException
     */
    virtual bool get(const char* name, Buffer* value) = 0;

    /**
     * Return information about all of the children of a node.
     *
     * \param name
     *      Name of the node whose children should be enumerated;
     *      NULL-terminated hierarchical path containing one or more path
     *      elements separated by slashes, such as "foo" or "/foo/bar".
     *      Relative names (no leading slash) are concatenated to the
     *      current workspace.
     * \param children
     *      This vector will be filled in with one entry for each child
     *      of name, including pure (directory) children. Any previous
     *      contents of the vector are discarded.
     *
     * \throws LostLeadershipException
     */
    virtual void getChildren(const char* name, vector<Object>* children) = 0;

    /**
     * Read a single lease object from external storage.
     *
     * Some external storage implementations pack more information than the
     * leader locator into the leader's lease object. This method extracts just
     * the value of 'leaderInfo' as previously passed into becomeLeader().
     *
     * Everything else is the same as in get(), and the default implementation
     * is just an alias for get().
     */
    virtual bool getLeaderInfo(const char* name, Buffer* value);

    bool getProtoBuf(const char* name, google::protobuf::Message* value);

    /**
     * Return the last value passed to setWorkspace (i.e. the path prefix
     * used for relative node names).
     *
     * \throws LostLeadershipException
     *
     * \warning This method is not thread safe.
     */
    virtual const char* getWorkspace();

    static ExternalStorage* open(string locator, Context* context);

    /**
     * Remove an object, if it exists. If it doesn't exist, do nothing.
     * If name has children, all of the children are removed recursively,
     * followed by name.
     *
     * \param name
     *      Name of the desired object; NULL-terminated hierarchical path
     *      containing one or more path elements separated by slashes,
     *      such as "foo" or "/foo/bar". Relative names (no leading slash)
     *      are concatenated to the current workspace.
     *
     * \throws LostLeadershipException
     */
    virtual void remove(const char* name) = 0;

    /**
     * Set the value of a particular object in external storage. If the
     * object did not previously exist, then it is created; otherwise
     * its existing value is overwritten. If the parent node of the object
     * does not exist, it will be created automatically.
     *
     * \param flavor
     *      Either Hint::CREATE or Hint::UPDATE: indicates whether or not
     *      the object (most likely) already exists. This argument does not
     *      affect the outcome, but if specified incorrectly it may impact
     *      performance on some storage systems (e.g., ZooKeeper has separate
     *      "create" and "update" operations, so we may have to try both to
     *      find one that works).
     * \param name
     *      Name of the desired object; NULL-terminated hierarchical path
     *      containing one or more path elements separated by slashes,
     *      such as "foo" or "/foo/bar". Relative names (no leading slash)
     *      are concatenated to the current workspace.
     * \param value
     *      Address of first byte of value to store for this object.
     * \param valueLength
     *      Size of value, in bytes. If less than zero, then value
     *      must be a NULL-terminated string, and the length will be computed
     *      automatically to include all the characters in the string and
     *      the terminating NULL character.
     *
     * \throws LostLeadershipException
     */
    virtual void set(Hint flavor, const char* name, const char* value,
            int valueLength = -1) = 0;

    /**
     * Specify the current workspace for the application. This is
     * equivalent to a working directory: if a node name specified to
     * any other method does not begin with a slash, then the workspace
     * is prepended to that name. For example, if the workspace is "/a/b/"
     * then the node name "c/d" really refers to "/a/b/c/d".  If this
     * method has not been called, then the workspace defaults to "/".
     *
     * \param pathPrefix
     *      Top-level node in the workspace. Must start and end with "/".
     *
     * \throws LostLeadershipException
     *
     * \warning This method is not thread safe.
     */
    virtual void setWorkspace(const char* pathPrefix);

  PROTECTED:
    const char* getFullName(const char* name);

    /// Holds the current workspace prefix.
    string workspace;

    /// Used to create full node names by concatenating a relative node
    /// name with the workspace. This object is retained in order to avoid
    /// malloc costs for recreating it (but it has the limitation that only
    /// one such name can be stored at a time). This string always contains
    /// workspace as its first characters.
    string fullName;

    /// Used for unit testing: if non-NULL, then ZooStorage::open
    /// ignores its arguments and returns this value instead.
    static ExternalStorage* storageOverride;

    DISALLOW_COPY_AND_ASSIGN(ExternalStorage);
};

} // namespace RAMCloud

#endif // RAMCLOUD_EXTERNALSTORAGE_H

