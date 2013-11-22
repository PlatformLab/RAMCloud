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

namespace RAMCloud {

/**
 * Constructor for ExternalStorage objects.
 */
ExternalStorage::ExternalStorage()
    : workspace("/")
    , fullName(workspace)
    , testName(NULL)
{}

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
    if (testName != NULL) {
        return testName;
    }
    if (name[0] == '/') {
        return name;
    }
    fullName.resize(workspace.size());
    fullName.append(name);
    return fullName.c_str();
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
