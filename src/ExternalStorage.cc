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
