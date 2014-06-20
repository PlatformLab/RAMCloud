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

#include "DataBlock.h"

namespace RAMCloud {

/**
 * Construct a DataBlock. Its initial value is empty.
 */
DataBlock::DataBlock()
    : mutex()
    , block(NULL)
    , length(0)
{
}

/**
 * Destructor for DataBlocks (must free storage).
 */
DataBlock::~DataBlock()
{
    free(block);
    this->block = NULL;
}

/*
 * Store a new value for the block (make a copy of the input block), replacing
 * any existing value.
 *
 * \param block
 *      Address of the first byte of the value.  NULL means the value for
 *      the block is empty.
 * \param length
 *      Length of the value, in bytes; may be 0.
 */
void DataBlock::set(const void* block, size_t length)
{
    if (this->block != NULL) {
        free(this->block);
    }
    if ((block == NULL) || (length == 0)) {
        this->block = NULL;
        this->length = 0;
        return;
    }
    this->block = malloc(length);
    memcpy(this->block, block, length);
    this->length = length;
}

/*
 * Return the current contents of the block.
 *
 * \param output
 *      The contents of the block are copied into this buffer. If no value
 *      has been assigned to the block, the buffer will end up empty.
 */
void DataBlock::get(Buffer* output)
{
    output->reset();
    if (block == NULL) {
        return;
    }
    output->appendCopy(block, downCast<uint32_t>(length));
}

} // namespace RAMCloud
