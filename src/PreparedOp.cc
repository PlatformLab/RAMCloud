/* Copyright (c) 2015-2016 Stanford University
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

#include "PreparedOp.h"

namespace RAMCloud {

/**
 * Construct a PreparedOp in preparation for storing it in the log.
 * This form is used when the header information is available in
 * individual pieces, while the keys and data are stored in  a Buffer
 * (typical use: during txPrepare RPCs).
 *
 * \param type
 *      Type of the staged operation.
 * \param clientId
 *      leaseId given for this linearizable RPC.  First half of this
 *      transaction's unique identifier.
 * \param clientTxId
 *      Second half of this transaction's unique identifier.
 * \param rpcId
 *      rpcId given for this linearizable RPC.
 * \param tableId
 *      TableId for this object.
 * \param version
 *      Version number of this object, which is used to disambiguate
 *      different incarnations of objects with the same key.
 * \param timestamp
 *      The creation time of this object, as returned by the WallTime
 *      module. Used primarily by the cleaner to order live objects and
 *      improve future cleaning performance.
 * \param keysAndValueBuffer
 *      Buffer containing all chunks that will comprise this object's
 *      keysAndValue. Refer to Object.h for a visual description
 *      of keysAndValue.
 * \param startDataOffset
 *      Byte offset in the buffer where keysAndValue start
 * \param length
 *      Length of keysAndValue.
 *      A length of 0 means that the object occupies the entire buffer
 *      starting at offset.
 */
PreparedOp::PreparedOp(WireFormat::TxPrepare::OpType type,
                       uint64_t clientId,
                       uint64_t clientTxId,
                       uint64_t rpcId,
                       uint64_t tableId,
                       uint64_t version,
                       uint32_t timestamp,
                       Buffer& keysAndValueBuffer,
                       uint32_t startDataOffset,
                       uint32_t length)
    : header(type, clientId, clientTxId, rpcId)
    , object(tableId, version, timestamp, keysAndValueBuffer,
             startDataOffset, length)
{
}

/**
 * Construct a PreparedOp in preparation for storing it in the log.
 * Use this constructor when the data for an object is contiguous and
 * the header information is  available in individual pieces. This is
 * primarily used by unit tests to construct objects with just a
 * single key. The main function that will be invoked after a call
 * to this constructor is assembleForLog.
 *
 * \param type
 *      Type of the staged operation.
 * \param clientId
 *      leaseId given for this linearizable RPC.  First half of this
 *      transaction's unique identifier.
 * \param clientTxId
 *      Second half of this transaction's unique identifier.
 * \param rpcId
 *      rpcId given for this linearizable RPC.
 * \param key
 *      Primary key for this object
 * \param value
 *      Pointer to a single contiguous piece of memory that comprises this
 *      object's value.
 * \param valueLength
 *      Length of the value portion in bytes.
 * \param version
 *      Version number of this object, which is used to disambiguate
 *      different incarnations of objects with the same key.
 * \param timestamp
 *      The creation time of this object, as returned by the WallTime
 *      module. Used primarily by the cleaner to order live objects and
 *      improve future cleaning performance.
 * \param buffer
 *      A buffer that can be used temporarily to store the keys and value
 *      for the object. Its lifetime must cover the lifetime of this Object.
 * \param [out] length
 *      Total length of keysAndValue
 */
PreparedOp::PreparedOp(WireFormat::TxPrepare::OpType type,
                       uint64_t clientId,
                       uint64_t clientTxId,
                       uint64_t rpcId,
                       Key& key,
                       const void* value,
                       uint32_t valueLength,
                       uint64_t version,
                       uint32_t timestamp,
                       Buffer& buffer,
                       uint32_t *length)
    : header(type, clientId, clientTxId, rpcId)
    , object(key, value, valueLength, version, timestamp, buffer, length)
{
}

/**
 * Construct a PreparedOp using information in the log.
 * This form of the constructor is typically used for extracting
 * information out of the log. For example to serve txDecision requests
 * or to perform log cleaning.
 *
 * \param buffer
 *      Buffer referring to a complete PreparedOp in the log. It is the
 *      caller's responsibility to make sure that the buffer passed in
 *      actually contains a full object. If it does not, then behavior
 *      is undefined.
 * \param offset
 *      Starting offset in the buffer where the PreparedOp begins.
 * \param length
 *      Total length of the PreparedOp in bytes.
 *      A length of 0 means that the PreparedOp occupies the entire buffer
 *      starting at offset.
 */
PreparedOp::PreparedOp(Buffer& buffer, uint32_t offset, uint32_t length)
    : header(*buffer.getOffset<Header>(offset))
    , object(buffer, offset + sizeof32(header), length - sizeof32(header))
{
}

/**
 * Append the serialized header, participant list, and object to the
 * provided buffer.
 *
 * \param buffer
 *      The buffer to append all data to.
 */
void
PreparedOp::assembleForLog(Buffer& buffer)
{
    header.checksum = computeChecksum();
    buffer.appendCopy(&header, sizeof32(Header));
    object.assembleForLog(buffer);
}

/**
 * Compute a checksum on the PreparedOp and determine whether or not it matches
 * what is stored in the PreparedOp. Returns true if the checksum looks ok,
 * otherwise returns false.
 */
bool
PreparedOp::checkIntegrity()
{
    return computeChecksum() == header.checksum;
}

/**
 * Compute the PreparedOp's checksum and return it.
 */
uint32_t
PreparedOp::computeChecksum()
{
    Crc32C crc;
    // first compute the checksum on the object header excluding the
    // checksum field
    crc.update(this,
               downCast<uint32_t>(sizeof(header) -
               sizeof(header.checksum)));

    object.applyChecksum(&crc);

    return crc.getResult();
}

/**
 * Return the unique identifier for the transaction to which this prepare
 * operations belongs.
 */
TransactionId
PreparedOp::getTransactionId()
{
     return TransactionId(header.clientId, header.clientTxId);
}

/**
 * Construct a new tombstone for a given completed PreparedOp. Use this
 * constructor when generating new tombstones to be written to the log.
 *
 * \param op
 *      The preparedOp this tombstone is marking as completed.
 * \param segmentId
 *      The 64-bit identifier of the segment in which the PreparedOp this
 *      tombstone refers to exists. Once this segment is no longer in
 *      the system, this tombstone may be garbage collected.
 */
PreparedOpTombstone::PreparedOpTombstone(PreparedOp& op, uint64_t segmentId)
    : header(op.object.getTableId(),
             Key::getHash(op.object.getTableId(),
                          op.object.getKey(),
                          op.object.getKeyLength()),
             op.header.clientId,
             op.header.rpcId,
             segmentId)
    , tombstoneBuffer()
{
}

/**
 * Construct a tombstone object by deserializing an existing tombstone. Use
 * this constructor when reading existing tombstones from the log or from
 * individual log segments.
 *
 * \param buffer
 *      Buffer pointing to a complete serialized tombstone. It is the
 *      caller's responsibility to make sure that the buffer passed in
 *      actually contains a full tombstone. If it does not, then behavior
 *      is undefined.
 * \param offset
 *      Starting offset in the buffer where the tombstone begins.
 */
PreparedOpTombstone::PreparedOpTombstone(Buffer& buffer, uint32_t offset)
    : header(*buffer.getOffset<Header>(offset))
    , tombstoneBuffer(&buffer)
{
}

/**
 * Append the serialized tombstone to the provided buffer.
 *
 * \param buffer
 *      The buffer to append all data to.
 */
void
PreparedOpTombstone::assembleForLog(Buffer& buffer)
{
    header.checksum = computeChecksum();
    buffer.appendExternal(&header, sizeof32(Header));
}

/**
 * Compute a checksum on the PreparedOpTombstone and determine whether
 *  or not it matches what is stored in the PreparedOpTombstone.
 *  Returns true if the checksum looks ok, otherwise returns false.
 */
bool
PreparedOpTombstone::checkIntegrity()
{
    return computeChecksum() == header.checksum;
}

/**
 * Compute the PreparedOpTombstone's checksum and return it.
 */
uint32_t
PreparedOpTombstone::computeChecksum()
{
    Crc32C crc;
    // first compute the checksum on the object header excluding the
    // checksum field
    crc.update(this,
               downCast<uint32_t>(sizeof(header) -
               sizeof(header.checksum)));

    return crc.getResult();
}

} // namespace RAMCloud
