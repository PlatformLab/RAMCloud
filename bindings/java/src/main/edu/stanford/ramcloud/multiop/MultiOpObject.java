/* Copyright (c) 2014 Stanford University
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

package edu.stanford.ramcloud.multiop;

import edu.stanford.ramcloud.*;

/**
 * RAMCloudObject returned from multi-op operations. Holds a status code for
 * the status of this operation.
 */
public abstract class MultiOpObject extends RAMCloudObject {
    /**
     * The ID of the table that this operation should operate on.
     */
    private long tableId;
    /**
     * The returned status code of this operation.
     */
    private Status status;

    /**
     * Contructor for MultiOpObject that initializes all values.
     *
     * @param tableId
     *      The ID of the table this object belongs to.
     * @param key
     *      The key of this object.
     * @param value
     *      The value of this object.
     * @param version
     *      The version of this object.
     * @param status
     *      The status of the operation for this object.
     */
    public MultiOpObject(long tableId, byte[] key, byte[] value, long version,
                         Status status) {
        super(key, value, version);
        this.tableId = tableId;
        this.status = status;
    }

    /**
     * Get the ID of the table this object is in.
     *
     * @return The ID of the table that this object is in.
     */
    public long getTableId() {
        return tableId;
    }

    /**
     * Set the ID of the table this object is in.
     *
     * @param tableId
     *      The ID of the table that this object is in.
     */
    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    /**
     * Gets the status code of this multi-op.
     *
     * @return The status code of this multi-op, after it has been carried out.
     */
    public Status getStatus() {
        return status;
    }

    /**
     * Sets the status code of this multi-op. Should only be called internally.
     *
     * @param status
     *      The new status of this multi-op.
     */
    public void setStatus(Status status) {
        this.status = status;
    }
}
