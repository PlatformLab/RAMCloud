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
 * RAMCloudObject used for multi-read operations.
 */
public class MultiRemoveObject extends MultiOpObject {
    /**
     * An object that stores data on the conditions under which this operation
     * should abort.
     */
    private RejectRules rejectRules;
    
    /**
     * Constructor for multi-remove requests.
     *
     * @param tableId
     *      The ID of the table to remove this object from.
     * @param key
     *      The key of the object to remove.
     * @param rules
     *      The conditions under which to abort the remove.
     */
    public MultiRemoveObject(long tableId,
                             byte[] key,
                             RejectRules rules) {
        super(tableId, key, null, -1L, Status.STATUS_OK);
        this.rejectRules = rules;
    }

    /**
     * Constructor for multi-remove requests.
     *
     * @see #MultiRemoveObject(long, byte[], edu.stanford.ramcloud.RejectRules)
     */
    public MultiRemoveObject(long tableId, byte[] key) {
        this(tableId, key, null);
    }

    /**
     * Constructor for multi-remove requests.
     *
     * @see #MultiRemoveObject(long, byte[], edu.stanford.ramcloud.RejectRules)
     */
    public MultiRemoveObject(long tableId, String key) {
        this(tableId, key.getBytes(), null);
    }

    /**
     * Constructor for multi-remove requests.
     *
     * @see #MultiRemoveObject(long, byte[], edu.stanford.ramcloud.RejectRules)
     */
    public MultiRemoveObject(long tableId, String key,
                             RejectRules rules) {
        this(tableId, key.getBytes(), rules);
    }

    /**
     * Get the circumstances under which this remove will abort.
     *
     * @return A RejectRules detailing the circumstances under which this remove
     *      will abort.
     */
    public RejectRules getRejectRules() {
        return rejectRules;
    }
    
    /**
     * Set the circumstances under which this remove will abort.
     *
     * @param rules
     *      The circumstances under which this remove will abort.
     */
    public void setRejectRules(RejectRules rules) {
        this.rejectRules = rules;
    }
}
