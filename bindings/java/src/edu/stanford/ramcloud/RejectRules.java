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
package edu.stanford.ramcloud;

/**
 * Used in conditional operations to specify conditions under which an operation
 * should be aborted with an error.
 *
 * RejectRules are typically used to ensure consistency of updates; for example,
 * we might want to update a value but only if it hasn't changed since the last
 * time we read it. If a RejectRules object is passed to an operation, the
 * operation will be aborted if any of the following conditions are satisfied: -
 * doesntExist is true and the object does not exist - exists is true and the
 * object does exist - versionLeGiven is true and the object exists with a
 * version less than or equal to givenVersion. - versionNeGiven is nonzero and
 * the object exists with a version different from givenVersion.
 */
public class RejectRules {

    private long givenVersion = -1;
    private boolean doesntExist = false;
    private boolean exists = false;
    private boolean versionLeGiven = false;
    private boolean versionNeGiven = false;

    /**
     * Set the version number for versionLeGiven and versionNeGiven to compare
     * to.
     *
     * @param givenVersion
     *            Version number to compare to.
     */
    public void setGivenVersion(long givenVersion) {
        this.givenVersion = givenVersion;
    }

    /**
     * Get the current version to compare to.
     *
     * @return The version number being compared to.
     */
    public long getGivenVersion() {
        return givenVersion;
    }

    public void rejectIfDoesntExist(boolean doesntExist) {
        this.doesntExist = doesntExist;
    }

    public boolean rejectIfDoesntExist() {
        return doesntExist;
    }

    public void rejectIfExists(boolean exists) {
        this.exists = exists;
    }

    public boolean rejectIfExists() {
        return exists;
    }

    public void rejectIfVersionLeGiven(boolean versionLeGiven) {
        this.versionLeGiven = versionLeGiven;
    }

    public boolean rejectIfVersionLeGiven() {
        return versionLeGiven;
    }

    public void rejectIfVersionNeGiven(boolean versionNeGiven) {
        this.versionNeGiven = versionNeGiven;
    }

    public boolean rejectIfVersionNeGiven() {
        return versionNeGiven;
    }
}
