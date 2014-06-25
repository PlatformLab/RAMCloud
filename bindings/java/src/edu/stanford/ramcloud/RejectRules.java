package edu.stanford.ramcloud;

// TODO: Comment block
public class RejectRules {
    private long givenVersion = -1;
    private boolean doesntExist = false;
    private boolean exists = false;
    private boolean versionLeGiven = false;
    private boolean versionNeGiven = false;

    public void setGivenVersion(long givenVersion) {
        this.givenVersion = givenVersion;
    }

    public long getGivenVersion() {
        return givenVersion;
    }

    public void setDoesntExist(boolean doesntExist) {
        this.doesntExist = doesntExist;
    }

    public boolean doesntExist() {
        return doesntExist;
    }

    public void setExists(boolean exists) {
        this.exists = exists;
    }

    public boolean exists() {
        return exists;
    }

    public void setVersionLeGiven(boolean versionLeGiven) {
        this.versionLeGiven = versionLeGiven;
    }

    public boolean versionLeGiven() {
        return versionLeGiven;
    }

    public void setVersionNeGiven(boolean versionNeGiven) {
        this.versionNeGiven = versionNeGiven;
    }

    public boolean versionNeGiven() {
        return versionNeGiven;
    }
}
