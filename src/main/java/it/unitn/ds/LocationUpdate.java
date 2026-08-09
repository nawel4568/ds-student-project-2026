package it.unitn.ds;

import scala.collection.generic.BitOperations;

public class LocationUpdate {
    public final int index;
    public final int value;
    public UpdateStatus status;
    public int quorum;

    public LocationUpdate(int index, int value) {
        this.index = index;
        this.value = value;
        this.status = UpdateStatus.STABLE;
        this.quorum = 0; // Quorum of this Update Request with this.value and this.index
    }

    public LocationUpdate(LocationUpdate other){
        this.index = other.index;
        this.value = other.value;
        this.status = other.status;
        this.quorum = other.quorum;
    }

    public void setStable(){
        this.status = UpdateStatus.STABLE;
    }

    public void setUnstable(){
        this.status = UpdateStatus.UNSTABLE;
    }

    public void increaseQuorum() {
        this.quorum = this.quorum + 1;
    }

    public enum UpdateStatus {
        UNSTABLE,
        STABLE
    }

}