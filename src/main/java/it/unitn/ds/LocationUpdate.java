package it.unitn.ds;

import akka.actor.ActorRef;

public class LocationUpdate {
    public final int index;
    public final int value;
    public UpdateStatus status;
    public int quorum;
    ActorRef replicaRequested; // The replica that got the WriteReqMsg from the client
    ActorRef clientRequesting; // The client that sent the WriteReqMsg

    public LocationUpdate(int index, int value, ActorRef clientRequesting, ActorRef replicaRequested) {
        this.index = index;
        this.value = value;
        this.status = UpdateStatus.STABLE;
        this.quorum = 0; // Quorum of this Update Request with this.value and this.index
        this.clientRequesting = clientRequesting;
        this.replicaRequested = replicaRequested;
    }

    public LocationUpdate(LocationUpdate other) {
        this.index = other.index;
        this.value = other.value;
        this.status = other.status;
        this.quorum = other.quorum;
        this.clientRequesting = other.clientRequesting;
        this.replicaRequested = other.replicaRequested;
    }

    public void setStable() {
        this.status = UpdateStatus.STABLE;
    }

    public void setUnstable() {
        this.status = UpdateStatus.UNSTABLE;
    }

    public void increaseQuorum() {
        this.quorum = this.quorum + 1;
    }

    public boolean isStable() {
        return this.status == UpdateStatus.STABLE;
    }

    public enum UpdateStatus {
        UNSTABLE,
        STABLE
    }
}
