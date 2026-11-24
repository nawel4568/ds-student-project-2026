package it.unitn.ds;

import akka.actor.ActorRef;

/**
 * One write in a replica's update history: positions[index] = value, plus who to reply to. Immutable, so it can be
 * shared in SyncMsg; whether it is still pending is tracked by the replica.
 */
public final class UpdateEntry {
    public final int index;
    public final int value;
    public final int reqId; // The client's request id, echoed in the WriteReplyMsg
    public final ActorRef clientRequesting; // The client that sent the WriteReqMsg
    public final ActorRef replicaRequested; // The replica that got the WriteReqMsg from the client

    public UpdateEntry(int index, int value, int reqId, ActorRef clientRequesting, ActorRef replicaRequested) {
        this.index = index;
        this.value = value;
        this.reqId = reqId;
        this.clientRequesting = clientRequesting;
        this.replicaRequested = replicaRequested;
    }
}
