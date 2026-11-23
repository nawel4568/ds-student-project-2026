package it.unitn.ds;

import java.io.Serializable;
import java.util.Collections;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import akka.actor.ActorRef;

/**
 * Messages exchanged between actors. Akka passes local messages by reference, so every
 * message must be immutable (checked by ImmutableMessagesTest).
 */
public final class Messages {

    private Messages() {
    }

    // =================================================================================
    // Client API
    // =================================================================================

    /* Read request from a client */
    public static final class ReadReqMsg implements Serializable {
        public final int reqId;
        public final int index;

        public ReadReqMsg(int reqId, int index) {
            this.reqId = reqId;
            this.index = index;
        }
    }

    /* Write request from a client */
    public static final class WriteReqMsg implements Serializable {
        public final int reqId;
        public final int index;
        public final int value;

        public WriteReqMsg(int reqId, int index, int value) {
            this.reqId = reqId;
            this.index = index;
            this.value = value;
        }
    }

    /* Reply to a read, tagged with the client's request id */
    public static final class ReadReplyMsg implements Serializable {
        public final int reqId;
        public final AbstractClient.ReadResult result;

        public ReadReplyMsg(int reqId, AbstractClient.ReadResult result) {
            this.reqId = reqId;
            this.result = result;
        }
    }

    /* Reply to a write, tagged with the client's request id */
    public static final class WriteReplyMsg implements Serializable {
        public final int reqId;
        public final AbstractClient.WriteResult result;

        public WriteReplyMsg(int reqId, AbstractClient.WriteResult result) {
            this.reqId = reqId;
            this.result = result;
        }
    }

    // =================================================================================
    // Two-phase update
    // =================================================================================

    /* Write request forwarded by a replica to the coordinator */
    public static final class UpdateRequestMsg implements Serializable {
        public final int index;
        public final int value;
        public final int reqId;
        public final ActorRef clientRequesting;

        public UpdateRequestMsg(int index, int value, int reqId, ActorRef clientRequesting) {
            this.index = index;
            this.value = value;
            this.reqId = reqId;
            this.clientRequesting = clientRequesting;
        }
    }

    /* Update broadcast by the coordinator */
    public static final class UpdateMsg implements Serializable {
        public final EpochSeqNum epochSeqNum;
        public final int index;
        public final int value;
        public final int reqId; // client's request id
        public final ActorRef clientRequesting; // client that sent the write
        public final ActorRef replicaRequested; // replica that received the write and replies to the client

        public UpdateMsg(EpochSeqNum epochSeqNum, int index, int value, int reqId, ActorRef clientRequesting,
                ActorRef replicaRequested) {
            this.epochSeqNum = epochSeqNum;
            this.index = index;
            this.value = value;
            this.reqId = reqId;
            this.clientRequesting = clientRequesting;
            this.replicaRequested = replicaRequested;
        }
    }

    /* Ack of an update, from a replica to the coordinator */
    public static final class AckMsg implements Serializable {
        public final EpochSeqNum epochSeqNum;

        public AckMsg(EpochSeqNum epochSeqNum) {
            this.epochSeqNum = epochSeqNum;
        }
    }

    /* Commit of an update, broadcast by the coordinator once a quorum acked */
    public static final class WriteOKMsg implements Serializable {
        public final EpochSeqNum epochSeqNum;

        public WriteOKMsg(EpochSeqNum epochSeqNum) {
            this.epochSeqNum = epochSeqNum;
        }
    }

    // =================================================================================
    // Failure detection
    // =================================================================================

    /* Heartbeat from the coordinator to the replicas */
    public static final class HeartbeatMsg implements Serializable {
        public final int epoch;

        public HeartbeatMsg(int epoch) {
            this.epoch = epoch;
        }
    }

    // =================================================================================
    // Coordinator election
    // =================================================================================

    /* Election token circulating on the ring, with each participant's last update */
    public static final class ElectionMsg implements Serializable {
        public final SortedSet<ElectionUpdate> electionUpdateList;

        public ElectionMsg(SortedSet<ElectionUpdate> electionUpdateList) {
            this.electionUpdateList = Collections.unmodifiableSortedSet(new TreeSet<>(electionUpdateList));
        }

        public ElectionMsg(ElectionMsg other) {
            this.electionUpdateList = other.electionUpdateList;
        }
    }

    /* Ack of an ElectionMsg, to the previous replica in the ring */
    public static final class ACKElectionMsg implements Serializable {
    }

    /* New coordinator announcement, with the updates the receiver is missing */
    public static final class SyncMsg implements Serializable {
        public final int newEpoch;
        public final int newCoordinatorId;
        public final SortedMap<EpochSeqNum, UpdateEntry> missingUpdates;

        public SyncMsg(int newEpoch, int newCoordinatorId, SortedMap<EpochSeqNum, UpdateEntry> missingUpdates) {
            this.newEpoch = newEpoch;
            this.newCoordinatorId = newCoordinatorId;
            this.missingUpdates = Collections.unmodifiableSortedMap(new TreeMap<>(missingUpdates));
        }
    }
}
