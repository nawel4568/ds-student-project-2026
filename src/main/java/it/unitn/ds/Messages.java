package it.unitn.ds;

import java.io.Serializable;

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
        public final int index;

        public ReadReqMsg(int index) {
            this.index = index;
        }
    }

    /* Write request from a client */
    public static final class WriteReqMsg implements Serializable {
        public final int index;
        public final int value;

        public WriteReqMsg(int index, int value) {
            this.index = index;
            this.value = value;
        }
    }

    // =================================================================================
    // Two-phase update
    // =================================================================================

    /* Write request forwarded by a replica to the coordinator */
    public static final class UpdateRequestMsg implements Serializable {
        public final int index;
        public final int value;
        public final ActorRef clientRequesting; // to know which client to send the WriteResult to

        public UpdateRequestMsg(int index, int value, ActorRef clientRequesting) {
            this.index = index;
            this.value = value;
            this.clientRequesting = clientRequesting;
        }
    }

    /* Update broadcast by the coordinator */
    public static final class UpdateMsg implements Serializable {
        public final EpochSeqNum epochSeqNum;
        public final int index;
        public final int value;
        public final ActorRef clientRequesting; // client that sent the write
        public final ActorRef replicaRequested; // replica that received the write and replies to the client

        public UpdateMsg(EpochSeqNum epochSeqNum, int index, int value, ActorRef clientRequesting,
                ActorRef replicaRequested) {
            this.epochSeqNum = epochSeqNum;
            this.index = index;
            this.value = value;
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
}
