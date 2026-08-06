package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.*;

public class Replica extends AbstractReplica {

    private final int[] locations = new int[AbstractReplica.POSITIONS_LIST_LENGTH]; // the stable values
    private final Map<Integer, ActorRef> replicasGroup;
    private int coordinator_id;

    private  EpochSeqNum lastStable; // the epoch and seqNum (index) of the last stable value
    private final EpochSeqNum unstableLocation;
    private int quorum;
    private boolean stable;


    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        // TODO: implement
        this.replicasGroup = new HashMap<>();
        this.coordinator_id = this.id;
        this.lastStable = new EpochSeqNum(0, 0);
        this.unstableLocation = new EpochSeqNum(0, 0);
        this.quorum = 0;
        this.stable = true;
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    @Override
    public int getSystemNumberOfActors() {
        // TODO: implement
        return 0;
    }

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        // TODO: implement
    }

    @Override
    public void initSystem(InitSystem sysInit) {
        // TODO: implement
        Logger.debug("Replica " + this.id + "is starting.");
        this.coordinator_id = sysInit.coordinator_id;
        this.replicasGroup.putAll(sysInit.group);
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // TODO add your message handlers here .match(, )
                .match(ReadReqMsg.class, this::onReadReqMsg)
                .match(WriteReqMsg.class, this::onWriteReqMsg)
                .match(UpdateRequestMsg.class, this::onUpdateRequestMsg)
                .match(AckMsg.class, this::onAckMsg)
                .build();
    }

    /* Messages */

    /* Read Request from Client */
    public static class ReadReqMsg implements Serializable {
        int index;
        public ReadReqMsg(int index){
            this.index = index;
        }

    }
    private void onReadReqMsg(ReadReqMsg msg) {
        /* Send the Value back to the Client */
        Logger.debug(getSelf().path().name() + " Received a Read Request from " + getSender().path().name());
        this.getSender().tell(new AbstractClient.ReadResult(true, msg.index, this.locations[msg.index], this.id), getSelf());
    }

    /* Write Request from Client */
    public static class WriteReqMsg implements Serializable {
        int index;
        int value;
        public WriteReqMsg(int index, int value){
            this.index = index;
            this.value = value;
        }

    }
    private void onWriteReqMsg(WriteReqMsg msg) {
        /* Send an Update Request to the coordinator */
        Logger.debug(getSelf().path().name() + " Received a Write Request from " + getSender().path().name());
        this.replicasGroup.get(coordinator_id).tell(new UpdateRequestMsg(msg.index, msg.value), getSelf());
    }

    /* Update Request From Replica */
    public static class UpdateRequestMsg {
        int index;
        int value;
        public UpdateRequestMsg(int index, int value) {
            this.index = index;
            this.value = value;
        }

        public UpdateRequestMsg(UpdateRequestMsg other) {
            this.index = other.index;
            this.value = other.value;
        }
    }

    private void onUpdateRequestMsg(UpdateRequestMsg msg) {
        Logger.debug(getSelf().path().name() + " Received an Update Request from " + getSender().path().name());
        if(this.id == this.coordinator_id) {
            // Coordinator Receives the Update Msg from a Replica
            this.stable = false;
            this.locations[msg.index] = msg.value; // Writing the received value to the locations
            this.unstableLocation.setSeqNum(this.unstableLocation.seqNum++);
            this.quorum++; // increasing the quorum
            Utils.broadcastUpdate(getSelf(), this.replicasGroup, msg); // Broadcast the Update to other Replicas
        }
        else {
            // Replica Receives the Update Msg from the Coordinator
            this.locations[msg.index] = msg.value; // Writing the received value to the locations
            this.unstableLocation.setSeqNum(this.unstableLocation.seqNum++);
            getSender().tell(new AckMsg(), getSelf());
        }
    }

    public static class AckMsg {
        public AckMsg() {
        }
    }

    private void onAckMsg(AckMsg msg) {
        Logger.debug(getSelf().path().name() + " Received an ACK Msg from " + getSender().path().name());
//        if(!stable) {
//            this.quorum++;
//            if (quorum > this.replicasGroup.size() / 2) {
//                this.lastStable = this.unstableLocation;
//            }
//        }
    }

}
