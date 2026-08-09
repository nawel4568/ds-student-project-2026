package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import javax.xml.stream.Location;
import java.io.Serializable;
import java.util.*;

public class Replica extends AbstractReplica {

    private final int[] locations = new int[AbstractReplica.POSITIONS_LIST_LENGTH]; // the stable values
    private final Map<Integer, ActorRef> replicasGroup;
    private int coordinator_id;

    private  EpochSeqNum lastUpdate; // the epoch and seqNum of the last Update Req
    private LinkedHashMap<EpochSeqNum, LocationUpdate> updatesHistory;


    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        // TODO: implement
        this.replicasGroup = new HashMap<>();
        this.coordinator_id = this.id;
        this.lastUpdate = new EpochSeqNum(0, 0);

        this.updatesHistory = new LinkedHashMap<EpochSeqNum, LocationUpdate>();
        this.updatesHistory.put(lastUpdate, new LocationUpdate(0, 0));
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
                .match(UpdateMsg.class, this::onUpdateMsg)
                .match(AckMsg.class, this::onAckMsg)
                .match(WriteOKMsg.class, this::onWriteOKMsg)
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
        // this is received only by the coordinator
        Logger.debug(getSelf().path().name() + " Received an Update Request from " + getSender().path().name());

        // Coordinator Receives the Update Msg from a Replica
        this.lastUpdate.incrementSeqNum();
        LocationUpdate currentUpdate = new LocationUpdate(msg.index, msg.value);
        currentUpdate.setUnstable();
        currentUpdate.increaseQuorum();
        updatesHistory.put(this.lastUpdate, currentUpdate);

        Utils.broadcastUpdate(getSelf(), this.replicasGroup, new Replica.UpdateMsg(new EpochSeqNum(this.lastUpdate), currentUpdate)); // Broadcast the Update to other Replicas

    }

    public static class UpdateMsg {
        EpochSeqNum epochSeqNum;
        LocationUpdate currentUpdate;

        public UpdateMsg(EpochSeqNum epochSeqNum, LocationUpdate currentUpdate) {
            this.epochSeqNum = epochSeqNum;
            this.currentUpdate = currentUpdate;
        }
    }

    private void onUpdateMsg(UpdateMsg msg) {
        // Message received by Replica from Coordinator
        this.lastUpdate = new EpochSeqNum(msg.epochSeqNum);
        updatesHistory.put(this.lastUpdate, new LocationUpdate(msg.currentUpdate));
        getSender().tell(new AckMsg(this.lastUpdate), getSelf());

    }

    public static class AckMsg {
        EpochSeqNum epochSeqNum;
        public AckMsg(EpochSeqNum epochSeqNum) {
            this.epochSeqNum = epochSeqNum;
        }
    }

    private void onAckMsg(AckMsg msg) {
        // Received by the Coordinator from Replicas
        Logger.debug(getSelf().path().name() + " Received an ACK Msg from " + getSender().path().name());
        LocationUpdate currentUpdate = updatesHistory.get(msg.epochSeqNum);
        currentUpdate.quorum ++;
        LocationUpdate localUpdate = updatesHistory.get(msg.epochSeqNum); // to manage the ACK msg after the WriteOK is broadcasted

        if(currentUpdate.quorum < (replicasGroup.size()/2)) {
            // add the updated quorum and do nothing
            updatesHistory.put(msg.epochSeqNum, currentUpdate);
        }
        else if(!localUpdate.isStable()) { // the if is to skip the ACK msg after th update is set to stable already
            // 1. Set the update in history to Stable
            // 2. Write the update to the locations array
            currentUpdate.setStable();
            updatesHistory.put(msg.epochSeqNum, currentUpdate);
            locations[currentUpdate.index] = currentUpdate.value;
            Utils.broadcastWriteOK(getSelf(), this.replicasGroup, new Replica.WriteOKMsg(msg.epochSeqNum));
        }
    }

    public static class WriteOKMsg {
        EpochSeqNum epochSeqNum;
        public WriteOKMsg(EpochSeqNum epochSeqNum) {
            this.epochSeqNum = epochSeqNum;

        }
    }

    private void onWriteOKMsg(WriteOKMsg msg) {
        // Received by the Replicas
        LocationUpdate currentUpdate = updatesHistory.get(msg.epochSeqNum);
        currentUpdate.setStable();
        updatesHistory.put(msg.epochSeqNum, currentUpdate);
        locations[currentUpdate.index] = currentUpdate.value;
    }


}
