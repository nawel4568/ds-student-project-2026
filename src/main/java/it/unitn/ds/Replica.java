package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.*;

public class Replica extends AbstractReplica {

    private final int[] locations = new int[AbstractReplica.POSITIONS_LIST_LENGTH];
    private final Map<Integer, ActorRef> ReplicasGroup;
    private int coordinator_id;

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        // TODO: implement
        this.ReplicasGroup = new HashMap<>();
        this.coordinator_id = this.id;

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
        Logger.log("Replica " + this.id + "is starting.");
        this.coordinator_id = sysInit.coordinator_id;
        this.ReplicasGroup.putAll(sysInit.group);
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // TODO add your message handlers here .match(, )
                .match(ReadReqMsg.class, this::onReadReqMsg)
                .match(WriteReqMsg.class, this::onWriteReqMsg)
                .match(UpdateRequestMsg.class, this::onUpdateRequestMsg)
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
        this.ReplicasGroup.get(coordinator_id).tell(new UpdateRequestMsg(msg.index, msg.value), getSelf());
    }

    /* Update Request From Replica */
    public static class UpdateRequestMsg {
        int index;
        int value;
        public UpdateRequestMsg(int index, int value) {
            this.index = index;
            this.value = value;
        }
    }

    private void onUpdateRequestMsg(UpdateRequestMsg msg) {

    }
}
