package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;

public class Replica extends AbstractReplica {
    private final int[] locations = new int[AbstractReplica.POSITIONS_LIST_LENGTH]; // the stable values
    private final Map<Integer, ActorRef> replicasGroup;
    private int coordinator_id;

    private EpochSeqNum lastUpdate; // the epoch and seqNum of the last Update Req
    private LinkedHashMap<EpochSeqNum, LocationUpdate> updatesHistory;

    private Crash.Type crashOn;
    private int crashBudget;

    private Cancellable heartbeatTimer; // coordinator: periodic HeartbeatTick to self
    private Cancellable coordinatorTimeout; // replica: fires if the coordinator stays silent

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        this.replicasGroup = new HashMap<>();
        this.coordinator_id = this.id;
        this.lastUpdate = new EpochSeqNum(0, 0);

        this.updatesHistory = new LinkedHashMap<EpochSeqNum, LocationUpdate>();
        this.updatesHistory.put(lastUpdate, new LocationUpdate(0, 0, ActorRef.noSender(), ActorRef.noSender()));
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    void broadcast(Serializable msg) {
        for(ActorRef replica : this.replicasGroup.values())
            if(!replica.equals(this.getSelf())){
                tell(msg, replica);
            }
    }

    public int getHeartbeatTimeout() {
        return getCoordinatorBeatInterval() + getMaxLatencyPlusTolerance();
    }

    @Override
    public int getSystemNumberOfActors() {
        return replicasGroup.size();
    }

    private void clearReceive() {
        getContext().become(receiveBuilder().matchAny(m -> {}).build());
    }

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        // TODO: missing to be handled:
        // - Election
        switch (how_to_crash.type) {
            case Crash.Type.Now:
                clearReceive();
                break;

            default:
                crashOn = how_to_crash.type;
                crashBudget = how_to_crash.after_n_messages_of_type;
        }
    }

    @Override
    public void initSystem(InitSystem sysInit) {
        Logger.debug("Replica " + this.id + " is starting.");
        this.coordinator_id = sysInit.coordinator_id;
        this.replicasGroup.putAll(sysInit.group);

        if (this.id == this.coordinator_id) {
            heartbeatTimer = getContext().system().getScheduler().scheduleWithFixedDelay(
                    Duration.ZERO,
                    Duration.ofMillis(getCoordinatorBeatInterval()),
                    getSelf(),
                    new HeartbeatTick(),
                    getContext().system().dispatcher(),
                    ActorRef.noSender());
        } else {
            scheduleCoordinatorTimeout();
        }
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(ReadReqMsg.class, this::onReadReqMsg)
                .match(WriteReqMsg.class, this::onWriteReqMsg)
                .match(UpdateRequestMsg.class, this::onUpdateRequestMsg)
                .match(UpdateMsg.class, this::onUpdateMsg)
                .match(AckMsg.class, this::onAckMsg)
                .match(WriteOKMsg.class, this::onWriteOKMsg)
                .match(HeartbeatTick.class, this::onHeartbeatTick)
                .match(HeartbeatMsg.class, this::onHeartbeatMsg)
                .match(CoordinatorTimeout.class, this::onCoordinatorTimeout)
                .build();
    }


    /* Messages */

    /* Read Request from Client */
    public static class ReadReqMsg implements Serializable {
        int index;

        public ReadReqMsg(int index) {
            this.index = index;
        }

    }

    private void onReadReqMsg(ReadReqMsg msg) {
        /* Send the Value back to the Client */
        Logger.debug(getSelf().path().name() + " Received a Read Request from " + getSender().path().name());
        tell(new AbstractClient.ReadResult(true, msg.index, this.locations[msg.index], this.id), getSender());
    }

    /* Write Request from Client */
    public static class WriteReqMsg implements Serializable {
        int index;
        int value;

        public WriteReqMsg(int index, int value) {
            this.index = index;
            this.value = value;
        }

    }

    private void onWriteReqMsg(WriteReqMsg msg) {
        /* Send an Update Request to the coordinator */
        Logger.debug(getSelf().path().name() + " Received a Write Request from " + getSender().path().name());

        tell(new UpdateRequestMsg(msg.index, msg.value, getSender()), this.replicasGroup.get(coordinator_id));
    }

    /* Update Request From Replica */
    public static class UpdateRequestMsg implements Serializable{
        int index;
        int value;
        ActorRef clientRequesting; // to know which is the client to send the msg of WriteResult to

        public UpdateRequestMsg(int index, int value, ActorRef clientRequesting) {
            this.index = index;
            this.value = value;
            this.clientRequesting = clientRequesting;
        }

        public UpdateRequestMsg(UpdateRequestMsg other) {
            this.index = other.index;
            this.value = other.value;
            this.clientRequesting = other.clientRequesting;
        }
    }

    private void onUpdateRequestMsg(UpdateRequestMsg msg) {
        if (crashOn == Crash.Type.Update && --crashBudget <= 0)
            clearReceive();

        // this is received only by the coordinator
        Logger.debug(getSelf().path().name() + " Received an Update Request from " + getSender().path().name());

        // Coordinator Receives the Update Msg from a Replica
        this.lastUpdate = this.lastUpdate.incrementSeqNum();
        LocationUpdate currentUpdate = new LocationUpdate(msg.index, msg.value, msg.clientRequesting, getSender());
        currentUpdate.setUnstable();
        currentUpdate.increaseQuorum();
        updatesHistory.put(this.lastUpdate, currentUpdate);

        broadcast(new Replica.UpdateMsg(new EpochSeqNum(this.lastUpdate), currentUpdate));
    }

    public static class UpdateMsg implements Serializable {
        EpochSeqNum epochSeqNum;
        LocationUpdate currentUpdate;

        public UpdateMsg(EpochSeqNum epochSeqNum, LocationUpdate currentUpdate) {
            this.epochSeqNum = epochSeqNum;
            this.currentUpdate = currentUpdate;
        }
    }

    private void onUpdateMsg(UpdateMsg msg) {
        if (crashOn == Crash.Type.Update && --crashBudget <= 0)
            clearReceive();

        // Message received by Replica from Coordinator
        Logger.debug(getSelf().path().name() + " Received an Update Msg from " + getSender().path().name());
        this.lastUpdate = new EpochSeqNum(msg.epochSeqNum);
        updatesHistory.put(this.lastUpdate, new LocationUpdate(msg.currentUpdate));
        tell(new AckMsg(this.lastUpdate), getSender());

    }

    public static class AckMsg implements Serializable {
        EpochSeqNum epochSeqNum;

        public AckMsg(EpochSeqNum epochSeqNum) {
            this.epochSeqNum = epochSeqNum;
        }
    }

    private void onAckMsg(AckMsg msg) {
        // Received by the Coordinator from Replicas
        Logger.debug(getSelf().path().name() + " Received an ACK Msg from " + getSender().path().name() + " for update " + msg.epochSeqNum.toString());
        LocationUpdate currentUpdate = updatesHistory.get(msg.epochSeqNum);
        currentUpdate.quorum++;
        LocationUpdate localUpdate = updatesHistory.get(msg.epochSeqNum); // to manage the ACK msg after the WriteOK is broadcasted

        if (currentUpdate.quorum < getSystemNumberOfActors() / 2 + 1) {
            // add the updated quorum and do nothing
            updatesHistory.put(msg.epochSeqNum, currentUpdate);
        } else if (!localUpdate.isStable()) { // the if is to skip the ACK msg after the update is set to stable already
            // 1. Set the update in history to Stable
            // 2. Write the update to the locations array
            Logger.debug(getSelf().path().name() + " set " + msg.epochSeqNum + " to stable.");
            currentUpdate.setStable();
            updatesHistory.put(msg.epochSeqNum, currentUpdate);
            locations[currentUpdate.index] = currentUpdate.value;
            callbackOnUpdateApplied(currentUpdate.index, currentUpdate.value);
            broadcast(new Replica.WriteOKMsg(msg.epochSeqNum));
            if (getSelf() == currentUpdate.replicaRequested) {
                tell(new AbstractClient.WriteResult(true, currentUpdate.index, currentUpdate.value, this.id), currentUpdate.clientRequesting);
            }
        }
    }

    public static class WriteOKMsg implements Serializable {
        EpochSeqNum epochSeqNum;

        public WriteOKMsg(EpochSeqNum epochSeqNum) {
            this.epochSeqNum = epochSeqNum;

        }
    }

    private void onWriteOKMsg(WriteOKMsg msg) {
        if (crashOn == Crash.Type.WriteOK && --crashBudget <= 0)
            clearReceive();

        // Received by the Replicas
        Logger.debug(getSelf().path().name() + " Received a WriteOK from " + getSender().path().name());

        LocationUpdate currentUpdate = updatesHistory.get(msg.epochSeqNum);
        currentUpdate.setStable();
        updatesHistory.put(msg.epochSeqNum, currentUpdate);
        // Logger.debug(getSelf().path().name() + " Write " + currentUpdate.value + " to locations with index " + currentUpdate.index);
        locations[currentUpdate.index] = currentUpdate.value;
        callbackOnUpdateApplied(currentUpdate.index, currentUpdate.value);
        if (getSelf() == currentUpdate.replicaRequested) {
            tell(new AbstractClient.WriteResult(true, currentUpdate.index, currentUpdate.value, this.id), currentUpdate.clientRequesting);
        }

    }

    /* Self-scheduled: time for the coordinator to send a heartbeat */
    public static class HeartbeatTick implements Serializable {
    }

    private void onHeartbeatTick(HeartbeatTick msg) {
        if (crashOn == Crash.Type.Heartbeat && --crashBudget <= 0)
            clearReceive();

        broadcast(new HeartbeatMsg(this.lastUpdate.epoch));
    }

    /* Heartbeat from Coordinator to Replicas */
    public static class HeartbeatMsg implements Serializable {
        int epoch;

        public HeartbeatMsg(int epoch) {
            this.epoch = epoch;
        }
    }

    private void onHeartbeatMsg(HeartbeatMsg msg) {
        if (msg.epoch < this.lastUpdate.epoch) return;

        if (crashOn == Crash.Type.Heartbeat && --crashBudget <= 0)
            clearReceive();

        // null until initSystem: ignore beats that overtake InitSystem
        if (this.coordinatorTimeout != null) {
            this.coordinatorTimeout.cancel();
            this.scheduleCoordinatorTimeout();
        }
    }

    /* Self-scheduled: no heartbeat from the coordinator within the timeout */
    public static class CoordinatorTimeout implements Serializable {
    }

    private void onCoordinatorTimeout(CoordinatorTimeout msg) {
        // TODO: Coordinator crashed, run election protocol.
    }

    private void scheduleCoordinatorTimeout() {
        this.coordinatorTimeout = getContext().system().getScheduler().scheduleOnce(
                Duration.ofMillis(getHeartbeatTimeout()),
                getSelf(),
                new CoordinatorTimeout(),
                getContext().system().dispatcher(),
                ActorRef.noSender());
    }
}
