package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds.Messages.*;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;

public class Replica extends AbstractReplica {
    private final int[] locations = new int[AbstractReplica.POSITIONS_LIST_LENGTH]; // the stable values
    private final Map<Integer, ActorRef> replicasGroup;
    private int coordinator_id;

    private EpochSeqNum lastUpdate; // the epoch and seqNum of the last Update Req
    private LinkedHashMap<EpochSeqNum, LocationUpdate> updatesHistory;

    private final CrashTrigger crashTrigger = new CrashTrigger();

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

    /* Call after processing a message of type t */
    private void crashIf(Crash.Type t) {
        if (crashTrigger.tick(t))
            clearReceive();
    }

    private Cancellable schedule(Serializable msg, long delayMillis) {
        return getContext().system().getScheduler().scheduleOnce(
                Duration.ofMillis(delayMillis),
                getSelf(),
                msg,
                getContext().system().dispatcher(),
                ActorRef.noSender());
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
                crashTrigger.arm(how_to_crash);
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
            coordinatorTimeout = schedule(new CoordinatorTimeout(), getHeartbeatTimeout());
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


    /* Messages: see Messages.java */

    private void onReadReqMsg(ReadReqMsg msg) {
        /* Send the Value back to the Client */
        Logger.debug(getSelf().path().name() + " Received a Read Request from " + getSender().path().name());
        tell(new AbstractClient.ReadResult(true, msg.index, this.locations[msg.index], this.id), getSender());
    }

    private void onWriteReqMsg(WriteReqMsg msg) {
        /* Send an Update Request to the coordinator */
        Logger.debug(getSelf().path().name() + " Received a Write Request from " + getSender().path().name());

        tell(new UpdateRequestMsg(msg.index, msg.value, getSender()), this.replicasGroup.get(coordinator_id));
    }

    private void onUpdateRequestMsg(UpdateRequestMsg msg) {
        // this is received only by the coordinator
        Logger.debug(getSelf().path().name() + " Received an Update Request from " + getSender().path().name());

        // Coordinator Receives the Update Msg from a Replica
        this.lastUpdate = this.lastUpdate.incrementSeqNum();
        LocationUpdate currentUpdate = new LocationUpdate(msg.index, msg.value, msg.clientRequesting, getSender());
        currentUpdate.setUnstable();
        currentUpdate.increaseQuorum();
        updatesHistory.put(this.lastUpdate, currentUpdate);

        broadcast(new UpdateMsg(this.lastUpdate, msg.index, msg.value, msg.clientRequesting, getSender()));
        crashIf(Crash.Type.Update);
    }

    private void onUpdateMsg(UpdateMsg msg) {
        // Message received by Replica from Coordinator
        Logger.debug(getSelf().path().name() + " Received an Update Msg from " + getSender().path().name());
        this.lastUpdate = msg.epochSeqNum;
        LocationUpdate update = new LocationUpdate(msg.index, msg.value, msg.clientRequesting, msg.replicaRequested);
        update.setUnstable();
        updatesHistory.put(this.lastUpdate, update);
        tell(new AckMsg(this.lastUpdate), getSender());
        crashIf(Crash.Type.Update);

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
            broadcast(new WriteOKMsg(msg.epochSeqNum));
            if (getSelf() == currentUpdate.replicaRequested) {
                tell(new AbstractClient.WriteResult(true, currentUpdate.index, currentUpdate.value, this.id), currentUpdate.clientRequesting);
            }
        }
    }

    private void onWriteOKMsg(WriteOKMsg msg) {
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
        crashIf(Crash.Type.WriteOK);

    }

    /* Self-scheduled: time for the coordinator to send a heartbeat */
    private static final class HeartbeatTick implements Serializable {
    }

    private void onHeartbeatTick(HeartbeatTick msg) {
        broadcast(new HeartbeatMsg(this.lastUpdate.epoch));
        crashIf(Crash.Type.Heartbeat);
    }

    private void onHeartbeatMsg(HeartbeatMsg msg) {
        if (msg.epoch < this.lastUpdate.epoch) return;
        // null until initSystem: ignore beats that overtake InitSystem
        if (this.coordinatorTimeout != null) {
            this.coordinatorTimeout.cancel();
            this.coordinatorTimeout = schedule(new CoordinatorTimeout(), getHeartbeatTimeout());
        }
        crashIf(Crash.Type.Heartbeat);
    }

    /* Self-scheduled: no heartbeat from the coordinator within the timeout */
    private static final class CoordinatorTimeout implements Serializable {
    }

    private void onCoordinatorTimeout(CoordinatorTimeout msg) {
        // TODO: Coordinator crashed, run election protocol.
    }
}
