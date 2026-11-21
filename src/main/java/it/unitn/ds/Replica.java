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

    // =================================================================================
    // System API
    // =================================================================================

    @Override
    public int getSystemNumberOfActors() {
        return replicasGroup.size();
    }

    @Override
    public void initSystem(InitSystem sysInit) {
        debug("starting");
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

    // =================================================================================
    // Network and timers
    // =================================================================================

    void broadcast(Serializable msg) {
        for (ActorRef replica : this.replicasGroup.values())
            if (!replica.equals(getSelf()))
                tell(msg, replica);
    }

    private Cancellable schedule(Serializable msg, long delayMillis) {
        return getContext().system().getScheduler().scheduleOnce(
                Duration.ofMillis(delayMillis),
                getSelf(),
                msg,
                getContext().system().dispatcher(),
                ActorRef.noSender());
    }

    // =================================================================================
    // Crash emulation
    // =================================================================================

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        // TODO: missing to be handled:
        // - Election
        switch (how_to_crash.type) {
            case Crash.Type.Now:
                clearReceive(); // AbstractReplica logs CRASHED right after this
                break;

            default:
                crashTrigger.arm(how_to_crash);
        }
    }

    private void clearReceive() {
        getContext().become(receiveBuilder().matchAny(m -> {}).build());
    }

    /* Call after processing a message of type t */
    private void crashIf(Crash.Type t) {
        if (crashTrigger.tick(t)) {
            clearReceive();
            log("CRASHED");
        }
    }

    // =================================================================================
    // Client requests
    // =================================================================================

    private void onReadReqMsg(ReadReqMsg msg) {
        debug("received READ (" + msg.index + ") from " + getSender().path().name());
        tell(new AbstractClient.ReadResult(true, msg.index, this.locations[msg.index], this.id), getSender());
    }

    private void onWriteReqMsg(WriteReqMsg msg) {
        debug("received WRITE (" + msg.index + ", " + msg.value + ") from " + getSender().path().name());
        tell(new UpdateRequestMsg(msg.index, msg.value, getSender()), this.replicasGroup.get(coordinator_id));
    }

    // =================================================================================
    // Two-phase update
    // =================================================================================

    /* Coordinator: start the two-phase broadcast for a forwarded write */
    private void onUpdateRequestMsg(UpdateRequestMsg msg) {
        debug("received UPDATE REQUEST from " + getSender().path().name());

        this.lastUpdate = this.lastUpdate.incrementSeqNum();
        LocationUpdate update = new LocationUpdate(msg.index, msg.value, msg.clientRequesting, getSender());
        update.setUnstable();
        update.increaseQuorum(); // the coordinator is part of the quorum
        updatesHistory.put(this.lastUpdate, update);

        broadcast(new UpdateMsg(this.lastUpdate, msg.index, msg.value, msg.clientRequesting, getSender()));
        crashIf(Crash.Type.Update);
    }

    /* Replica: store the pending update and ack it */
    private void onUpdateMsg(UpdateMsg msg) {
        debug("received UPDATE " + msg.epochSeqNum + " from " + getSender().path().name());

        this.lastUpdate = msg.epochSeqNum;
        LocationUpdate update = new LocationUpdate(msg.index, msg.value, msg.clientRequesting, msg.replicaRequested);
        update.setUnstable();
        updatesHistory.put(this.lastUpdate, update);

        tell(new AckMsg(this.lastUpdate), getSender());
        crashIf(Crash.Type.Update);
    }

    /* Coordinator: commit once a quorum acked; later acks are ignored */
    private void onAckMsg(AckMsg msg) {
        debug("received ACK " + msg.epochSeqNum + " from " + getSender().path().name());

        LocationUpdate update = updatesHistory.get(msg.epochSeqNum);
        update.increaseQuorum();
        if (update.quorum < getSystemNumberOfActors() / 2 + 1 || update.isStable())
            return;

        applyUpdate(msg.epochSeqNum, update);
        broadcast(new WriteOKMsg(msg.epochSeqNum));
    }

    /* Replica: commit the update */
    private void onWriteOKMsg(WriteOKMsg msg) {
        debug("received WRITEOK " + msg.epochSeqNum + " from " + getSender().path().name());

        applyUpdate(msg.epochSeqNum, updatesHistory.get(msg.epochSeqNum));
        crashIf(Crash.Type.WriteOK);
    }

    private void applyUpdate(EpochSeqNum key, LocationUpdate update) {
        update.setStable();
        locations[update.index] = update.value;
        log("applied update " + key.epoch + ":" + key.seqNum + " (" + update.index + ", " + update.value + ")");
        callbackOnUpdateApplied(update.index, update.value);

        // only the replica contacted by the client replies to it
        if (getSelf().equals(update.replicaRequested))
            tell(new AbstractClient.WriteResult(true, update.index, update.value, this.id), update.clientRequesting);
    }

    // =================================================================================
    // Failure detection
    // =================================================================================

    public int getHeartbeatTimeout() {
        return getCoordinatorBeatInterval() + getMaxLatencyPlusTolerance();
    }

    /* Self-scheduled: time for the coordinator to send a heartbeat */
    private static final class HeartbeatTick implements Serializable {
    }

    private void onHeartbeatTick(HeartbeatTick msg) {
        broadcast(new HeartbeatMsg(this.lastUpdate.epoch));
        crashIf(Crash.Type.Heartbeat);
    }

    private void onHeartbeatMsg(HeartbeatMsg msg) {
        if (msg.epoch < this.lastUpdate.epoch)
            return;

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
