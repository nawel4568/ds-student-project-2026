package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds.Messages.*;
import java.io.Serializable;
import java.time.Duration;
import java.util.*;

public class Replica extends AbstractReplica {

    // =================================================================================
    // State
    // =================================================================================

    private final int[] locations = new int[AbstractReplica.POSITIONS_LIST_LENGTH]; // the stable values
    private final Map<Integer, ActorRef> replicasGroup;
    private int coordinator_id;
    private int successor;

    private EpochSeqNum lastUpdate; // the epoch and seqNum of the last Update Req
    private final TreeMap<EpochSeqNum, UpdateEntry> updatesHistory = new TreeMap<>(); // sorted by <e,i>
    // Updates not applied yet -> acks received (only the coordinator counts them)
    private final Map<EpochSeqNum, Integer> unstable = new HashMap<>();

    // coordinator: periodic HeartbeatTick to self
    // replica: fires if the coordinator stays silent
    private Cancellable heartbeatTimer;
    // Forwarded (or buffered mid-election) writes whose UpdateMsg hasn't arrived yet, with their UPDATE timer
    private final Map<WriteId, PendingWrite> pendingWrites = new LinkedHashMap<>();
    // WRITEOK timers, keyed by the update id
    private final Map<EpochSeqNum, Cancellable> writeOKTimers = new HashMap<>();

    private final CrashTrigger crashTrigger = new CrashTrigger();

    private enum ReplicaMode {
        NORMAL,
        COORDINATOR,
        ELECTION
    };

    private ReplicaMode mode = ReplicaMode.NORMAL;

    // =================================================================================
    // Self-scheduled messages
    // =================================================================================

    /* Time for the coordinator to send a heartbeat */
    private static final class HeartbeatTick implements Serializable {}

    /* No heartbeat from the coordinator within the timeout */
    private static final class CoordinatorTimeout implements Serializable {}

    /* A client write. Also the self-message sent when the coordinator did not broadcast its UpdateMsg in time */
    private record WriteId(ActorRef client, int reqId) implements Serializable {}

    /* timer is null while the write is buffered during an election */
    private record PendingWrite(UpdateRequestMsg req, Cancellable timer) {}

    /* The coordinator did not send the WriteOKMsg for an update this replica acked */
    private record WriteOKTimeout(EpochSeqNum epochSeqNum) implements Serializable {}

    /* The coordinator did not receive an ACK for the ElectionMsg it sent */
    private record ACKElectionTimeout(int replicaId) implements Serializable {}

    private static final class ElectionTimeout implements Serializable {}

    // =================================================================================
    // Construction
    // =================================================================================

    public Replica(int id) {
        this(
                id,
                AbstractReplica.MIN_LATENCY,
                AbstractReplica.MAX_LATENCY,
                AbstractReplica.COORDINATOR_BEAT_INTERVAL,
                Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        this.replicasGroup = new HashMap<>();
        this.coordinator_id = this.id;
        this.lastUpdate = new EpochSeqNum(0, 0);
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(
                Replica.class,
                () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(
            int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(
                Replica.class,
                () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
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
        this.successor = (this.id + 1) % this.replicasGroup.size();

        if (this.id == this.coordinator_id) {
            setBehavior(ReplicaMode.COORDINATOR);
            startHeartbeatTimer();
        } else {
            this.heartbeatTimer = schedule(new CoordinatorTimeout(), getHeartbeatTimeout());
        }
    }

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        switch (how_to_crash.type) {
            case Crash.Type.Now:
                clearReceive(); // AbstractReplica logs CRASHED right after this
                break;

            default:
                crashTrigger.arm(how_to_crash);
        }
    }

    // =================================================================================
    // Behaviors
    // =================================================================================

    /* Default: normal replica behavior. */
    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(ReadReqMsg.class, this::onReadReqMsg)
                .match(WriteReqMsg.class, this::onWriteReqMsg)
                .match(UpdateMsg.class, this::onUpdateMsg)
                .match(WriteOKMsg.class, this::onWriteOKMsg)
                .match(HeartbeatMsg.class, this::onHeartbeatMsg)
                .match(CoordinatorTimeout.class, this::onCoordinatorTimeout)
                .match(WriteId.class, this::onUpdateTimeout)
                .match(WriteOKTimeout.class, this::onWriteOKTimeout)
                .match(ElectionMsg.class, this::onElectionMsg)
                .build();
    }

    public final Receive coordinatorBehavior() {
        return createBaseReceiveBuilder()
                .match(ReadReqMsg.class, this::onReadReqMsg)
                .match(WriteReqMsg.class, this::onWriteReqMsg)
                .match(UpdateRequestMsg.class, this::onUpdateRequestMsg)
                .match(UpdateMsg.class, this::onUpdateMsg)
                .match(AckMsg.class, this::onAckMsg)
                .match(WriteOKMsg.class, this::onWriteOKMsg)
                .match(HeartbeatTick.class, this::onHeartbeatTick)
                .match(ElectionMsg.class, this::onRedundantElectionMsg)
                .build();
    }

    public final Receive electionBehavior() {
        return createBaseReceiveBuilder()
                .match(ReadReqMsg.class, this::onReadReqMsg)
                .match(ElectionMsg.class, this::onElectionMsg)
                .match(SyncMsg.class, this::onSyncMsg)
                .match(ACKElectionMsg.class, this::onACKElectionMsg)
                .match(ACKElectionTimeout.class, this::onACKElectionTimeout)
                .match(ElectionTimeout.class, this::onElectionTimeout)
                .match(WriteReqMsg.class, this::bufferWriteMsg)
                .build();
    }

    public final void setBehavior(ReplicaMode newMode) {
        this.mode = newMode;
        switch (newMode) {
            case NORMAL:
                getContext().become(createReceive());
                break;
            case COORDINATOR:
                getContext().become(coordinatorBehavior());
                break;
            case ELECTION:
                getContext().become(electionBehavior());
                break;
        }
    }

    // =================================================================================
    // Network, timers and crash emulation
    // =================================================================================

    void broadcast(Serializable msg, Crash.Type crashType) {
        for (ActorRef replica : this.replicasGroup.values()) {
            if (replica.equals(getSelf())) continue;
            tell(msg, replica);

            if (crashIf(crashType)) return; // stop mid-broadcast
        }
    }

    private Cancellable schedule(Serializable msg, long delayMillis) {
        return getContext()
                .system()
                .getScheduler()
                .scheduleOnce(
                        Duration.ofMillis(delayMillis),
                        getSelf(),
                        msg,
                        getContext().system().dispatcher(),
                        ActorRef.noSender());
    }

    private static void cancel(Cancellable timer) {
        if (timer != null) timer.cancel();
    }

    public int getHeartbeatTimeout() {
        return getCoordinatorBeatInterval() + getMaxLatencyPlusTolerance();
    }

    /*
     * Wait for UpdateMsg / WriteOKMsg: two hops (to the coordinator and back), each allowed twice the
     * per-hop tolerance because it can queue behind a burst of writes on the same FIFO channel.
     * Must stay below getHeartbeatTimeout(), or the heartbeat always detects the crash first.
     */
    private int getRequestTimeout() {
        return 4 * getMaxLatencyPlusTolerance();
    }

    private void cancelTimers() {
        cancel(heartbeatTimer);
        pendingWrites.values().forEach(p -> cancel(p.timer())); // keep the writes: re-forwarded after the election
        writeOKTimers.values().forEach(Cancellable::cancel);
        writeOKTimers.clear();
    }

    private void clearReceive() {
        getContext().become(receiveBuilder().matchAny(m -> {}).build());
    }

    /* Call after processing a message of type t */
    private boolean crashIf(Crash.Type t) {
        if (!crashTrigger.tick(t)) return false;
        clearReceive();
        log("CRASHED");
        return true;
    }

    // =================================================================================
    // Client requests
    // =================================================================================

    private void forwardWrite(UpdateRequestMsg req) {
        tell(req, replicasGroup.get(coordinator_id));
        if (this.id == this.coordinator_id) return; // the coordinator can't time out on itself

        WriteId key = new WriteId(req.clientRequesting, req.reqId);
        pendingWrites.put(key, new PendingWrite(req, schedule(key, getRequestTimeout())));
    }

    private void onReadReqMsg(ReadReqMsg msg) {
        debug("received READ (" + msg.index + ") from " + getSender().path().name());

        if (msg.index < 0 || msg.index >= POSITIONS_LIST_LENGTH) {
            tell(new ReadReplyMsg(msg.reqId, new AbstractClient.ReadResult(false, msg.index, 0, this.id)), getSender());
            return;
        }

        tell(
                new ReadReplyMsg(
                        msg.reqId, new AbstractClient.ReadResult(true, msg.index, this.locations[msg.index], this.id)),
                getSender());
    }

    private void onWriteReqMsg(WriteReqMsg msg) {
        debug("received WRITE (" + msg.index + ", " + msg.value + ") from "
                + getSender().path().name());

        if (msg.index < 0 || msg.index >= POSITIONS_LIST_LENGTH) {
            tell(
                    new WriteReplyMsg(msg.reqId, new AbstractClient.WriteResult(false, msg.index, msg.value, this.id)),
                    getSender());
            return;
        }

        forwardWrite(new UpdateRequestMsg(msg.index, msg.value, msg.reqId, getSender()));
    }

    // =================================================================================
    // Two-phase update
    // =================================================================================

    /* Coordinator: start the two-phase broadcast for a forwarded write */
    private void onUpdateRequestMsg(UpdateRequestMsg msg) {
        debug("received UPDATE REQUEST from " + getSender().path().name());

        this.lastUpdate = this.lastUpdate.incrementSeqNum();
        UpdateEntry update = new UpdateEntry(msg.index, msg.value, msg.reqId, msg.clientRequesting, getSender());
        updatesHistory.put(this.lastUpdate, update);
        unstable.put(this.lastUpdate, 1); // the coordinator is part of the quorum

        broadcast(new UpdateMsg(this.lastUpdate, update), Crash.Type.Update);
    }

    /* Replica: store the pending update and ack it */
    private void onUpdateMsg(UpdateMsg msg) {
        debug("received UPDATE " + msg.epochSeqNum + " from "
                + getSender().path().name());

        dropPendingWrite(msg.update);

        this.lastUpdate = msg.epochSeqNum;
        updatesHistory.put(this.lastUpdate, msg.update);
        unstable.put(this.lastUpdate, 0);

        tell(new AckMsg(this.lastUpdate), getSender());
        writeOKTimers.put(this.lastUpdate, schedule(new WriteOKTimeout(this.lastUpdate), getRequestTimeout()));
        crashIf(Crash.Type.Update);
    }

    /* Coordinator: commit once a quorum acked; later acks are ignored */
    private void onAckMsg(AckMsg msg) {
        debug("received ACK " + msg.epochSeqNum + " from " + getSender().path().name());

        // null once the update is applied: later acks are ignored
        Integer acks = unstable.computeIfPresent(msg.epochSeqNum, (k, n) -> n + 1);
        if (acks == null || acks < getSystemNumberOfActors() / 2 + 1) return;

        applyUpdate(msg.epochSeqNum, updatesHistory.get(msg.epochSeqNum));
        broadcast(new WriteOKMsg(msg.epochSeqNum), Crash.Type.WriteOK);
    }

    /* Replica: commit the update */
    private void onWriteOKMsg(WriteOKMsg msg) {
        debug("received WRITEOK " + msg.epochSeqNum + " from "
                + getSender().path().name());
        cancel(writeOKTimers.remove(msg.epochSeqNum));

        applyUpdate(msg.epochSeqNum, updatesHistory.get(msg.epochSeqNum));
        crashIf(Crash.Type.WriteOK);
    }

    private void applyUpdate(EpochSeqNum key, UpdateEntry update) {
        unstable.remove(key);
        locations[update.index] = update.value;
        log("applied update " + key.epoch() + ":" + key.seqNum() + " (" + update.index + ", " + update.value + ")");
        callbackOnUpdateApplied(update.index, update.value);

        // only the replica contacted by the client replies to it
        if (getSelf().equals(update.replicaRequested))
            tell(
                    new WriteReplyMsg(
                            update.reqId, new AbstractClient.WriteResult(true, update.index, update.value, this.id)),
                    update.clientRequesting);
    }

    private void deliverUpdate(EpochSeqNum id, UpdateEntry u) {
        updatesHistory.put(id, u);
        applyUpdate(id, u);
        dropPendingWrite(u);
    }

    /* The write has an UpdateMsg now: stop waiting for it */
    private void dropPendingWrite(UpdateEntry u) {
        PendingWrite p = pendingWrites.remove(new WriteId(u.clientRequesting, u.reqId));
        if (p != null) cancel(p.timer());
    }

    // =================================================================================
    // Heartbeat
    // =================================================================================

    /* Coordinator: start ticking */
    private void startHeartbeatTimer() {
        cancel(this.heartbeatTimer);

        this.heartbeatTimer = getContext()
                .system()
                .getScheduler()
                .scheduleWithFixedDelay(
                        Duration.ZERO,
                        Duration.ofMillis(getCoordinatorBeatInterval()),
                        getSelf(),
                        new HeartbeatTick(),
                        getContext().system().dispatcher(),
                        ActorRef.noSender());
    }

    /* Coordinator */
    private void onHeartbeatTick(HeartbeatTick msg) {
        broadcast(new HeartbeatMsg(this.lastUpdate.epoch()), Crash.Type.Heartbeat);
    }

    /* Replica: restart the coordinator timeout */
    private void onHeartbeatMsg(HeartbeatMsg msg) {
        if (msg.epoch < this.lastUpdate.epoch()) return;

        cancel(this.heartbeatTimer);
        this.heartbeatTimer = schedule(new CoordinatorTimeout(), getHeartbeatTimeout());
        crashIf(Crash.Type.Heartbeat);
    }

    // =================================================================================
    // Failure detection
    // =================================================================================

    private void onCoordinatorTimeout(CoordinatorTimeout msg) {
        startElection();
    }

    // not pending: the UpdateMsg arrived after the timer had already fired
    private void onUpdateTimeout(WriteId msg) {
        if (!pendingWrites.containsKey(msg)) return;
        log("UPDATE timeout for request " + msg.reqId() + " of "
                + msg.client().path().name());
        startElection();
    }

    private void onWriteOKTimeout(WriteOKTimeout msg) {
        if (writeOKTimers.remove(msg.epochSeqNum()) == null) return;
        log("WRITEOK timeout for " + msg.epochSeqNum());
        startElection();
    }

    // =================================================================================
    // Coordinator election
    // =================================================================================

    private Cancellable electionAckTimer;
    private Cancellable electionTimer;

    private void setElectionAckTimer(int replicaId) {
        cancel(this.electionAckTimer);
        this.electionAckTimer = schedule(new ACKElectionTimeout(replicaId), getRequestTimeout());
    }

    private void setElectionTimer() {
        cancel(this.electionTimer);
        this.electionTimer = schedule(new ElectionTimeout(), this.replicasGroup.size() * getRequestTimeout());
    }

    private void startElection() {
        enterElection();
        sendFreshToken();
        crashIf(Crash.Type.Election);
    }

    private void enterElection() {
        callbackOnElectionStarted(this.coordinator_id);
        setBehavior(ReplicaMode.ELECTION);
        setElectionTimer();
        cancelTimers();
    }

    /* Start a new token holding only this replica's last update, and send it to the successor */
    private void sendFreshToken() {
        tell(
                new ElectionMsg(new TreeMap<>(Map.of(this.id, this.lastUpdate)), this.id, this.lastUpdate.epoch()),
                this.replicasGroup.get(this.successor));
        setElectionAckTimer(this.successor);
    }

    private void onElectionMsg(ElectionMsg msg) {
        tell(new ACKElectionMsg(), getSender());
        if (msg.epoch < this.lastUpdate.epoch()) return;

        if (this.mode != ReplicaMode.ELECTION) enterElection();

        if (msg.visitedReplicas.containsKey(this.id)) {
            if (msg.candidateID == this.id) {
                // This is the new coordinator: new epoch, finish pending update, send SYNCHRONIZATION
                this.becomeCoordinator(msg.visitedReplicas);
            } else {
                // forward ELECTION to the candidate replica
                tell(msg, this.replicasGroup.get(msg.candidateID));
                setElectionAckTimer(msg.candidateID);
            }
        } else {
            TreeMap<Integer, EpochSeqNum> visitedReplicas = new TreeMap<>(msg.visitedReplicas);
            visitedReplicas.put(this.id, this.lastUpdate);
            // the most recent update wins; ties go to the lower id
            int c = this.lastUpdate.compareTo(msg.getCandidateEpochSeq());
            int newCandidateID = (c > 0 || (c == 0 && this.id < msg.candidateID)) ? this.id : msg.candidateID;

            tell(new ElectionMsg(visitedReplicas, newCandidateID, msg.epoch), this.replicasGroup.get(this.successor));
            setElectionAckTimer(this.successor);
        }
        crashIf(Crash.Type.Election);
    }

    private void onRedundantElectionMsg(ElectionMsg msg) {
        tell(new ACKElectionMsg(), getSender());
    }

    private void onACKElectionMsg(ACKElectionMsg msg) {
        cancel(this.electionAckTimer);
    }

    private void onElectionTimeout(ElectionTimeout msg) {
        startElection();
    }

    private void onACKElectionTimeout(ACKElectionTimeout msg) {
        log("ELECTIONACK timeout");
        if (msg.replicaId() == this.successor) {
            this.successor = (this.successor + 1) % this.replicasGroup.size();
        }
        sendFreshToken();
    }

    private void becomeCoordinator(SortedMap<Integer, EpochSeqNum> electionUpdateList) {
        callbackOnCoordinatorElected(this.id);
        // Set stable all pending updates, Become Coordinator, Send a sync Msg to the replicas announcing new
        // coordinator
        // and provide the other nodes with all the missing updates

        // 1. Finish the local pending updates if any, in <e,i> order
        new TreeSet<>(unstable.keySet()).forEach(k -> deliverUpdate(k, updatesHistory.get(k)));

        // 2. Set a new epoch = epoch of the last update + 1 & the sequence number restarts at 0
        this.coordinator_id = this.id;
        this.lastUpdate = this.lastUpdate.incrementEpoch();
        int newEpoch = this.lastUpdate.epoch();

        // 3. Send each replica only what it is missing, starting from its last update
        for (Map.Entry<Integer, EpochSeqNum> eu : electionUpdateList.entrySet()) {
            if (eu.getKey() == this.id) continue; // skip self
            tell(
                    new SyncMsg(newEpoch, this.id, updatesHistory.tailMap(eu.getValue(), true)),
                    this.replicasGroup.get(eu.getKey()));
        }

        setBehavior(ReplicaMode.COORDINATOR);
        startHeartbeatTimer();

        pendingWrites.values().forEach(p -> tell(p.req(), getSelf()));
        pendingWrites.clear();
    }

    private void onSyncMsg(SyncMsg msg) {
        // 1. Ignore stale or duplicate synchronizations
        if (msg.newEpoch <= this.lastUpdate.epoch()) return;

        callbackOnCoordinatorElected(msg.newCoordinatorId);

        cancel(this.electionAckTimer);
        cancel(this.electionTimer);

        TreeMap<EpochSeqNum, UpdateEntry> todo = new TreeMap<>(msg.missingUpdates);
        unstable.keySet().forEach(k -> todo.putIfAbsent(k, updatesHistory.get(k)));

        // 2. Deliver missing updates in <e,i> order (the map is sorted), without duplicates
        for (Map.Entry<EpochSeqNum, UpdateEntry> e : todo.entrySet()) {
            if (updatesHistory.containsKey(e.getKey()) && !unstable.containsKey(e.getKey()))
                continue; // already delivered
            deliverUpdate(e.getKey(), e.getValue());
        }

        // 3. Adopt the new epoch and coordinator
        this.coordinator_id = msg.newCoordinatorId;
        this.lastUpdate = new EpochSeqNum(msg.newEpoch, 0);
        new ArrayList<>(pendingWrites.values()).forEach(p -> forwardWrite(p.req()));

        // 4. Back to normal behavior
        setBehavior(ReplicaMode.NORMAL);
        this.heartbeatTimer = schedule(new CoordinatorTimeout(), getHeartbeatTimeout());
    }

    private void bufferWriteMsg(WriteReqMsg msg) {
        if (msg.index < 0 || msg.index >= POSITIONS_LIST_LENGTH) {
            tell(
                    new WriteReplyMsg(msg.reqId, new AbstractClient.WriteResult(false, msg.index, msg.value, this.id)),
                    getSender());
            return;
        }
        UpdateRequestMsg req = new UpdateRequestMsg(msg.index, msg.value, msg.reqId, getSender());
        this.pendingWrites.put(new WriteId(getSender(), msg.reqId), new PendingWrite(req, null));
    }
}
