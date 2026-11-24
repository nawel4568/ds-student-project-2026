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
    private ActorRef successor;

    private EpochSeqNum lastUpdate; // the epoch and seqNum of the last Update Req
    private final LinkedHashMap<EpochSeqNum, UpdateEntry> updatesHistory;
    // Updates not applied yet -> acks received (only the coordinator counts them)
    private final Map<EpochSeqNum, Integer> unstable = new HashMap<>();

    // coordinator: periodic HeartbeatTick to self
    // replica: fires if the coordinator stays silent
    private Cancellable heartbeatTimer;
    // Pending request timers; the timer message is the key for UPDATE, the update id for WRITEOK
    private final Map<UpdateTimeout, Cancellable> updateTimers = new HashMap<>();
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

    /* The coordinator did not broadcast the UpdateMsg for a write this replica forwarded */
    private record UpdateTimeout(ActorRef client, int reqId) implements Serializable {}

    /* The coordinator did not send the WriteOKMsg for an update this replica acked */
    private record WriteOKTimeout(EpochSeqNum epochSeqNum) implements Serializable {}

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
        this.successor = ActorRef.noSender();
        this.lastUpdate = new EpochSeqNum(0, 0);

        this.updatesHistory = new LinkedHashMap<EpochSeqNum, UpdateEntry>();
        this.updatesHistory.put(lastUpdate, new UpdateEntry(0, 0, 0, ActorRef.noSender(), ActorRef.noSender()));
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(
                Replica.class,
                () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(
            int id,
            int minLatency,
            int maxLatency,
            int coordinatorBeatInterval,
            ActorRef listener) {
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
        this.successor = this.replicasGroup.get((this.id + 1) % this.replicasGroup.size());

        if (this.id == this.coordinator_id) {
            setBehavior(ReplicaMode.COORDINATOR);
            startHeartbeatTimer();
        }
    }

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

    // =================================================================================
    // Behaviors
    // =================================================================================

    /* Default: normal replica behavior. */
    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder().match(ReadReqMsg.class, this::onReadReqMsg)
                .match(WriteReqMsg.class, this::onWriteReqMsg)
                .match(UpdateMsg.class, this::onUpdateMsg)
                .match(WriteOKMsg.class, this::onWriteOKMsg)
                .match(HeartbeatMsg.class, this::onHeartbeatMsg)
                .match(CoordinatorTimeout.class, this::onCoordinatorTimeout)
                .match(UpdateTimeout.class, this::onUpdateTimeout)
                .match(WriteOKTimeout.class, this::onWriteOKTimeout)
                .match(ElectionMsg.class, this::onElectionMsg)
                .build();
    }

    public final Receive coordinatorBehavior() {
        return createBaseReceiveBuilder().match(ReadReqMsg.class, this::onReadReqMsg)
                .match(WriteReqMsg.class, this::onWriteReqMsg)
                .match(UpdateRequestMsg.class, this::onUpdateRequestMsg)
                .match(UpdateMsg.class, this::onUpdateMsg)
                .match(AckMsg.class, this::onAckMsg)
                .match(WriteOKMsg.class, this::onWriteOKMsg)
                .match(HeartbeatTick.class, this::onHeartbeatTick)
                .build();
    }

    public final Receive electionBehavior() {
        return createBaseReceiveBuilder()
                // TODO: - handle another Election (best candidate crashes)
                .match(ReadReqMsg.class, this::onReadReqMsg)
                .match(ElectionMsg.class, this::onElectionMsg)
                .match(SyncMsg.class, this::onSyncMsg)
                .match(ACKElectionMsg.class, this::onACKElectionMsg)
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

    void broadcast(Serializable msg) {
        for (ActorRef replica : this.replicasGroup.values())
            if (!replica.equals(getSelf())) tell(msg, replica);
    }

    private Cancellable schedule(Serializable msg, long delayMillis) {
        return getContext().system()
                .getScheduler()
                .scheduleOnce(
                        Duration.ofMillis(delayMillis),
                        getSelf(),
                        msg,
                        getContext().system().dispatcher(),
                        ActorRef.noSender());
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
        if (heartbeatTimer != null) heartbeatTimer.cancel();
        updateTimers.values().forEach(Cancellable::cancel);
        updateTimers.clear();
        writeOKTimers.values().forEach(Cancellable::cancel);
        writeOKTimers.clear();
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
        tell(
                new ReadReplyMsg(
                        msg.reqId,
                        new AbstractClient.ReadResult(true, msg.index, this.locations[msg.index], this.id)),
                getSender());
    }

    private void onWriteReqMsg(WriteReqMsg msg) {
        debug("received WRITE (" + msg.index + ", " + msg.value + ") from " + getSender().path().name());
        tell(
                new UpdateRequestMsg(msg.index, msg.value, msg.reqId, getSender()),
                this.replicasGroup.get(coordinator_id));
        // the coordinator does not receive its own UpdateMsg, and cannot time out on itself
        if (this.id != this.coordinator_id) {
            UpdateTimeout timeout = new UpdateTimeout(getSender(), msg.reqId);
            updateTimers.put(timeout, schedule(timeout, getRequestTimeout()));
        }
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

        broadcast(new UpdateMsg(this.lastUpdate, msg.index, msg.value, msg.reqId, msg.clientRequesting, getSender()));
        crashIf(Crash.Type.Update);
    }

    /* Replica: store the pending update and ack it */
    private void onUpdateMsg(UpdateMsg msg) {
        debug("received UPDATE " + msg.epochSeqNum + " from " + getSender().path().name());

        if (getSelf().equals(msg.replicaRequested)) {
            Cancellable forwarded = updateTimers.remove(new UpdateTimeout(msg.clientRequesting, msg.reqId));
            if (forwarded != null) forwarded.cancel();
        }

        this.lastUpdate = msg.epochSeqNum;
        updatesHistory.put(
                this.lastUpdate,
                new UpdateEntry(msg.index, msg.value, msg.reqId, msg.clientRequesting, msg.replicaRequested));
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
        broadcast(new WriteOKMsg(msg.epochSeqNum));
    }

    /* Replica: commit the update */
    private void onWriteOKMsg(WriteOKMsg msg) {
        debug("received WRITEOK " + msg.epochSeqNum + " from " + getSender().path().name());
        Cancellable acked = writeOKTimers.remove(msg.epochSeqNum);
        if (acked != null) acked.cancel();

        applyUpdate(msg.epochSeqNum, updatesHistory.get(msg.epochSeqNum));
        crashIf(Crash.Type.WriteOK);
    }

    private void applyUpdate(EpochSeqNum key, UpdateEntry update) {
        unstable.remove(key);
        locations[update.index] = update.value;
        log("applied update " + key.epoch + ":" + key.seqNum + " (" + update.index + ", " + update.value + ")");
        callbackOnUpdateApplied(update.index, update.value);

        // only the replica contacted by the client replies to it
        if (getSelf().equals(update.replicaRequested)) tell(
                new WriteReplyMsg(
                        update.reqId,
                        new AbstractClient.WriteResult(true, update.index, update.value, this.id)),
                update.clientRequesting);
    }

    private void deliverUpdate(EpochSeqNum id, UpdateEntry u) {
        updatesHistory.put(id, u);
        applyUpdate(id, u);
    }

    // =================================================================================
    // Heartbeat
    // =================================================================================

    /* Coordinator: start ticking */
    private void startHeartbeatTimer() {
        if (this.heartbeatTimer != null && !this.heartbeatTimer.isCancelled()) this.heartbeatTimer.cancel();

        this.heartbeatTimer = getContext().system()
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
        broadcast(new HeartbeatMsg(this.lastUpdate.epoch));
        crashIf(Crash.Type.Heartbeat);
    }

    /* Replica: restart the coordinator timeout */
    private void onHeartbeatMsg(HeartbeatMsg msg) {
        if (msg.epoch < this.lastUpdate.epoch) return;

        // null before first heartbeat arrives.
        if (this.heartbeatTimer != null && !this.heartbeatTimer.isCancelled()) {
            this.heartbeatTimer.cancel();
        }
        this.heartbeatTimer = schedule(new CoordinatorTimeout(), getHeartbeatTimeout());
        crashIf(Crash.Type.Heartbeat);
    }

    // =================================================================================
    // Failure detection
    // =================================================================================

    private void onCoordinatorTimeout(CoordinatorTimeout msg) {
        startElection();
    }

    // remove() == null: the timer was cancelled after it had already fired
    private void onUpdateTimeout(UpdateTimeout msg) {
        if (updateTimers.remove(msg) == null) return;
        log("UPDATE timeout for request " + msg.reqId() + " of " + msg.client().path().name());
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

    private void startElection() {
        callbackOnElectionStarted(this.coordinator_id);
        // Starting the Election protocol the first time to timeout
        // case 1) if timeout is HEARTBEAT, UPDATE, WRITEOK
        setBehavior(ReplicaMode.ELECTION);
        cancelTimers();

        ElectionMsg newElectionMsg =
                new ElectionMsg(new TreeMap<>(Map.of(this.id, this.lastUpdate)), this.id, this.lastUpdate.epoch);

        // send the message (id, updateList) to the successor
        tell(newElectionMsg, this.successor);
        // TODO: set ACK timeout

        // case 2) if timeout is ELECTIONACK
        // TODO: compute the next successor and send the ElectionMsg
    }

    private void onElectionMsg(ElectionMsg msg) {
        tell(new ACKElectionMsg(), getSender());
        if (msg.epoch < this.lastUpdate.epoch) return;

        if (this.mode != ReplicaMode.ELECTION) {
            callbackOnElectionStarted(this.coordinator_id);
            setBehavior(ReplicaMode.ELECTION);
            // when receiving the election msg from the replica that detected the crash of the coord
            // Sends an ACK back
            cancelTimers();
        }
        cancelTimers();

        // add its id and its last update to the Update List
        ElectionMsg electionMsg = new ElectionMsg(msg);
        if (electionMsg.visitedReplicas.containsKey(this.id)) {
            if (electionMsg.candidateID == this.id) {
                // This is the new coordinator: new epoch, finish pending update, send SYNCHRONIZATION
                this.becomeCoordinator(electionMsg.visitedReplicas);
            } else {
                // forward ELECTION to the candidate replica
                tell(msg, this.replicasGroup.get(msg.candidateID));
                // TODO: set ACK timeout
            }
        } else {
            TreeMap<Integer, EpochSeqNum> visitedReplicas = new TreeMap<>(msg.visitedReplicas);
            visitedReplicas.put(this.id, this.lastUpdate);
            Integer newCandidateID;
            if (this.lastUpdate.compareTo(msg.getCandidateEpochSeq()) == 0) {
                newCandidateID = Math.min(this.id, msg.candidateID);
            } else if (this.lastUpdate.compareTo(msg.getCandidateEpochSeq()) > 0) {
                newCandidateID = this.id;
            } else { // if (this.lastUpdate.compareTo(msg.getCandidateEpochSeq()) < 0)
                newCandidateID = msg.candidateID;
            }

            tell(new ElectionMsg(visitedReplicas, newCandidateID, msg.epoch), this.successor);
            // TODO: set ACK timeout
        }
    }

    private void onACKElectionMsg(ACKElectionMsg msg) {
        // cancel the ACK timeout
    }

    private void becomeCoordinator(SortedMap<Integer, EpochSeqNum> electionUpdateList) {
        callbackOnCoordinatorElected(this.id);
        // Set stable all pending updates, Become Coordinator, Send a sync Msg to the replicas announcing new coordinator
        // and provide the other nodes with all the missing updates

        TreeMap<EpochSeqNum, UpdateEntry> sorted = new TreeMap<>(updatesHistory);
        // 1. Finish the local pending updates if any
        for (Map.Entry<EpochSeqNum, UpdateEntry> e : sorted.entrySet()) {
            if (unstable.containsKey(e.getKey())) {
                deliverUpdate(e.getKey(), e.getValue());
            }
        }

        // 2. Set a new epoch = epoch of the last update + 1 & the sequence number restarts at 0
        int newEpoch = this.lastUpdate.epoch + 1;
        this.coordinator_id = this.id;
        this.lastUpdate = new EpochSeqNum(newEpoch, 0);

        // 3. Send each replica only what it is missing, starting from its last update
        for (Map.Entry<Integer, EpochSeqNum> eu : electionUpdateList.entrySet()) {
            if (eu.getKey() == this.id) continue; // skip self
            tell(
                    new SyncMsg(newEpoch, this.id, sorted.tailMap(eu.getValue(), true)),
                    this.replicasGroup.get(eu.getKey()));
        }

        setBehavior(ReplicaMode.COORDINATOR);
        startHeartbeatTimer();
    }

    private void onSyncMsg(SyncMsg msg) {
        // 1. Ignore stale or duplicate synchronizations
        if (msg.newEpoch <= this.lastUpdate.epoch) return;

        callbackOnCoordinatorElected(msg.newCoordinatorId);

        // TODO: cancel election timeout and ACK timeout

        // 2. Deliver missing updates in <e,i> order (the map is sorted), without duplicates
        for (Map.Entry<EpochSeqNum, UpdateEntry> e : msg.missingUpdates.entrySet()) {
            if (updatesHistory.containsKey(e.getKey()) && !unstable.containsKey(e.getKey())) continue; // already delivered
            deliverUpdate(e.getKey(), e.getValue());
        }

        // 3. Adopt the new epoch and coordinator
        this.coordinator_id = msg.newCoordinatorId;
        this.lastUpdate = new EpochSeqNum(msg.newEpoch, 0);

        // 4. Back to normal behavior
        setBehavior(ReplicaMode.NORMAL);
    }
}
