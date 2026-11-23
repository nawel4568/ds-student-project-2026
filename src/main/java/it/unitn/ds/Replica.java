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
    private ActorRef successor;

    private EpochSeqNum lastUpdate; // the epoch and seqNum of the last Update Req
    private LinkedHashMap<EpochSeqNum, UpdateEntry> updatesHistory;
    // Updates not applied yet -> acks received (only the coordinator counts them)
    private final Map<EpochSeqNum, Integer> unstable = new HashMap<>();

    private final CrashTrigger crashTrigger = new CrashTrigger();

    // Election Variables
    private final SortedSet<ElectionUpdate> electionUpdateList = new TreeSet<>();

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
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
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    // =================================================================================
    // Heartbeat
    // =================================================================================

    // coordinator: periodic HeartbeatTick to self
    // replica: fires if the coordinator stays silent
    private Cancellable heartbeatTimer;

    private void startHeartbeatTimer() {
        if (this.heartbeatTimer != null && !this.heartbeatTimer.isCancelled())
            this.heartbeatTimer.cancel();

        this.heartbeatTimer = getContext().system().getScheduler().scheduleWithFixedDelay(
                Duration.ZERO,
                Duration.ofMillis(getCoordinatorBeatInterval()),
                getSelf(),
                new HeartbeatTick(),
                getContext().system().dispatcher(),
                ActorRef.noSender());
    }

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

        // null before first heartbeat arrives.
        if (this.heartbeatTimer != null && !this.heartbeatTimer.isCancelled()) {
            this.heartbeatTimer.cancel();
        }
        this.heartbeatTimer = schedule(new CoordinatorTimeout(), getHeartbeatTimeout());
        crashIf(Crash.Type.Heartbeat);
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
        this.successor = this.replicasGroup.get((this.id+1) % this.replicasGroup.size());

        if (this.id == this.coordinator_id) {
            getContext().become(coordinatorBehavior());
            startHeartbeatTimer();
        }
    }

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
                .build();
    }

    public final Receive electionBehavior() {
        return createBaseReceiveBuilder()
                // TODO: - handle another Election (best candidate crashes)
                .match(ReadReqMsg.class, this::onReadReqMsg)
                .match(ElectionMsg.class, this::onLapCompElectionMsg)
                .match(SyncMsg.class, this::onSyncMsg)
                .match(ACKElectionMsg.class, this::onACKElectionMsg)
                .matchAny(msg -> {})
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
        tell(
            new ReadReplyMsg(msg.reqId, new AbstractClient.ReadResult(true, msg.index, this.locations[msg.index], this.id)),
            getSender()
        );
    }

    private void onWriteReqMsg(WriteReqMsg msg) {
        debug("received WRITE (" + msg.index + ", " + msg.value + ") from " + getSender().path().name());
        tell(
            new UpdateRequestMsg(msg.index, msg.value, msg.reqId, getSender()), 
            this.replicasGroup.get(coordinator_id)
        );
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

        this.lastUpdate = msg.epochSeqNum;
        updatesHistory.put(this.lastUpdate,
                new UpdateEntry(msg.index, msg.value, msg.reqId, msg.clientRequesting, msg.replicaRequested));
        unstable.put(this.lastUpdate, 0);

        tell(new AckMsg(this.lastUpdate), getSender());
        crashIf(Crash.Type.Update);
    }

    /* Coordinator: commit once a quorum acked; later acks are ignored */
    private void onAckMsg(AckMsg msg) {
        debug("received ACK " + msg.epochSeqNum + " from " + getSender().path().name());

        // null once the update is applied: later acks are ignored
        Integer acks = unstable.computeIfPresent(msg.epochSeqNum, (k, n) -> n + 1);
        if (acks == null || acks < getSystemNumberOfActors() / 2 + 1)
            return;

        applyUpdate(msg.epochSeqNum, updatesHistory.get(msg.epochSeqNum));
        broadcast(new WriteOKMsg(msg.epochSeqNum));
    }

    /* Replica: commit the update */
    private void onWriteOKMsg(WriteOKMsg msg) {
        debug("received WRITEOK " + msg.epochSeqNum + " from " + getSender().path().name());

        applyUpdate(msg.epochSeqNum, updatesHistory.get(msg.epochSeqNum));
        crashIf(Crash.Type.WriteOK);
    }

    private void applyUpdate(EpochSeqNum key, UpdateEntry update) {
        unstable.remove(key);
        locations[update.index] = update.value;
        log("applied update " + key.epoch + ":" + key.seqNum + " (" + update.index + ", " + update.value + ")");
        callbackOnUpdateApplied(update.index, update.value);

        // only the replica contacted by the client replies to it
        if (getSelf().equals(update.replicaRequested))
            tell(new WriteReplyMsg(update.reqId,
                    new AbstractClient.WriteResult(true, update.index, update.value, this.id)), update.clientRequesting);
    }

    // =================================================================================
    // Failure detection
    // =================================================================================

    /* Self-scheduled: no heartbeat from the coordinator within the timeout */
    private static final class CoordinatorTimeout implements Serializable {
    }

    private void onCoordinatorTimeout(CoordinatorTimeout msg) {
        startElection();
    }

    // =================================================================================
    // Coordinator election
    // =================================================================================

    private void startElection() {
        // Starting the Election protocol the first time to timeout
        // case 1) if timeout is HEARTBEAT, UPDATE, WRITEOK
        this.getContext().become(electionBehavior()); // switch to the election behavior
        this.heartbeatTimer.cancel();

        // create the Update List and add its id and its last update
        SortedSet<ElectionUpdate> updateList = new TreeSet<>();
        updateList.add(new ElectionUpdate(this.id, this.lastUpdate));
        ElectionMsg newElectionMsg = new ElectionMsg(updateList);

        // send the message (id, updateList) to the successor
        tell(newElectionMsg, this.successor);
        // TODO: set ACK timeout

        // case 2) if timeout is ELECTIONACK
        // TODO: compute the next successor and send the ElectionMsg
    }

    private void onElectionMsg(ElectionMsg msg) {
        // when receiving the election msg from the replica that detected the crash of the coord
        // Sends an ACK back
        tell(new ACKElectionMsg(), getSender());
        // TODO:
        //  - Cancel all the timeouts

        // switch to the election behavior
        this.getContext().become(electionBehavior());

        // add its id and its last update to the Update List
        SortedSet<ElectionUpdate> updateList = new TreeSet<>(msg.electionUpdateList);
        updateList.add(new ElectionUpdate(this.id, this.lastUpdate));
        ElectionMsg newElectionMsg = new ElectionMsg(updateList);

        // send the message (id, updateList) to the successor node
        tell(newElectionMsg, this.successor);
        // TODO: set ACK timeout
    }

    private void onLapCompElectionMsg(ElectionMsg msg) {
        // When a Replica receives an Election message & it is in Election Behavior
        // Sends an ACK back
        tell(new ACKElectionMsg(), getSender());

        // compute the current winner
        ElectionUpdate winner = Collections.max(msg.electionUpdateList, ElectionUpdate::compareTo);
        if (winner.replicaID == this.id) {
            // This is the new coordinator: new epoch, finish pending update, send SYNCHRONIZATION
            this.becomeCoordinator(msg.electionUpdateList);
        } else {
            // forward ELECTION to the next live node in the ring
            tell(new ElectionMsg(msg), this.successor);
            // TODO: set ACK timeout
        }
    }

    private void onACKElectionMsg(ACKElectionMsg msg) {
        // cancel the ACK timeout
    }

    private void onSyncMsg(SyncMsg msg) {
        // 1. Ignore stale or duplicate synchronizations
        if (msg.newEpoch <= this.lastUpdate.epoch)
            return;

        // TODO: cancel election timeout and ACK timeout

        // 2. Deliver missing updates in <e,i> order (the map is sorted), without duplicates
        for (Map.Entry<EpochSeqNum, UpdateEntry> e : msg.missingUpdates.entrySet()) {
            if (updatesHistory.containsKey(e.getKey()) && !unstable.containsKey(e.getKey()))
                continue; // already delivered
            deliverUpdate(e.getKey(), e.getValue());
        }

        // 3. Adopt the new epoch and coordinator
        this.coordinator_id = msg.newCoordinatorId;
        this.lastUpdate = new EpochSeqNum(msg.newEpoch, 0);

        // 4. Back to normal behavior
        getContext().become(createReceive());

        // TODO: reset the heartbeat timeout for the new coordinator
    }

    private void becomeCoordinator(SortedSet<ElectionUpdate> electionUpdateList) {
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
        for (ElectionUpdate eu : electionUpdateList) {
            if (eu.replicaID == this.id)
                continue;
            tell(new SyncMsg(newEpoch, this.id, sorted.tailMap(eu.lastUpdate, true)), this.replicasGroup.get(eu.replicaID));
        }

        // 4. Leave the election behavior
        getContext().become(createReceive());
        // TODO: start sending HEARTBEATs
    }

    private void deliverUpdate(EpochSeqNum id, UpdateEntry u) {
        updatesHistory.put(id, u);
        applyUpdate(id, u);
    }
}
