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
    private ActorRef successor;

    private  EpochSeqNum lastUpdate; // the epoch and seqNum of the last Update Req
    private LinkedHashMap<EpochSeqNum, LocationUpdate> updatesHistory;


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

        this.updatesHistory = new LinkedHashMap<EpochSeqNum, LocationUpdate>();
        LocationUpdate dummy = new LocationUpdate(0, 0, ActorRef.noSender(), ActorRef.noSender());
        dummy.setStable();
        this.updatesHistory.put(lastUpdate, dummy);
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
        Logger.debug("Replica " + this.id + " is starting.");
        this.coordinator_id = sysInit.coordinator_id;
        this.replicasGroup.putAll(sysInit.group);
        this.successor = this.replicasGroup.get((this.id+1) % this.replicasGroup.size());
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
                .match(ElectionMsg.class, this::onElectionMsg)
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
        getSender().tell(new AbstractClient.ReadResult(true, msg.index, this.locations[msg.index], this.id), getSelf());
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

        this.replicasGroup.get(coordinator_id).tell(new UpdateRequestMsg(msg.index, msg.value, getSender()), getSelf());
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
        // this is received only by the coordinator
        Logger.debug(getSelf().path().name() + " Received an Update Request from " + getSender().path().name());

        // Coordinator Receives the Update Msg from a Replica
        this.lastUpdate = this.lastUpdate.incrementSeqNum();
        LocationUpdate currentUpdate = new LocationUpdate(msg.index, msg.value, msg.clientRequesting, getSender());
        currentUpdate.setUnstable();
        currentUpdate.increaseQuorum();
        updatesHistory.put(this.lastUpdate, currentUpdate);

        Utils.broadcast(getSelf(), this.replicasGroup, new Replica.UpdateMsg(new EpochSeqNum(this.lastUpdate), currentUpdate)); // Broadcast the Update to other Replicas

    }

    public static class UpdateMsg implements Serializable{
        EpochSeqNum epochSeqNum;
        LocationUpdate currentUpdate;

        public UpdateMsg(EpochSeqNum epochSeqNum, LocationUpdate currentUpdate) {
            this.epochSeqNum = epochSeqNum;
            this.currentUpdate = currentUpdate;
        }
    }

    private void onUpdateMsg(UpdateMsg msg) {
        // Message received by Replica from Coordinator
        Logger.debug(getSelf().path().name() + " Received an Update Msg from " + getSender().path().name());
        this.lastUpdate = new EpochSeqNum(msg.epochSeqNum);
        updatesHistory.put(this.lastUpdate, new LocationUpdate(msg.currentUpdate));
        getSender().tell(new AckMsg(this.lastUpdate), getSelf());

    }

    public static class AckMsg implements Serializable{
        EpochSeqNum epochSeqNum;
        public AckMsg(EpochSeqNum epochSeqNum) {
            this.epochSeqNum = epochSeqNum;
        }
    }

    private void onAckMsg(AckMsg msg) {
        // Received by the Coordinator from Replicas
        Logger.debug(getSelf().path().name() + " Received an ACK Msg from " + getSender().path().name() + " for update " + msg.epochSeqNum.toString());
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
            Logger.debug(getSelf().path().name() + " set " + msg.epochSeqNum + " to stable.");
            currentUpdate.setStable();
            updatesHistory.put(msg.epochSeqNum, currentUpdate);
            locations[currentUpdate.index] = currentUpdate.value;
            callbackOnUpdateApplied(currentUpdate.index, currentUpdate.value);
            Utils.broadcast(getSelf(), this.replicasGroup, new Replica.WriteOKMsg(msg.epochSeqNum));
            if (getSelf() == currentUpdate.replicaRequested) {
                currentUpdate.clientRequesting.tell(new AbstractClient.WriteResult(true, currentUpdate.index, currentUpdate.value, this.id), getSelf());
            }
        }
    }

    public static class WriteOKMsg implements Serializable{
        EpochSeqNum epochSeqNum;
        public WriteOKMsg(EpochSeqNum epochSeqNum) {
            this.epochSeqNum = epochSeqNum;

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
            currentUpdate.clientRequesting.tell(new AbstractClient.WriteResult(true, currentUpdate.index, currentUpdate.value, this.id), getSelf());
        }

    }


    public static class TimeoutMsg implements Serializable{
        // can be sent if a timeout happens
        public TimeoutMsg() {

        }
    }

    private void onTimeoutMsg(TimeoutMsg msg) {
        // Starting the Election protocol the first time to timeout
        // case 1) if timeout is HEARTBEAT, UPDATE, WRITEOK
        this.getContext().become(electionBehavior()); // switch to the election behavior

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

    public static class ElectionMsg implements Serializable{
        public final SortedSet<ElectionUpdate> electionUpdateList;
        public ElectionMsg(SortedSet<ElectionUpdate> electionUpdateList) {
            this.electionUpdateList = Collections.unmodifiableSortedSet(new TreeSet<ElectionUpdate>(electionUpdateList));
        }
        public ElectionMsg(ElectionMsg other) {
            this.electionUpdateList = other.electionUpdateList;
        }
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

    private void onLapCompElectionMsg(ElectionMsg msg){
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

    public static class ACKElectionMsg implements Serializable{
        public ACKElectionMsg() {
        }
    }

    private void onACKElectionMsg(ACKElectionMsg msg){
        // cancel the ACK timeout
    }

    public static class SyncMsg implements Serializable {
        public final int newEpoch;
        public final int newCoordinatorId;
        public final SortedMap<EpochSeqNum, LocationUpdate> missingUpdates;

        public SyncMsg(int newEpoch, int newCoordinatorId, SortedMap<EpochSeqNum, LocationUpdate> missingUpdates) {
            this.newEpoch = newEpoch;
            this.newCoordinatorId = newCoordinatorId;
            this.missingUpdates = Collections.unmodifiableSortedMap(new TreeMap<>(missingUpdates));
        }
    }

    private void onSyncMsg(SyncMsg msg) {
        // 1. Ignore stale or duplicate synchronizations
        if (msg.newEpoch <= this.lastUpdate.epoch) return;

        // TODO: cancel election timeout and ACK timeout

        // 2. Deliver missing updates in <e,i> order (the map is sorted), without duplicates
        for (Map.Entry<EpochSeqNum, LocationUpdate> e : msg.missingUpdates.entrySet()) {
            LocationUpdate existing = updatesHistory.get(e.getKey());
            if (existing != null && existing.isStable()) continue;   // already delivered
            deliverUpdate(e.getKey(), new LocationUpdate(e.getValue()));
        }

        // 3. Adopt the new epoch and coordinator
        this.coordinator_id = msg.newCoordinatorId;
        this.lastUpdate = new EpochSeqNum(msg.newEpoch, 0);

        // 4. Back to normal behavior
        getContext().become(createReceive());

        // TODO: reset the heartbeat timeout for the new coordinator
    }



    // Data structure for election msg Update List
    public static class ElectionUpdate implements Comparable<ElectionUpdate> {
        public final int replicaID;
        public final EpochSeqNum lastUpdate;

        public ElectionUpdate(int replicaID, EpochSeqNum lastUpdate) {
            this.replicaID = replicaID;
            this.lastUpdate = new EpochSeqNum(lastUpdate);
        }

        @Override
        public int compareTo(ElectionUpdate other) {
            int compare = this.lastUpdate.compareTo(other.lastUpdate);
            if(compare != 0)
                return compare;
            //otherwise compare by replica ID
            return Integer.compare(this.replicaID, other.replicaID);
        }
    }

    private void becomeCoordinator(SortedSet<ElectionUpdate> electionUpdateList){
        // Set stable all pending updates, Become Coordinator, Send a sync Msg to the replicas announcing new coordinator
        // and provide the other nodes with all the missing updates

        TreeMap<EpochSeqNum, LocationUpdate> sorted = new TreeMap<>(updatesHistory);
        // 1. Finish the local pending updates if any
        for (Map.Entry<EpochSeqNum, LocationUpdate> e : sorted.entrySet()) {
            if (!e.getValue().isStable()) {
                deliverUpdate(e.getKey(), e.getValue());
            }
        }

        // 2. Set a new epoch = epoch of the last update + 1 & the sequence number restarts at 0
        int newEpoch = this.lastUpdate.epoch + 1;
        this.coordinator_id = this.id;
        this.lastUpdate = new EpochSeqNum(newEpoch, 0);

        // 3. Send each replica only what it is missing, starting from its last update
        for (ElectionUpdate eu : electionUpdateList) {
            if (eu.replicaID == this.id) continue;
            SortedMap<EpochSeqNum, LocationUpdate> missing = new TreeMap<>();
            for (Map.Entry<EpochSeqNum, LocationUpdate> e : sorted.tailMap(eu.lastUpdate, true).entrySet()) {
                missing.put(e.getKey(), new LocationUpdate(e.getValue()));
            }
            tell(new SyncMsg(newEpoch, this.id, missing), this.replicasGroup.get(eu.replicaID));
        }

        // 4. Leave the election behavior
        getContext().become(createReceive());
        // TODO: start sending HEARTBEATs
    }

    private void deliverUpdate(EpochSeqNum id, LocationUpdate u) {
        u.setStable();
        updatesHistory.put(id, u);
        locations[u.index] = u.value;
        callbackOnUpdateApplied(u.index, u.value);
        if (getSelf() == u.replicaRequested) {
            tell(new AbstractClient.WriteResult(true, u.index, u.value, this.id), u.clientRequesting);
        }
    }

}
