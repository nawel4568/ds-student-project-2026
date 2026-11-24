package it.unitn.ds;

/* Entry of the election token: a replica and the last update it knows. Ordered by update, then id. */
public class ElectionUpdate implements Comparable<ElectionUpdate> {
    public final int replicaID;
    public final EpochSeqNum lastUpdate;

    public ElectionUpdate(int replicaID, EpochSeqNum lastUpdate) {
        this.replicaID = replicaID;
        this.lastUpdate = lastUpdate;
    }

    @Override
    public int compareTo(ElectionUpdate other) {
        int compare = this.lastUpdate.compareTo(other.lastUpdate);
        if (compare != 0) return compare;
        // otherwise compare by replica ID
        return Integer.compare(this.replicaID, other.replicaID);
    }
}
