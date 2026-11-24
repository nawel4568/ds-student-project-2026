package it.unitn.ds;

import java.io.Serializable;

/* Update id <e,i>: ordered by epoch, then sequence number */
public record EpochSeqNum(int epoch, int seqNum) implements Serializable, Comparable<EpochSeqNum> {
    public EpochSeqNum incrementEpoch() {
        return new EpochSeqNum(epoch + 1, 0);
    }

    public EpochSeqNum incrementSeqNum() {
        return new EpochSeqNum(epoch, seqNum + 1);
    }

    @Override
    public int compareTo(EpochSeqNum other) {
        return epoch != other.epoch ? Integer.compare(epoch, other.epoch) : Integer.compare(seqNum, other.seqNum);
    }

    @Override
    public String toString() {
        return "<" + epoch + "," + seqNum + ">";
    }
}
