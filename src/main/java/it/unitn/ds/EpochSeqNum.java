package it.unitn.ds;

import java.io.Serializable;
import java.util.Objects;

public class EpochSeqNum implements Serializable, Comparable<EpochSeqNum>{
    public int epoch;
    public int seqNum;

    public EpochSeqNum(int epoch, int seqNum) {
        this.epoch = epoch;
        this.seqNum = seqNum;
    }

    public EpochSeqNum(EpochSeqNum other) {
        this.epoch = other.epoch;
        this.seqNum = other.seqNum;
    }

    public int getEpoch() {
        return this.epoch;
    }
    public int getSeqNum() {
        return seqNum;
    }

    public void setEpoch(int epoch){
        this.epoch = epoch;
    }

    public void setSeqNum(int seqNum) {
        this.seqNum = seqNum;
    }

    @Override
    public int compareTo(EpochSeqNum other) {
        if (this.epoch != other.epoch) return Integer.compare(this.epoch, other.epoch);
        return Integer.compare(this.seqNum, other.seqNum);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof EpochSeqNum)) return false;
        EpochSeqNum o = (EpochSeqNum) obj;
        return this.epoch == o.epoch && this.seqNum == o.seqNum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch, seqNum);
    }

    @Override
    public String toString() {
        return "<" + epoch + "," + seqNum + ">";
    }


}
