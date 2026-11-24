package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import java.io.Serializable;
import java.time.Duration;
import java.util.*;

public class Client extends AbstractClient {
    // Pending requests: request id -> timer. A request is pending until its reply or its timeout, whichever comes
    // first.
    private final Map<Integer, Cancellable> timeouts = new HashMap<>();
    private int nextReqId = 0;

    Client(
            long readTimeoutDelay,
            long writeTimeoutDelay,
            Optional<ActorRef> defaultTargetReplica,
            Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
    }

    public static Props props(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica) {
        return Props.create(
                Client.class,
                () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(
            long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
        return Props.create(
                Client.class,
                () -> new Client(
                        readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.ofNullable(listener)));
    }

    /* Self-scheduled: the read with this request id got no reply in time */
    private record ReadTimer(int reqId, ReadTimeout timeout) implements Serializable {}

    /* Self-scheduled: the write with this request id got no reply in time */
    private record WriteTimer(int reqId, WriteTimeout timeout) implements Serializable {}

    private void startTimer(int reqId, long delayMillis, Serializable timer) {
        timeouts.put(
                reqId,
                getContext()
                        .getSystem()
                        .scheduler()
                        .scheduleOnce(
                                Duration.ofMillis(delayMillis),
                                getSelf(),
                                timer,
                                getContext().system().dispatcher(),
                                getSelf()));
    }

    @Override
    public void sendRead(ActorRef replica, int index) {
        int reqId = nextReqId++;
        log("requesting READ (" + index + ") to Replica " + replica.path().name());
        replica.tell(new Messages.ReadReqMsg(reqId, index), getSelf());
        startTimer(reqId, getReadTimeoutDelay(), new ReadTimer(reqId, new ReadTimeout(getSelf(), replica, index)));
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        int reqId = nextReqId++;
        log("requesting WRITE (" + index + ", " + value + ") to Replica "
                + replica.path().name());
        replica.tell(new Messages.WriteReqMsg(reqId, index, value), getSelf());
        startTimer(
                reqId,
                getWriteTimeoutDelay(),
                new WriteTimer(reqId, new WriteTimeout(getSelf(), replica, index, value)));
    }

    /* Removes the request; false if it already completed or timed out (late reply, or a timer that fired before cancel) */
    private boolean complete(int reqId) {
        Cancellable timer = timeouts.remove(reqId);
        if (timer == null) return false;
        timer.cancel();
        return true;
    }

    /* Getting Results back from the Replica */
    private void onReadReply(Messages.ReadReplyMsg msg) {
        if (complete(msg.reqId)) callbackOnReadResult(msg.result);
    }

    private void onWriteReply(Messages.WriteReplyMsg msg) {
        if (complete(msg.reqId)) callbackOnWriteResult(msg.result);
    }

    private void onReadTimer(ReadTimer msg) {
        if (complete(msg.reqId)) callbackOnReadTimeout(msg.timeout);
    }

    private void onWriteTimer(WriteTimer msg) {
        if (complete(msg.reqId)) callbackOnWriteTimeout(msg.timeout);
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(Messages.ReadReplyMsg.class, this::onReadReply)
                .match(Messages.WriteReplyMsg.class, this::onWriteReply)
                .match(ReadTimer.class, this::onReadTimer)
                .match(WriteTimer.class, this::onWriteTimer)
                .build();
    }
}
