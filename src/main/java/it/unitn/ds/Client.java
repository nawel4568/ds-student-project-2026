package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Cancellable;

import java.util.*;

import java.time.Duration;

public class Client extends AbstractClient {

    // Map with Index and Cancellable timeout object
    private final Map<Integer, Cancellable> readTimeoutList = new HashMap<>();
    private final Map<Integer, Cancellable> writeTimeoutList = new HashMap<>();


    Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
    }

    public static Props props(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.ofNullable(listener)));
    }

    /* Cancelling Read timeout */
    private void cancelReadTimeout(int index) {

        Cancellable timeout = readTimeoutList.remove(index);
        if (timeout != null) {
            timeout.cancel();
        }
    }
    /* Cancelling Write timeout */
    private void cancelWriteTimeout(int index) {

        Cancellable timeout = writeTimeoutList.remove(index);
        if (timeout != null) {
            timeout.cancel();
        }
    }

    @Override
    public void sendRead(ActorRef replica, int index) {
        cancelReadTimeout(index);
        Logger.debug(getSelf().path().name() + " sends a read Req of index " + index + " to Replica " + replica.path().name());
        replica.tell(new Replica.ReadReqMsg(index), getSelf());

        Cancellable readTimeout = getContext().getSystem().scheduler().scheduleOnce(
                Duration.ofMillis(getReadTimeoutDelay()),
                getSelf(),
                new ReadTimeout(getSelf(), replica, index),
                getContext().system().dispatcher(),
                getSelf()
        );
        readTimeoutList.put(index, readTimeout);
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        /* To be checked since the Write does not need to wait for a reply and
        should the Write to the same index trigger the cancellation of the old timeout */
        cancelWriteTimeout(index);
        Logger.debug(getSelf().path().name() + " sends a write Req of value " + value + " at index " + index + " to Replica " + replica.path().name());
        replica.tell(new Replica.WriteReqMsg(index, value), getSelf());

        Cancellable writeTimeout = getContext().getSystem().scheduler().scheduleOnce(
                Duration.ofMillis(getReadTimeoutDelay()),
                getSelf(),
                new WriteTimeout(getSelf(), replica, index, value),
                getContext().system().dispatcher(),
                getSelf()
        );
        writeTimeoutList.put(index, writeTimeout);
    }

    /* Getting Results back from the Replica */
    private void onReadResult(ReadResult msg) {
        cancelReadTimeout(msg.index);
        callbackOnReadResult(new ReadResult(true, msg.index, msg.value, msg.fromReplica));
    }

    private void onWriteResult(WriteResult msg) {
        cancelWriteTimeout(msg.index);
        callbackOnWriteResult(new WriteResult(true, msg.index, msg.value, msg.fromReplica));
    }

    private void onReadTimeout(ReadTimeout msg) {
        callbackOnReadTimeout(new ReadTimeout(msg.client, msg.replica, msg.index));
    }
    private void onWriteTimeout(WriteTimeout msg) {
        callbackOnWriteTimeout(new WriteTimeout(msg.client,msg.replica, msg.index, msg.value));
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(ReadResult.class, this::onReadResult)
                .match(WriteResult.class, this::onWriteResult)
                .match(ReadTimeout.class, this::onReadTimeout)
                .match(WriteTimeout.class, this::onWriteTimeout)
                .build();
    }


}
