package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.*;

public class Client extends AbstractClient {

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

    @Override
    public void sendRead(ActorRef replica, int index) {
        // TODO: implement
        replica.tell(new Replica.ReadReqMsg(index), getSelf());
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        // TODO: implement
        replica.tell(new WriteRequest(index, value), getSelf());

    }

    /* Getting Results back from the Replica */
    private void onReadResult(ReadResult msg) {
        Logger.log(getSelf().path().name() + " received the location " + msg.value + " of index " + msg.index + " from Replica " + msg.fromReplica);
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // TODO add your message handlers here .match(, )
                .match(ReadResult.class, this::onReadResult)
                .build();
    }


}
