package it.unitn.ds;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.Map;

public class Utils {

    public static void broadcast(ActorRef coordinator, Map<Integer, ActorRef> replicasGroup, Serializable msg) {
        for(ActorRef replica : replicasGroup.values())
            if(!replica.equals(coordinator)){
                replica.tell(msg, coordinator);
            }
    }

}
