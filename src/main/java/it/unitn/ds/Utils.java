package it.unitn.ds;

import akka.actor.ActorRef;

import java.util.List;
import java.util.Map;

public class Utils {

    public static void broadcastUpdate(ActorRef coordinator, Map<Integer, ActorRef> replicasGroup, Replica.UpdateRequestMsg msg) {
        for(ActorRef replica : replicasGroup.values())
            if(!replica.equals(coordinator)){
                replica.tell(new Replica.UpdateRequestMsg(msg), coordinator);
            }
    }


}
