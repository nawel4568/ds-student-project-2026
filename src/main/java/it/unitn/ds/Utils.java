package it.unitn.ds;

import akka.actor.Actor;
import akka.actor.ActorRef;

import java.util.List;
import java.util.Map;

public class Utils {

    public static void broadcastUpdate(ActorRef coordinator, Map<Integer, ActorRef> replicasGroup, Replica.UpdateMsg msg) {
        for(ActorRef replica : replicasGroup.values())
            if(!replica.equals(coordinator)){
                replica.tell(msg, coordinator);
            }
    }

    public static void broadcastWriteOK(ActorRef coordinator, Map<Integer, ActorRef> replicasGroup, Replica.WriteOKMsg msg){
        for(ActorRef replica : replicasGroup.values())
            if(!replica.equals(coordinator)){
                replica.tell(msg, coordinator);
            }
    }

}
