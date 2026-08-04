package it.unitn.ds;

import java.util.*;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractReplica.InitSystem;


public class Main {
    final static int N_REPLICAS = 4;
    final static int N_CLIENTS = 4;
    final static int COORDINATOR_ID = 0;
    final static long clientReadTimeoutDelay = 2000;
    final static long clientWriteTimeoutDelay = 2000;


    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("START");
        System.out.println("========================================\n");


        final ActorSystem system = ActorSystem.create("TestMain");

        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);

        Map<Integer, ActorRef> replicas = new HashMap<>(N_REPLICAS);
        for (int i = 0; i < N_REPLICAS; i++) {
            replicas.put(i,
                system.actorOf(
                    Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                    "Replica_" + i
                )
            );
        }

        InitSystem initMsg = new InitSystem(replicas, COORDINATOR_ID);
        for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
            entry.getValue().tell(initMsg, ActorRef.noSender());
        }

        // Create clients
        Map<Integer, ActorRef> clients = new HashMap<>(N_CLIENTS);
        for (int i=0; i<N_CLIENTS; i++) {
            clients.put(i,
                system.actorOf(
                    Client.props(clientReadTimeoutDelay, clientWriteTimeoutDelay, Optional.empty()),
                    "Client_" + i
                )
            );
            Logger.log("Client " + i + " was added!");
        }

        // TODO: Implement your main logic

        system.terminate();

        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
    }


}
