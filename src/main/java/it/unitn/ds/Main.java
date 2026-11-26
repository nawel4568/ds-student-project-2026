package it.unitn.ds;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.InitSystem;

import java.io.Serializable;
import java.util.*;

public class Main {
    static final int N_CLIENTS = 4;
    static final int COORDINATOR_ID = 0;
    static final long CLIENT_TIMEOUT = 2000;

    static ActorSystem system;
    static ActorRef checker;
    static final Map<Integer, ActorRef> replicas = new HashMap<>();
    static final List<ActorRef> clients = new ArrayList<>();
    static int n = 6;

    public static void main(String[] args) throws Exception {
        int scenario = args.length > 0 ? Integer.parseInt(args[0]) : 1;
        n = (scenario == 9) ? 7 : 6;   // scenario 9 needs 3 crashes -> N=7

        System.out.println("========================================");
        System.out.println("START scenario " + scenario + " (N=" + n + ")");
        System.out.println("========================================\n");

        system = ActorSystem.create("TestMain");
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(false);

        checker = system.actorOf(Props.create(Checker.class), "Checker");

        for (int i = 0; i < n; i++) {
            replicas.put(i, system.actorOf(
                Replica.propsWithListener(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY,
                    AbstractReplica.COORDINATOR_BEAT_INTERVAL, checker),
                "Replica_" + i));
        }
        InitSystem init = new InitSystem(replicas, COORDINATOR_ID);
        replicas.values().forEach(r -> r.tell(init, ActorRef.noSender()));

        for (int i = 0; i < N_CLIENTS; i++) {
            clients.add(system.actorOf(
                Client.propsWithListener(CLIENT_TIMEOUT, CLIENT_TIMEOUT, Optional.empty(), checker),
                "Client_" + i));
        }
        pause(300);

        Expect expect = runScenario(scenario);

        pause(500);
        checker.tell(expect, ActorRef.noSender());
        pause(500);
        system.terminate();

        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
    }

    // ---------------------------------------------------------------- helpers
    static void pause(long ms) throws InterruptedException { Thread.sleep(ms); }

    static void crash(int replica, Crash.Type type, int afterN) {
        replicas.get(replica).tell(new Crash(type, afterN), ActorRef.noSender());
    }
    static void write(int client, int replica, int idx, int val) {
        clients.get(client).tell(new AbstractClient.WriteRequest(idx, val, replicas.get(replica)), ActorRef.noSender());
    }
    static void read(int client, int replica, int idx) {
        clients.get(client).tell(new AbstractClient.ReadRequest(idx, replicas.get(replica)), ActorRef.noSender());
    }

    /** dead = replicas that must be crashed at the end; coordinator: expected final coordinator,
     *  -1 = no election expected, -2 = any but all survivors must agree;
     *  updates = expected number of applied updates (-1 = skip); rounds = elections each survivor sees. */
    public static final class Expect implements Serializable {
        final int n; final Set<Integer> dead; final int coordinator; final int updates; final int rounds;
        Expect(int n, Set<Integer> dead, int coordinator, int updates, int rounds) {
            this.n = n; this.dead = Set.copyOf(dead); this.coordinator = coordinator;
            this.updates = updates; this.rounds = rounds;
        }
    }

    // -------------------------------------------------------------- scenarios
    static Expect runScenario(int s) throws Exception {
        switch (s) {
            case 1 -> { // no crash
                write(0, 1, 0, 10); write(1, 2, 1, 20); write(2, 3, 2, 30);
                pause(500); read(3, 4, 0); read(3, 4, 1); read(3, 4, 2);
                pause(2500);
                return new Expect(n, Set.of(), -1, 3, 0);
            }
            case 2 -> { // idle coordinator crash (heartbeat timeout)
                crash(0, Crash.Type.Now, 0);
                pause(4000);
                write(0, 3, 0, 10);
                pause(1500);
                return new Expect(n, Set.of(0), 1, 1, 1);
            }
            case 3 -> { // coordinator dead before the write: write is re-forwarded, not lost
                crash(0, Crash.Type.Now, 0);
                write(0, 3, 0, 10);
                pause(5000);
                return new Expect(n, Set.of(0), 1, 1, 1);
            }
            case 4 -> { // crash during UPDATE broadcast: only replicas 1,2 got it
                crash(0, Crash.Type.Update, 2);
                write(0, 4, 0, 10);
                pause(5000);
                return new Expect(n, Set.of(0), 1, 1, 1);
            }
            case 5 -> { // crash during WRITEOK broadcast: only replicas 1,2 applied (uniform agreement)
                crash(0, Crash.Type.WriteOK, 2);
                write(0, 4, 0, 10);
                pause(5000);
                return new Expect(n, Set.of(0), 1, 1, 1);
            }
            case 6 -> { // crash after the whole WRITEOK broadcast (sync must not duplicate)
                crash(0, Crash.Type.WriteOK, n - 1);
                write(0, 4, 0, 10);
                pause(5000);
                return new Expect(n, Set.of(0), 1, 1, 1);
            }
            case 7 -> { // two plain replicas crash, system keeps working, no election
                crash(3, Crash.Type.Now, 0); crash(4, Crash.Type.Now, 0);
                pause(200);
                write(0, 1, 0, 10); write(1, 2, 1, 20); write(2, 5, 2, 30);
                pause(3000);
                return new Expect(n, Set.of(3, 4), -1, 3, 0);
            }
            case 8 -> { // coordinator and its ring successor crash: 2 ACK timeouts at replica 5
                crash(0, Crash.Type.Now, 0); crash(1, Crash.Type.Now, 0);
                pause(5000);
                write(0, 4, 0, 10);
                pause(1500);
                return new Expect(n, Set.of(0, 1), 2, 1, 1);
            }
            case 9 -> { // N=7: three consecutive dead nodes in the ring
                crash(0, Crash.Type.Now, 0); crash(1, Crash.Type.Now, 0); crash(2, Crash.Type.Now, 0);
                pause(7000);
                write(0, 5, 0, 10);
                pause(1500);
                return new Expect(n, Set.of(0, 1, 2), 3, 1, 1);
            }
            case 10 -> { // the would-be winner (1) crashes during the election
                crash(0, Crash.Type.Now, 0);
                crash(1, Crash.Type.Election, 1);
                pause(6000);
                write(0, 3, 0, 10);
                pause(1500);
                return new Expect(n, Set.of(0, 1), 2, 1, 1);
            }
            case 11 -> { // a non-winner crashes right after forwarding the token
                crash(0, Crash.Type.Now, 0);
                crash(3, Crash.Type.Election, 1);
                pause(6000);
                write(0, 4, 0, 10);
                pause(1500);
                return new Expect(n, Set.of(0, 3), 1, 1, 1);
            }
            case 12 -> { // the new coordinator crashes too: epoch 2
                crash(0, Crash.Type.Now, 0);
                pause(4000);
                crash(1, Crash.Type.Now, 0);
                pause(5000);
                write(0, 4, 0, 10);
                pause(1500);
                return new Expect(n, Set.of(0, 1), 2, 1, 2);
            }
            case 13 -> { // force concurrent elections: crash right after a complete heartbeat round
                pause(1500);
                crash(0, Crash.Type.Heartbeat, n - 1);
                pause(5000);
                return new Expect(n, Set.of(0), 1, 0, 1);
            }
            case 14 -> { // continuous writes (idx 0, increasing) + reads across a coordinator crash
                for (int i = 0; i < 40; i++) {
                    if (i == 10) crash(0, Crash.Type.Now, 0);
                    write(i % 3, 3, 0, i + 1);
                    read(3, 2, 0);
                    pause(150);
                }
                pause(4000);
                return new Expect(n, Set.of(0), -2, 40, 1);
            }
            default -> throw new IllegalArgumentException("scenario 1-14");
        }
    }

    // ---------------------------------------------------------------- checker
    public static class Checker extends AbstractActor {
        private final Map<Integer, List<String>> applied = new TreeMap<>();
        private final Map<Integer, List<Integer>> elected = new TreeMap<>();
        private final Map<ActorRef, List<Integer>> reads = new HashMap<>();
        private int electionsStarted, writesOk, readTimeouts, writeTimeouts;

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                .match(AbstractReplica.UpdateApplied.class, m ->
                    applied.computeIfAbsent(m.replicaId, k -> new ArrayList<>()).add(m.index + "=" + m.value))
                .match(AbstractReplica.CoordinatorElected.class, m ->
                    elected.computeIfAbsent(m.replicaId, k -> new ArrayList<>()).add(m.newCoordinatorId))
                .match(AbstractReplica.ElectionStarted.class, m -> electionsStarted++)
                .match(AbstractClient.ReadResult.class, m -> {
                    String[] p = m.toString().replaceAll("[()]", "").split(", "); // success, idx, value, from
                    if (p[0].equals("true"))
                        reads.computeIfAbsent(getSender(), k -> new ArrayList<>()).add(Integer.valueOf(p[2]));
                })
                .match(AbstractClient.WriteResult.class, m -> { if (m.toString().startsWith("(true")) writesOk++; })
                .match(AbstractClient.ReadTimeout.class, m -> readTimeouts++)
                .match(AbstractClient.WriteTimeout.class, m -> writeTimeouts++)
                .match(Expect.class, this::verify)
                .build();
        }

        private void verify(Expect e) {
            List<String> problems = new ArrayList<>();
            List<String> ref = null; int refId = -1;

            for (int id = 0; id < e.n; id++) {                       // survivors: identical order
                if (e.dead.contains(id)) continue;
                List<String> a = applied.getOrDefault(id, List.of());
                if (ref == null) { ref = a; refId = id; }
                else if (!ref.equals(a))
                    problems.add("Replica " + id + " applied " + a + " but replica " + refId + " applied " + ref);
            }
            if (ref == null) ref = List.of();
            for (int id : e.dead) {                                   // uniform agreement: dead = prefix
                List<String> a = applied.getOrDefault(id, List.of());
                if (a.size() > ref.size() || !ref.subList(0, a.size()).equals(a))
                    problems.add("Crashed replica " + id + " applied " + a + " which is not a prefix of " + ref);
            }
            if (new HashSet<>(ref).size() != ref.size()) problems.add("Duplicate delivery in " + ref);
            if (e.updates >= 0 && ref.size() != e.updates)
                problems.add("Expected " + e.updates + " applied updates, got " + ref.size());

            Set<Integer> lastCoord = new HashSet<>();
            for (int id = 0; id < e.n; id++) {
                if (e.dead.contains(id)) continue;
                List<Integer> el = elected.getOrDefault(id, List.of());
                if (el.size() != e.rounds)
                    problems.add("Replica " + id + " saw " + el.size() + " coordinator announcements " + el
                        + ", expected " + e.rounds);
                if (!el.isEmpty()) lastCoord.add(el.get(el.size() - 1));
            }
            if (e.coordinator >= 0 && !lastCoord.equals(Set.of(e.coordinator)))
                problems.add("Final coordinator(s) " + lastCoord + ", expected " + e.coordinator);
            if (e.coordinator == -2 && lastCoord.size() != 1)
                problems.add("Survivors disagree on the coordinator: " + lastCoord);

            for (Map.Entry<ActorRef, List<Integer>> r : reads.entrySet()) {  // monotonic reads
                List<Integer> v = r.getValue();
                for (int i = 1; i < v.size(); i++)
                    if (v.get(i) < v.get(i - 1)) {
                        problems.add(r.getKey().path().name() + " read went backwards: " + v); break;
                    }
            }

            System.out.println("\n---------------- CHECKER ----------------");
            applied.forEach((id, l) -> System.out.println("Replica " + id + " applied " + l.size() + ": " + l));
            elected.forEach((id, l) -> System.out.println("Replica " + id + " coordinator events: " + l));
            System.out.println("elections started=" + electionsStarted + " writesOK=" + writesOk
                + " writeTimeouts=" + writeTimeouts + " readTimeouts=" + readTimeouts);
            if (problems.isEmpty()) System.out.println("RESULT: PASS");
            else { System.out.println("RESULT: FAIL"); problems.forEach(p -> System.out.println("  - " + p)); }
            System.out.println("-----------------------------------------");
        }
    }
}