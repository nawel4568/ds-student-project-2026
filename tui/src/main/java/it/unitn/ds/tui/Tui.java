package it.unitn.ds.tui;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.googlecode.lanterna.SGR;
import com.googlecode.lanterna.TerminalSize;
import com.googlecode.lanterna.TextColor;
import com.googlecode.lanterna.graphics.TextGraphics;
import com.googlecode.lanterna.input.KeyStroke;
import com.googlecode.lanterna.screen.Screen;
import com.googlecode.lanterna.terminal.DefaultTerminalFactory;
import it.unitn.ds.AbstractClient.ReadRequest;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.AbstractClient.ReadTimeout;
import it.unitn.ds.AbstractClient.WriteRequest;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.AbstractClient.WriteTimeout;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.AbstractReplica.CoordinatorElected;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.ElectionStarted;
import it.unitn.ds.AbstractReplica.InitSystem;
import it.unitn.ds.AbstractReplica.UpdateApplied;
import it.unitn.ds.Client;
import it.unitn.ds.Logger;
import it.unitn.ds.Replica;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Demo console for the replicated system. It only uses the public API (props, InitSystem, Crash,
 * Read/WriteRequest) and renders what the API callbacks and the Logger report.
 *
 * <p>Usage: {@code tui [replicas=5] [clients=3] [coordinator=0]}
 */
public final class Tui {
    private static final long READ_TIMEOUT_MS = 2000;
    // Must outlast heartbeat detection + election, otherwise writes during an election time out.
    private static final long WRITE_TIMEOUT_MS = 10000;
    private static final int LOG_LINES_KEPT = 500;
    private static final String HELP =
            "r <client> <replica> <idx> | w <client> <replica> <idx> <val> | c <replica> [Now|Heartbeat|Update|WriteOK|Election] [n] | debug | q";

    record Event(ActorRef from, Object msg) {}

    /** Listener given to every replica and client: forwards each callback to the UI thread. */
    static final class Probe extends AbstractActor {
        private final Queue<Event> out;

        Probe(Queue<Event> out) {
            this.out = out;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(m -> out.add(new Event(getSender(), m))).build();
        }
    }

    static final class ReplicaView {
        boolean crashed, electing;
        String armed; // pending deferred crash, as requested via the API
        int coordinator;
        final Map<Integer, Integer> values = new TreeMap<>();
    }

    static final class ClientView {
        int pending;
        String last = "-";
    }

    private final ActorSystem system = ActorSystem.create("Tui");
    private final Queue<Event> events = new ConcurrentLinkedQueue<>();
    private final List<ActorRef> replicas = new ArrayList<>();
    private final List<ActorRef> clients = new ArrayList<>();
    private final Map<ActorRef, Integer> replicaIds = new HashMap<>();
    private final Map<ActorRef, Integer> clientIds = new HashMap<>();
    private final ReplicaView[] replicaViews;
    private final ClientView[] clientViews;
    private final Deque<String> log = new ArrayDeque<>();
    private final RandomAccessFile logFile;
    private final StringBuilder input = new StringBuilder();
    private String status = "";
    private boolean debug;

    Tui(int nReplicas, int nClients, int coordinator) throws Exception {
        Path logPath = Files.createTempFile("ds-tui", ".log");
        Logger.setDestinationFile(logPath.toString());
        logFile = new RandomAccessFile(logPath.toFile(), "r");

        ActorRef probe = system.actorOf(Props.create(Probe.class, () -> new Probe(events)), "tui_probe");
        replicaViews = new ReplicaView[nReplicas];
        Map<Integer, ActorRef> group = new HashMap<>();
        for (int i = 0; i < nReplicas; i++) {
            ActorRef r = system.actorOf(
                    Replica.propsWithListener(
                            i,
                            AbstractReplica.MIN_LATENCY,
                            AbstractReplica.MAX_LATENCY,
                            AbstractReplica.COORDINATOR_BEAT_INTERVAL,
                            probe),
                    "Replica_" + i);
            replicas.add(r);
            replicaIds.put(r, i);
            group.put(i, r);
            replicaViews[i] = new ReplicaView();
            replicaViews[i].coordinator = coordinator;
        }
        InitSystem init = new InitSystem(group, coordinator);
        replicas.forEach(r -> r.tell(init, ActorRef.noSender()));

        clientViews = new ClientView[nClients];
        for (int i = 0; i < nClients; i++) {
            ActorRef c = system.actorOf(
                    Client.propsWithListener(READ_TIMEOUT_MS, WRITE_TIMEOUT_MS, Optional.empty(), probe),
                    "Client_" + i);
            clients.add(c);
            clientIds.put(c, i);
            clientViews[i] = new ClientView();
        }
    }

    public static void main(String[] args) throws Exception {
        int nReplicas = args.length > 0 ? Integer.parseInt(args[0]) : 5;
        int nClients = args.length > 1 ? Integer.parseInt(args[1]) : 3;
        int coordinator = args.length > 2 ? Integer.parseInt(args[2]) : 0;
        if (nReplicas < 1 || nClients < 1 || coordinator < 0 || coordinator >= nReplicas)
            throw new IllegalArgumentException("need replicas>=1, clients>=1, 0<=coordinator<replicas");

        Tui tui = new Tui(nReplicas, nClients, coordinator);
        try (Screen screen = new DefaultTerminalFactory().createScreen()) {
            screen.startScreen();
            screen.setCursorPosition(null);
            tui.loop(screen);
        } finally {
            tui.system.terminate();
        }
    }

    private void loop(Screen screen) throws Exception {
        while (true) {
            Event e;
            while ((e = events.poll()) != null) apply(e);
            tailLog();
            KeyStroke k;
            while ((k = screen.pollInput()) != null) {
                switch (k.getKeyType()) {
                    case Character -> input.append(k.getCharacter());
                    case Backspace -> {
                        if (!input.isEmpty()) input.setLength(input.length() - 1);
                    }
                    case Escape -> input.setLength(0);
                    case Enter -> {
                        String cmd = input.toString().trim();
                        input.setLength(0);
                        if (cmd.equals("q") || cmd.equals("quit")) return;
                        status = execute(cmd);
                    }
                    case EOF -> {
                        return;
                    }
                    default -> {}
                }
            }
            screen.doResizeIfNecessary();
            draw(screen);
            screen.refresh();
            Thread.sleep(50);
        }
    }

    private void apply(Event e) {
        switch (e.msg()) {
            case UpdateApplied u -> replicaViews[u.replicaId].values.put(u.index, u.value);
            case ElectionStarted s -> replicaViews[s.replicaId].electing = true;
            case CoordinatorElected c -> {
                replicaViews[c.replicaId].coordinator = c.newCoordinatorId;
                replicaViews[c.replicaId].electing = false;
            }
            case Crash c -> {
                ReplicaView v = replicaViews[replicaIds.get(e.from())];
                if (c.type == Crash.Type.Now) v.crashed = true;
                else v.armed = c.type + " after " + c.after_n_messages_of_type;
            }
            case ReadResult r -> clientDone(e.from(), "READ [" + r.index + "] = " + r.value + " from R" + r.fromReplica);
            case WriteResult w -> clientDone(
                    e.from(),
                    "WRITE [" + w.index + "] := " + w.value + (w.success ? " OK" : " FAILED") + " via R" + w.fromReplica);
            case ReadTimeout t -> clientDone(e.from(), "READ [" + t.index + "] TIMEOUT at " + t.replica.path().name());
            case WriteTimeout t -> clientDone(
                    e.from(), "WRITE [" + t.index + "] := " + t.value + " TIMEOUT at " + t.replica.path().name());
            default -> {}
        }
    }

    private void clientDone(ActorRef client, String what) {
        ClientView v = clientViews[clientIds.get(client)];
        v.pending = Math.max(0, v.pending - 1);
        v.last = what;
    }

    private String execute(String cmd) {
        String[] a = cmd.split("\\s+");
        try {
            switch (a[0]) {
                case "r", "read" -> {
                    int c = idx(a[1], clients), r = idx(a[2], replicas), i = Integer.parseInt(a[3]);
                    clients.get(c).tell(new ReadRequest(i, replicas.get(r)), ActorRef.noSender());
                    clientViews[c].pending++;
                    return "C" + c + " -> READ [" + i + "] at R" + r;
                }
                case "w", "write" -> {
                    int c = idx(a[1], clients), r = idx(a[2], replicas);
                    int i = Integer.parseInt(a[3]), val = Integer.parseInt(a[4]);
                    clients.get(c).tell(new WriteRequest(i, val, replicas.get(r)), ActorRef.noSender());
                    clientViews[c].pending++;
                    return "C" + c + " -> WRITE [" + i + "] := " + val + " at R" + r;
                }
                case "c", "crash" -> {
                    int r = idx(a[1], replicas);
                    Crash.Type type = a.length > 2 ? crashType(a[2]) : Crash.Type.Now;
                    int n = a.length > 3 ? Integer.parseInt(a[3]) : 1;
                    replicas.get(r).tell(new Crash(type, n), ActorRef.noSender());
                    return "crash R" + r + ": " + type + (type == Crash.Type.Now ? "" : " after " + n);
                }
                case "debug" -> {
                    debug = !debug;
                    Logger.setDebugEnabled(debug);
                    return "debug logs " + (debug ? "on" : "off");
                }
                case "" -> {
                    return "";
                }
                default -> {
                    return "unknown command: " + a[0];
                }
            }
        } catch (ArrayIndexOutOfBoundsException | IllegalArgumentException ex) {
            return "bad arguments (" + ex.getMessage() + ")";
        }
    }

    private static int idx(String s, List<?> range) {
        int i = Integer.parseInt(s.replaceFirst("^[rRcC]", ""));
        if (i < 0 || i >= range.size()) throw new IllegalArgumentException("id out of range: " + s);
        return i;
    }

    private static Crash.Type crashType(String s) {
        for (Crash.Type t : Crash.Type.values()) if (t.name().equalsIgnoreCase(s)) return t;
        throw new IllegalArgumentException("unknown crash type: " + s);
    }

    private void tailLog() throws Exception {
        String line;
        while ((line = logFile.readLine()) != null) {
            // readLine decodes as Latin-1; re-decode as the UTF-8 the Logger writes.
            line = new String(line.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
            log.addLast(line.length() > 11 ? line.substring(11) : line); // drop the date, keep the time
            if (log.size() > LOG_LINES_KEPT) log.removeFirst();
        }
    }

    private void draw(Screen screen) {
        screen.clear();
        TextGraphics g = screen.newTextGraphics();
        TerminalSize size = screen.getTerminalSize();
        int row = 0;

        g.enableModifiers(SGR.BOLD);
        g.putString(0, row++, "DS demo: " + replicas.size() + " replicas, " + clients.size() + " clients");
        g.disableModifiers(SGR.BOLD);
        row++;

        // Union of all indices any replica reported applying, one column each.
        SortedSet<Integer> indices = new TreeSet<>();
        for (ReplicaView v : replicaViews) indices.addAll(v.values.keySet());
        StringBuilder header = new StringBuilder(String.format("%-4s %-26s %-6s ", "ID", "STATUS", "COORD"));
        for (int i : indices) header.append(String.format("%6s", "[" + i + "]"));
        g.enableModifiers(SGR.UNDERLINE);
        g.putString(0, row++, header.toString());
        g.disableModifiers(SGR.UNDERLINE);
        for (int id = 0; id < replicaViews.length; id++) {
            ReplicaView v = replicaViews[id];
            String state;
            TextColor color;
            if (v.crashed) {
                state = "CRASHED";
                color = TextColor.ANSI.RED;
            } else if (v.electing) {
                state = "ELECTING";
                color = TextColor.ANSI.MAGENTA;
            } else if (v.coordinator == id) {
                state = "UP (coordinator)";
                color = TextColor.ANSI.YELLOW;
            } else {
                state = "UP";
                color = TextColor.ANSI.GREEN;
            }
            if (!v.crashed && v.armed != null) state += ", armed: " + v.armed;
            StringBuilder line = new StringBuilder(String.format("%-4s %-26s %-6s ", "R" + id, state, "R" + v.coordinator));
            for (int i : indices) {
                Integer val = v.values.get(i);
                line.append(String.format("%6s", val == null ? "." : val));
            }
            g.setForegroundColor(color);
            g.putString(0, row++, line.toString());
            g.setForegroundColor(TextColor.ANSI.DEFAULT);
        }
        row++;

        g.enableModifiers(SGR.UNDERLINE);
        g.putString(0, row++, String.format("%-4s %-8s %s", "ID", "PENDING", "LAST RESULT"));
        g.disableModifiers(SGR.UNDERLINE);
        for (int id = 0; id < clientViews.length; id++)
            g.putString(0, row++, String.format("%-4s %-8d %s", "C" + id, clientViews[id].pending, clientViews[id].last));
        row++;

        int bottom = size.getRows() - 4;
        g.enableModifiers(SGR.UNDERLINE);
        g.putString(0, row++, "LOG" + (debug ? " (debug)" : ""));
        g.disableModifiers(SGR.UNDERLINE);
        int logRows = Math.max(0, bottom - row);
        List<String> tail = new ArrayList<>(log).subList(Math.max(0, log.size() - logRows), log.size());
        for (String l : tail) {
            if (l.contains("CRASH")) g.setForegroundColor(TextColor.ANSI.RED);
            else if (l.contains("ELECT") || l.contains("COORDINATOR")) g.setForegroundColor(TextColor.ANSI.MAGENTA);
            g.putString(0, row++, l);
            g.setForegroundColor(TextColor.ANSI.DEFAULT);
        }

        g.setForegroundColor(TextColor.ANSI.CYAN);
        g.putString(0, size.getRows() - 3, HELP);
        g.putString(0, size.getRows() - 2, status);
        g.setForegroundColor(TextColor.ANSI.DEFAULT);
        g.enableModifiers(SGR.BOLD);
        g.putString(0, size.getRows() - 1, "> " + input + "_");
        g.disableModifiers(SGR.BOLD);
    }
}
